# -*- coding: UTF-8 -*-
import os
import re
import time
from bisect import bisect_left
import pymysql
from format_output import time_statistic
import pickle

bond_list = []
comp_list = []
news_list_file = 'news_100.pkl'
news_data_file = 'news_100.txt'
bond_list_file = 'all_bonds.pkl'
comp_list_file = 'all_comps.pkl'
annotation_data_name = 'label_100_2.txt'
ROOT_DIR = ''


@time_statistic
def dict_analysis():
    minlen = 9999
    maxlen = 0
    single_word_list = []
    long_word_list = []
    for word in bond_list:
        minlen = min(len(word), minlen)
        if (len(word) == 1):
            single_word_list.append(word)
        elif len(word) > 50:
            long_word_list.append(word)
        maxlen = max(len(word), maxlen)
    for word in comp_list:
        minlen = min(len(word), minlen)
        if (len(word) == 1):
            single_word_list.append(word)
        elif len(word) > 30:
            long_word_list.append(word)
        maxlen = max(len(word), maxlen)
    return maxlen, minlen, single_word_list, long_word_list


def binary_search(a, x, lo=0, hi=None):  # can't use a to specify default for hi
    hi = hi if hi is not None else len(a)  # hi defaults to len(a)
    pos = bisect_left(a, x, lo, hi)  # find insertion position
    return (pos if pos != hi and a[pos] == x else -1)  # don't walk off the end


def preprocess(text):
    return re.split(r'，|。|,|;|；|！|!|\?|？|\s|\"|“|”', text)


def auto_tag(annotation_data_name, text, max_len):
    back_to_root()
    os.chdir("../database")
    with open(annotation_data_name, 'w', encoding='utf-8') as train_file:
        i = 0
        while i < len(text):
            find = False
            tag = ""
            word_length = max_len
            while word_length > 1 and i + word_length < len(text):
                word = text[i:i+word_length]
                if binary_search(bond_list, word) != -1:
                    find = True
                    tag = "BOND"
                elif binary_search(comp_list, word) != -1:
                    find = True
                    tag = "COMP"
                if find:
                    break
                else:
                    word_length -= 1
            if not find:
                train_file.write("%s O\n" % text[i])
                i += 1
            else:
                train_file.write("%s B-%s\n" % (text[i], tag))
                word_end = i + word_length - 1
                i += 1
                while i < word_end and i < len(text) - 1:
                    train_file.write("%s I-%s\n" % (text[i], tag))
                    i += 1
                train_file.write("%s E-%s\n" % (text[i], tag))
                i += 1
            if i % 2000 == 0:
                print("processed %d chars" % i)


# auto label data by domain dict to generate train dataset
@time_statistic
def gen_news_data():
    db = connect_db()
    cursor = db.cursor()
    back_to_root()
    os.chdir("../database")
    cursor.execute('select newscode,newstitle,newscontent from lh_bond_news')
    res = cursor.fetchall()
    train_news_list = {}
    with open(news_list_file, 'wb') as f1, open(news_data_file, 'w', encoding='utf-8') as f2:
        for row in res:
            if len(row) != 3:
                print("news record not completed")
            else:
                newscode, newstitle, newscontent = row
                newstitle = newstitle.strip()
                newscontent = newscontent.strip()
                train_news_list[newscode] = (newstitle, newscontent)
                f2.write("%s\n%s\n" % (newstitle, newscontent))
        try:
            pickle.dump(train_news_list, f1)
        except Exception as e:
            print(e)
    db.close()


@time_statistic
def connect_db():
    db = pymysql.connect(host="10.1.10.61", user="bond_read", password="2@HQm#VG", db="lh_bond", charset='utf8')
    return db


@time_statistic
def load_data(news_list_file, bond_list_file, comp_list_file):
    back_to_root()
    os.chdir("../database")
    news_list = []
    comp_list = []
    bond_list = []
    try:
        with open(news_list_file, 'rb') as nf, open(bond_list_file, 'rb') as f1, open(comp_list_file, 'rb') as f2:
            news_list = pickle.load(nf, encoding='utf-8')
            print("load news file successfully, len of bonds：", len(news_list))
            bond_list = pickle.load(f1, encoding='utf-8')
            print("load bonds file successfully, len of bonds：", len(bond_list))
            comp_list = pickle.load(f2, encoding='utf-8')
            print("load comps file successfully, len of comps：", len(comp_list))
    except Exception as e:
        print(e)
    return news_list, bond_list, comp_list


def auto_label_data(annotation_data_name, news_data_file):
    back_to_root()
    os.chdir("../database")
    with open(news_data_file, 'r', encoding='utf-8') as news_file:
        text = news_file.read()
        auto_tag(annotation_data_name, text, max_word_len)


def back_to_root():
    global ROOT_DIR
    os.chdir(ROOT_DIR)


def exist_files(filelist):
    back_to_root()
    os.chdir("../database")
    exist = True
    for filename in filelist:
        if not os.path.exists(filename):
            exist = False
            break
    return exist

@time_statistic
def get_domain_dict(bond_list_file, comp_list_file):
    db = connect_db()
    cursor = db.cursor()
    back_to_root()
    os.chdir("../database")
    comps_set = set()
    bonds_set = set()
    cursor.execute("select compcode,compname,compsname,engname,engsname from TQ_COMP_INFO")
    res = cursor.fetchall()
    for row in res:
        for attr in row:
            if attr:
                comps_set.add(str(attr))
    cursor.execute("select secode, exchange, symbol, bondname, bondsname from TQ_BD_BASICINFO")
    res = cursor.fetchall()
    for row in res:
        for attr in row:
            if attr:
                bonds_set.add(str(attr))
    print("len of bonds:", len(bonds_set))
    print("len of comps:",len(comps_set))
    bonds = list(bonds_set)
    comps = list(comps_set)
    bonds.sort()
    comps.sort()
    with open(bond_list_file,'wb') as f1, open(comp_list_file,'wb') as f2:
        pickle.dump(bonds,f1,True)
        pickle.dump(comps,f2,True)
    print("completed generate domain dict")


if __name__ == '__main__':
    print('start program')
    t1 = time.time()
    ROOT_DIR = os.getcwd()
    # if exist_files([news_list_file, news_data_file]):
    #     command = input("%s and %s exists, do you want to override them? input Y or N:" % (news_list_file, news_data_file))
    #     if command.lower() == 'y':
    #         gen_news_data()
    # else:
    #     gen_news_data()
    # if exist_files([bond_list_file, comp_list_file]):
    #     command = input("%s and %s exists, do you want to override them? input Y or N:" % (bond_list_file, comp_list_file))
    #     if command.lower() == 'y':
    #         get_domain_dict(bond_list_file,comp_list_file)
    # else:
    #     get_domain_dict(bond_list_file, comp_list_file)
    news_list, bond_list, comp_list = load_data(news_list_file, bond_list_file, comp_list_file)
    max_word_len, min_word_len, single_word_list, long_word_list = dict_analysis()
    print("min word len is %d" % min_word_len)
    print(single_word_list)
    print("max word len is %d" % max_word_len)
    print(long_word_list)
    auto_label_data(annotation_data_name, news_data_file)
    t2 = time.time()
    print('completed, spent time:', time.strftime("%H:%M:%S", time.gmtime(t2 - t1)))
    print("end program")
