# -*- coding: UTF-8 -*-
import pymysql
import io
import re
import sys
import time
import os
import jieba
import jieba.posseg as pseg
from format_output import html_head,html_end,html_css,time_statistic,get_current_time
import traceback
from bisect import bisect_left
from aip import AipNlp

try:
    import cPickle as pickle
except:
    import  pickle


bonds = []
comps = []
find_cnt=0
api=None
call_cnt = 0
@time_statistic
def connect_db():
    db = pymysql.connect(host="10.1.10.61", user="bond_read", password="2@HQm#VG", db="lh_bond", charset='utf8')
    return db

@time_statistic
def get_test_data(cursor,newsfile='newsdata.pkl'):
    news=set()
    newsdata = {}
    cursor.execute('select NEWSCODE from T_NEWS_FIN_BD order by rand() limit 55')
    res = cursor.fetchall()
    for row in res:
        news.add(str(row[0]))
    cursor.execute('select NEWSCODE from T_NEWS_COMPANY_BD order by rand() limit 55')
    res = cursor.fetchall()
    for row in res:
        news.add(str(row[0]))
    for newscode in news:
        cursor.execute('select newstitle,newscontent from lh_bond_news where newscode=%s',newscode)
        res = cursor.fetchone()
        if res:
            newsdata[newscode] = res
        else:
            print("find newscode %s failed" % newscode)

    with open("../database/%s" % newsfile,'wb') as wf:
        pickle.dump(newsdata,wf,True)
    return newsfile

@time_statistic
def get_bond(cursor,resfile='../database/bonds_data.pkl'):
    bonds = []
    cursor.execute("select secode, exchange, symbol, bondname, bondsname from TQ_BD_BASICINFO")
    res = cursor.fetchall()
    for row in res:
        for attr in row:
            if attr:
                bonds.append(str(attr))
    print("len of bonds:", len(bonds))
    bonds.sort()
    with open(resfile,'wb') as wf:
        pickle.dump(bonds,wf,True)
    return resfile


@time_statistic
def get_comp(cursor,resfile='../database/comps_data.pkl'):
    comps = []
    cursor.execute("select compcode,compname,compsname,engname,engsname from TQ_COMP_INFO")
    res = cursor.fetchall()
    for row in res:
        for attr in row:
            if attr:
                comps.append(str(attr))
    print("len of comps:",len(comps))
    comps.sort()
    with open(resfile,'wb') as wf:
        pickle.dump(comps,wf,True)
    return resfile

# @time_statistic
# def recognize_news(newsdata, resultfile=None,write_encoding='utf-8',sample=None):
#     def tokenize_text(text):
#         res = pseg.cut(text)
#         html = ""
#         for word, tag in res:
#             if (tag == 'comp'):
#                 html += '''<span id="comp">%s</span>''' % word
#             elif (tag == 'bond'):
#                 html += '''<span id="bond">%s</span>''' % word
#             else:
#                 html  += word
#         html += '<br>'
#         return html
#
#     with open(resultfile, 'w', encoding=write_encoding) as wf:
#         i = 0
#         html_text = ""
#         for newscode in newsdata:
#             newstitle,newscontent = newsdata[newscode]
#             try:
#                 html_text += "<p>newscode: %s</p>" % newscode
#                 html_text += tokenize_text(newstitle) + tokenize_text(newscontent)
#                 i += 1
#                 if i == 100:
#                     print("processed %d news" % i)
#                 if sample and i == sample:
#                     print("complete test %d news" % sample)
#                     break
#             except Exception as e:
#                 print("newscode = %s error" % newscode)
#                 print(e)
#         html = html_head + html_css + html_text + html_end
#         wf.write(html)



def load_data(newsfile,file1,file2):
    os.chdir("../database")
    with open(newsfile,'rb') as nf, open(file1,'rb') as f1, open(file2,'rb') as f2:
        newsdata = pickle.load(nf,encoding='utf-8')
        print("load news file successfully, len of bonds：", len(newsdata))
        bonds = pickle.load(f1,encoding='utf-8')
        print("load bonds file successfully, len of bonds：", len(bonds))
        comps = pickle.load(f2,encoding='utf-8')
        print("load comps file successfully, len of comps：", len(comps))
    os.chdir("../scripts")
    return newsdata,bonds,comps

def preprocess(text):
    return re.split(r'，|。|,|;|；|！|!|\?|？|\s|\"|“|”', text)


def binary_search(a, x, lo=0, hi=None):  # can't use a to specify default for hi
    hi = hi if hi is not None else len(a)  # hi defaults to len(a)
    pos = bisect_left(a, x, lo, hi)  # find insertion position
    return (pos if pos != hi and a[pos] == x else -1)  # don't walk off the end




def tag_search(text):
    def findwords(sentence, window):
        tagwords = []
        n = len(sentence)
        for i in range(n-window):
            word = sentence[i:i+window]
            if binary_search(bonds,word)!=-1:
                tagwords.append((word,'bond'))
            elif binary_search(comps,word)!=-1:
                tagwords.append(((word,'comp')))
        return tagwords
    ptext = preprocess(text)
    related_words = []
    for sentence in ptext:
        for d in range(1,min(30,len(sentence) + 1)):
            related_words += findwords(sentence,d)
    return related_words


def tag_jieba(text):
    def load_jieba_dicts(dict_folder="../jieba_low_freq_dict"):
        for dictname in os.listdir(dict_folder):
            print("load dict %s" % dictname)
            jieba.load_userdict(dict_folder + '/' + dictname)
    jieba.initialize()
    load_jieba_dicts()
    res = pseg.cut(text)
    for word,tag in res:
        print(word,tag)

def get_baidu_api():
    APP_ID = '10703963'
    API_KEY = 'CliSHDD6DSoUN9dPTUMbDNdS'
    SECRET_KEY = 'MP5pdqNggcX97sacVcsOLRo5qXsplPku'
    global  api
    api = AipNlp(APP_ID, API_KEY, SECRET_KEY)



def tag_baidu(text):
    global api,call_cnt
    # print("baidu tag words:")
    related_words = []
    try:
        ptext = preprocess(text)
        for sentence in ptext:
            if len(sentence)>1:
                sentence = sentence.replace('\xa0', ' ').replace('\\u000', ' ')
                res = api.lexerCustom(sentence)
                call_cnt += 1
                if call_cnt% 19 == 0:
                    time.sleep(1)
                if res and res.get('items'):
                    for item in res.get('items'):
                        if item['ne'] == 'BOND':
                            related_words.append((item['item'],'bond'))
                        elif item['ne'] == 'COMP' or item['ne'] == 'ORG':
                            related_words.append((item['item'], 'comp'))
    except Exception as e:
        print(e)
        traceback.print_exc()
    # for word,tag in related_words:
    #     print(word,tag)
    return related_words


'''
use baidu api and search mode
three mode: tag_baidu, tag_jieba, tag_search

'''
def tag_text(text):
    global find_cnt
    words1 = tag_search(text)
    words2 = tag_baidu(text)
    related_words = words1 + words2
    html = ''
    if related_words:
        print("find %d realted words" % len(related_words))
        find_cnt += len(related_words)
        tag_words = {}
        for word,tag in related_words:
            # print(word,tag)
            index = text.find(word)
            if index in tag_words:
                tag_words[index][0] = max(len(word),tag_words[index][0])
            else:
                tag_words[index] = [len(word),tag]

        tag_words = sorted(tag_words.items())
        last = 0
        for index,(wordlen,tag) in tag_words:
            if last>index:
                continue
            if last<index:
                html += text[last:index]
            html += '''<span id="%s">%s</span>''' % (tag,text[index:index+wordlen])
            last = index + wordlen
        if last<len(text):
            html += text[last:len(text)]
    else:
        html += text
    html += '<br>'
    return html


@time_statistic
def read_news(newsdata, resultfile=None,write_encoding='utf-8',sample=None):
    with open(resultfile, 'w', encoding=write_encoding) as wf:
        i = 0
        html_text = ""
        for newscode in newsdata:
            newstitle,newscontent = newsdata[newscode]
            try:
                html_text += "<p>newscode: %s</p>" % newscode
                html_text += tag_text(newstitle) + tag_text(newscontent)
                i += 1
                if i % 10 == 0:
                    print("processed %d news" % i)
                if sample and i == sample:
                    print("complete test %d news" % sample)
                    break
            except Exception as e:
                print("newscode = %s error" % newscode)
                print(e)
                traceback.print_exc()
        html = html_head + html_css + html_text + html_end
        wf.write(html)


def baidu_test(text):
    get_baidu_api()
    res = tag_baidu(text)
    for word,tag in res:
        print(word,tag)

'''
just for test, a piece of news
'''
def test_one(text):
    words1 = tag_search(text)
    words2 = tag_baidu(text)
    # related_words format:[(word,tag),(word,tag),....]
    related_words = words1 + words2
    if related_words:
        tag_words = {}
        for word, tag in related_words:
            print(word,tag)
            index = text.find(word)
            if index in tag_words:
                tag_words[index][0] = max(len(word), tag_words[index][0])
            else:
                tag_words[index] = [len(word), tag]

        tag_words = sorted(tag_words.items())
        last = 0
        for index, (wordlen, tag) in tag_words:
            if last > index:
                continue
            if last < index:
                html += text[last:index]
            html += '''<span id="%s">%s</span>''' % (tag, text[index:index + wordlen])
            last = index + wordlen
        if last < len(text):
            html += text[last:len(text)]
    else:
        html += text
    html += '<br>'
    return html
if __name__ == '__main__':
    print('start program')
    t1 = time.time()
    db = connect_db()
    cursor = db.cursor()
    # newsfile =get_test_data(cursor)
    # bond_file = get_bond(cursor)
    # comp_file = get_comp(cursor)
    # db.close()
    # recognize_news(newsdata, resultfile="../report/180408_sample_100.html",sample=100)
    # global bonds,comps
    # newsdata,bonds,comps = load_data('newsdata.pkl','bonds_data.pkl','comps_data.pkl')
    # get_baidu_api()
    # read_news(newsdata,resultfile="../report/180410_1142_sample_100.html",sample=100)
    # print("find %d words" % find_cnt)
    # text = '上海银监局称，2012年、2013年，该分行未能通过有效的内部控制措施发现并纠正其员工私售行为，内部控制严重违反审慎经营规则。　　上海银监局表示，依据《中华人民共和国银行业监督管理法》第四十六条第（五）项，决定对民生银行上海分行做出责令改正，并处罚款人民币50万元的行政处罚决定。 '
    # text2='民生银行上海分行内控不当员工私售 被责令改正罚款50万	民生银行上海分行内控不当员工私售 被责令改正罚款50万信息来源:新浪财经   2017-03-17       　　　　上海银监局发布的对民生银行上海分行的行政处罚决定　　3月17日，上海银监局发布对中国民生银行股份有限公司上海分行的行政处罚决定，该行主要负责人为胡庆华，上海银监局称，2012年、2013年，该分行未能通过有效的内部控制措施发现并纠正其员工私售行为，内部控制严重违反审慎经营规则。　　上海银监局表示，依据《中华人民共和国银行业监督管理法》第四十六条第（五）项，决定对民生银行上海分行做出责令改正，并处罚款人民币50万元的行政处罚决定。'
    # text3='巨潮资讯网网站上发布有公告　“证通电子：为全资子公司向银行申请综合授信提供担保的公告”，详情请见附件：附件下载超链接 '
    # text4='在新入4家股东中，瑞煜（上海）股权投资基金合伙企业（以下简称“瑞煜”）、北京合盛源投资管理有限公司（以下简称“合盛源”）两家公司资金均来源于无法穿透的信托'
    # text5='光大银行今年涉及的违约债券共7起，其中3起（11中城建MTN1、12中城建MTN1、14中城建PPN004）为中城建近期发生的违约债券'
    # baidu_test(text5)
    # t2 = time.time()
    # print('completed, spent time:', time.strftime("%H:%M:%S", time.gmtime(t2-t1)))
    print("vpn is ok")
