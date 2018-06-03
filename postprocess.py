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

def checkfile(filename):
    if not os.path.exists(filename):
        print("no file")
        exit()


def complete_tag(file):
    pattern1 = re.compile(r'(?<=<span id="comp">).*?(?=</span>)')
    pattern2 = re.compile(r'(?<=<span id="bond">).*?(?=</span>)')
    compwords = set()
    with open(file,encoding='utf-8') as rf:
        for line in rf:
            compwords.update(set(pattern1.findall(line)))
    resfile = '../report/%s.html' % get_current_time()
    with open(file,encoding='utf-8') as rf,open(resfile,'w',encoding='utf-8') as wf:
        cnt=0
        for line in rf:
            # bondwords = set(pattern2.findall(line))
            for comp in compwords:
                rx =r'(?<!"comp">)(%s)(?!</span>)' % comp
                pat = re.compile(rx)
                to_change = pat.findall(line)
                if to_change:
                    findcnt=len(to_change)
                    print("%s %d" % (comp, findcnt))
                    cnt+=findcnt
                line = pat.sub(r'<span id="comp">\1</span>',line)

            wf.write(line)
        print("replace %d words" % cnt)
    return resfile

'''
    ss = '<span id="comp">abc交行</span>wordlibabc交行<span id="comp">abc交行</span>abc交行<span id="comp">abc交行</span>'
    p1=re.compile(r'(?<=<span id="comp">).*?(?=</span>)')
    res = p1.findall(ss)
    for word in res:
        print(word)
    pat = re.compile('[^>]abc交行)
'''

def delete_comp_tag(file):
    words = ['公司','ABS','的近','汉族','一卡通','保荐代表人','SCP003','私人公司','法人','股份制银行','中期息','国华','融资租赁','大公司','央企','太阳能','绿色建筑']
    resfile = '../report/%s.html' % get_current_time()
    with open(file, encoding='utf-8') as rf, open(resfile, 'w', encoding='utf-8') as wf:
        cnt = 0
        for line in rf:
            for word in words:
                regx = r'<span id="comp">%s</span>' % word
                cnt+=len(re.findall(regx, line))
                line = re.sub(regx, word, line)
            wf.write(line)
        print("delete %d words" % cnt)
    return resfile

def add_bond_tag(file):
    resfile = '../report/%s.html' % get_current_time()
    with open(file, encoding='utf-8') as rf, open(resfile, 'w', encoding='utf-8') as wf:
        cnt = 0
        for line in rf:
            regx = r'(?<!id="bond">)(?<=[\(（])(\d+\.[a-zA-Z]+)'
            cnt+=len(re.findall(regx, line))
            line = re.sub(regx,'<span id="bond">\1</span>', line)
            wf.write(line)
        print("add %d bonds" % cnt)
    return resfile

def add_bond_tag2(file):
    resfile = '../report/%s.html' % get_current_time()
    with open(file, encoding='utf-8') as rf, open(resfile, 'w', encoding='utf-8') as wf:
        cnt = 0
        for line in rf:
            regx = r'(?<=\()(\d{6})(?=\))'
            cnt+=len(re.findall(regx, line))
            line = re.sub(regx,'<span id="bond">\1</span>', line)
            wf.write(line)
        print("add %d bonds" % cnt)
    return resfile

def add_comp_tag(file):
    words = ['公司','ABS','的近','汉族','一卡通','保荐代表人','SCP003','私人公司','法人','股份制银行','中期息','国华','融资租赁','央企']
    time.sleep(1)
    resfile = '../report/%s.html' % get_current_time()
    with open(file, encoding='utf-8') as rf, open(resfile, 'w', encoding='utf-8') as wf:
        cnt = 0
        for line in rf:
            for word in words:
                regx = r'<span id="comp">%s</span>' % word
                cnt+=len(re.findall(regx, line))
                line = re.sub(regx, word, line)
            wf.write(line)
        print("delete %d words" % cnt)
    return resfile


if __name__ == '__main__':
    file = '../report/0411-3.html'
    checkfile(file)
    delete_comp_tag(file)



