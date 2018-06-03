# coding:utf-8
import pickle
import os
from collections import Counter
ROOT_PATH = 'C:\\Users\XZC\stock'
DATA_PATH = ROOT_PATH + '\database'
all_news_pickle_file  = 'all_news_data_table.pkl'
word_freq_file = 'word_freq_news_100.txt'
word_freq_pickle_file = 'word_freq_news_100.pkl'
news = []
def get_news(all_news_pickle_file, news_cnt):
    news_pickle_file = "news_%d.pkl" % news_cnt
    news_file = "news_%d.txt" % news_cnt
    os.chdir(DATA_PATH)
    news = []
    if os.path.exists(news_pickle_file):
        override = input("%s exists, do you want to override it? Y/N?\n" % news_pickle_file)
        if override.lower()=='n':
            with open(news_pickle_file,'rb') as nf:
                news = pickle.load(nf)
    else:
        with open(all_news_pickle_file, 'rb') as rf, open(news_file, 'w', encoding='utf-8') as wf:
            all_news = pickle.load(rf)
            cnt = 0
            for news_id in all_news:
                text = all_news[news_id][1]
                text = text.replace('\r\n',' ').replace('\n',' ')
                news.append((news_id, text))
                wf.write(text + '\n')
                cnt += 1
                if cnt == 100:
                    break
            with open(news_pickle_file, 'wb') as wf:
                pickle.dump(news, wf)
    return news,news_file


def get_word_dict(corpus_file, word_freq_file):
    os.chdir(DATA_PATH)
    word_dict = {}
    with open(corpus_file,'r',encoding='utf-8') as cf:
        text = cf.read()
        mycount = Counter(text)
    for word,cnt in mycount.items():
        word_dict[word] = cnt
    word_freq = sorted(word_dict.items(),key=lambda x:x[1],reverse=True)
    with open(word_freq_file, 'w', encoding='utf-8') as wf, open(word_freq_pickle_file,'wb') as pwf:
        for word,cnt in word_freq:
            wf.write(word + '\t' + str(cnt) + '\n')
        pickle.dump(word_freq,pwf)
    return word_freq


if __name__ == '__main__':
    news,news_file = get_news(all_news_pickle_file,100)
    word_freq = get_word_dict(news_file,word_freq_file)