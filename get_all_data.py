# -*- coding: utf-8 -*-

import pymysql
try:
    import cPickle as pickle
except:
    import pickle

def connect_db():
    db = pymysql.connect(host="10.1.10.61", user="bond_read", password="2@HQm#VG", db="lh_bond", charset='utf8')
    return db
