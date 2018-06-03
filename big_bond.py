# -*- coding: UTF-8 -*-
from cgitb import reset

import cx_Oracle
import os
import time
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
# conn_str = 'bond_read/2@HQm#VG@10.1.10.99:1521/ORCL'
# conn = cx_Oracle.connect(conn_str)
# print(conn.version)
# conn.close()

# myscript.py

# Connect as user "hr" with password "welcome" to the "oraclepdb" service running on this computer.
connection = cx_Oracle.connect("bond_read", "2@HQm#VG", "10.1.10.99:1521/ORCL")

cursor = connection.cursor()
# cursor.execute("desc ENTREL.ENTITY_INFO_ADD")
# cursor.execute('select * from ALL_TAB_COLUMNS WHERE TABLE_NAME="ENTREL.ENTITY_INFO_ADD"')

t0 = time.time()
with open("../database/all_entities_0524.txt", 'w',encoding='utf-8') as wf:

    # res = cursor.fetchmany(numRows = 10)
    rowid = 0
    batch = 200000
    rownums = 43516447
    while(rowid<rownums):
        endrow = min(rowid+batch,rownums)
        sql = 'SELECT ENTITY_NAME from (select m.ENTITY_NAME, rownum r from ENTREL.ENTITY_INFO_ADD m) where r > {start} and r <= {end}'.format(start = rowid, end = endrow)
        print(sql)
        try:
            cursor.execute(sql)
            rowid += batch
            res = cursor.fetchall()
            i = 0
            t1 = time.time()
            print("comleted selected, start write")
            for resone in res:
                try:
                    wf.write(resone[0] + '\n')
                    i += 1
                    if i%10000 == 0:
                        print("have written %d entites\n" % i)
                        print('spent time:', time.strftime("%H:%M:%S", time.gmtime(time.time() - t1)))
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)
print("SUCCESS")
t2 = time.time()
print('totallly spent time:', time.strftime("%H:%M:%S", time.gmtime(t2 - t0)))
connection.close()