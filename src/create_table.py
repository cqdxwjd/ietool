# coding=utf-8

from pyhive import hive
import re
import os
import pandas

database = 'ietool'
conn = hive.Connection(host='172.30.1.233',
                       port=10000,
                       auth="CUSTOM",
                       database=database,
                       username='admin',
                       password='admin')
cursor = conn.cursor()
try:
    for file in os.listdir('ddl'):
        print file
        with open('ddl/' + file, 'r') as f:
            ddl = ''.join(f.readlines())
            print ddl
            cursor.execute(ddl)
except Exception as e:
    print e
finally:
    cursor.close()
    conn.close()
