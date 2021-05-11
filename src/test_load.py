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
    cursor.execute("load data local inpath 'data/test.dat' into table test")
except Exception as e:
    print e
finally:
    cursor.close()
    conn.close()
