# coding=utf-8

from pyhive import hive
import re
import os

database = 'dsep'
conn = hive.Connection(host='172.30.1.233',
                       port=10000,
                       auth="CUSTOM",
                       database=database,
                       username='admin',
                       password='admin')
cursor = conn.cursor()
try:
    cursor.execute('show tables')
    for result in cursor.fetchall():
        table_name = result[0]
        print table_name
except Exception as e:
    print e
finally:
    cursor.close()
    conn.close()
