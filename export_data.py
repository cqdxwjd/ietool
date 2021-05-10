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
    cursor.execute('show tables')
    for result in cursor.fetchall():
        table_name = result[0]
        print table_name
        cursor.execute('select * from ' + table_name)
        rows = cursor.fetchall()
        df = pandas.DataFrame(rows)
        if not os.path.exists('data'):
            os.mkdir('data')
        df.to_csv('data/' + table_name + '.dat', sep=',', header=True, index=False)
except Exception as e:
    print e
finally:
    cursor.close()
    conn.close()
