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
    df = pandas.read_csv('data/test2.dat', sep=',', header=0)
    print len(df)
    col_num = len(df.columns)
    sql = 'insert into table test2 values'
    for index, row in df.iterrows():
        line = '('
        if col_num == 1:
            line += "'"+row[0]+"',"
        else:
            for i in range(col_num):
                line += "'" + row[i] + "',"

        new_line = line[:len(line) - 1] + ')'
        sql += new_line + ','
    new_sql = sql[:len(sql) - 1]
    # cursor.execute(new_sql)
except Exception as e:
    print e
finally:
    cursor.close()
    conn.close()
