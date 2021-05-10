# coding=utf-8

from pyhive import hive
import re
import os

# database = 'ietool'
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
        cursor.execute('show create table ' + database + '.' + result[0])
        ddl_list = cursor.fetchall()
        ddl_string = ''
        new_string = ''
        for ddl_line in ddl_list:
            ddl_string += ddl_line[0] + '\n'
        location = re.search('location', ddl_string, re.I)
        if location is not None:
            location_start = location.start()
            tblp_start = re.search('TBLPROPERTIES', ddl_string, re.I).start()
            new_string = ddl_string.replace(ddl_string[location_start:tblp_start], '')
        else:
            new_string = ddl_string
        if not os.path.exists('ddl'):
            os.mkdir('ddl')
        with open('ddl/' + table_name + '.ddl', 'w') as f:
            f.write(new_string)
            f.close()
except Exception as e:
    print e
finally:
    cursor.close()
    conn.close()
