# coding=utf-8

import pandas
from pyhive import hive


def import_data():
    try:
        df = pandas.read_csv('mock.dat', sep=',')
        print len(df)
        col_num = len(df.columns)
        if partition is not None:
            sql = 'insert overwrite table ' + table + ' partition(' + partition + ') ' + ' values'
        else:
            sql = 'insert overwrite table ' + table + ' values'
        for index, row in df.iterrows():
            line = '('
            if col_num == 1:
                line += "'" + row[0] + "',"
            else:
                for i in range(col_num):
                    line += "'" + str(row[i]) + "',"

            new_line = line[:len(line) - 1] + ')'
            sql += new_line + ','
        new_sql = sql[:len(sql) - 1]
        print new_sql
        cursor.execute(new_sql)
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    table = 'test3'
    # partition = 'pdate="2021-05-12",chour="2021-05-12-12"'
    partition = None
    conn = hive.Connection(host='172.30.1.233',
                           port=10000,
                           auth="CUSTOM",
                           database='ietool',
                           username='admin',
                           password='admin')
    cursor = conn.cursor()
    with open('mock.dat', 'w') as f:
        f.write('0,1\n')
        for i in range(10):
            f.write('111,aaa\n')
        f.close()
    import_data()
