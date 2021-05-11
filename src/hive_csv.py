# coding=utf-8

from pyhive import hive
import re
import os
import pandas
import sys
import configparser


def export_ddl():
    try:
        cursor.execute('show tables')
        for result in cursor.fetchall():
            table_name = result[0]
            cursor.execute('show create table ' + result[0])
            ddl_list = cursor.fetchall()
            ddl_string = ''
            for ddl_line in ddl_list:
                ddl_string += ddl_line[0] + '\n'
            location = re.search('location', ddl_string, re.I)
            if location is not None:
                location_start = location.start()
                tblp_start = re.search('TBLPROPERTIES', ddl_string, re.I).start()
                new_string = ddl_string.replace(ddl_string[location_start:tblp_start], '')
            else:
                new_string = ddl_string
            if not os.path.exists('./ddl'):
                os.mkdir('./ddl')
            with open('./ddl/' + table_name + '.ddl', 'w') as f:
                f.write(new_string)
            f.close()
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()


def export_data():
    try:
        cursor.execute('show tables')
        for result in cursor.fetchall():
            table_name = result[0]
            cursor.execute('select count(1) from ' + table_name)
            count = cursor.fetchone()[0]
            # 最多导出100万条数据
            cursor.execute('select * from ' + table_name + ' limit 1000000')
            rows = cursor.fetchall()
            df = pandas.DataFrame(rows)
            if not os.path.exists('./data'):
                os.mkdir('./data')
            df.to_csv('./data/' + table_name + '.dat', sep=',', header=False, index=False)
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()


def import_ddl(path):
    try:
        for file in os.listdir(path):
            with open(path + '/' + file, 'r') as f:
                ddl = ''.join(f.readlines())
                cursor.execute(ddl)
                # 修改表字段分隔符为逗号
                cursor.execute("alter table " + file[:len(file) - 4] + " set serdeproperties('field.delim'=',')")
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()


def import_data(in_path):
    try:
        for file in os.listdir(in_path):
            df = pandas.read_csv(in_path + '/' + file, sep=',', header=0)
            print len(df)
            col_num = len(df.columns)
            sql = 'insert into table ' + file[:len(file) - 4] + ' values'
            for index, row in df.iterrows():
                line = '('
                if col_num == 1:
                    line += "'" + row[0] + "',"
                else:
                    for i in range(col_num):
                        line += "'" + row[i] + "',"

                new_line = line[:len(line) - 1] + ')'
                sql += new_line + ','
            new_sql = sql[:len(sql) - 1]
            cursor.execute(new_sql)
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()


def import_data2(in_path):
    try:
        for file in os.listdir(in_path):
            cursor.execute(
                "load data local inpath '" + os.path.abspath(in_path) + '/' + file + "' overwrite into table " + file[
                                                                                                                 :len(
                                                                                                                     file) - 4])
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    parser = configparser.ConfigParser()
    parser.read(filenames=['config.ini'])
    database = parser.get('hive', 'database')
    host = parser.get('hive', 'host')
    port = parser.get('hive', 'port')
    username = 'admin'
    password = 'admin'

    conn = hive.Connection(host=host,
                           port=int(port),
                           auth="CUSTOM",
                           database=database,
                           username=username,
                           password=password)
    cursor = conn.cursor()

    action, target, in_path = sys.argv[1:]
    if action == 'export':
        if target == 'ddl':
            export_ddl()
        elif target == 'data':
            export_data()
        else:
            print 'incorrect params'
    elif action == 'import':
        if target == 'ddl':
            import_ddl(in_path)
        elif target == 'data':
            import_data2(in_path)
        else:
            print 'incorrect params'
    else:
        print 'incorrect params'
