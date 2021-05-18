# coding=utf-8

from pyhive import hive
import re
import os
import pandas
import sys
import configparser


def get_tables():
    try:
        cursor.execute('show tables like "' + filter + '"')
        table_list = []
        for result in cursor.fetchall():
            table_list.append(result[0])
        print table_list
        if not os.path.exists('./table'):
            os.mkdir('./table')
        with open('./table/tables.txt', 'w') as f:
            f.write(','.join(table_list))
    except Exception as e:
        print e


def export_ddl():
    try:
        with open('./table/tables.txt', 'r') as f:
            tables = f.readline().split(',')
        for table in tables[int(start):int(end)]:
            cursor.execute('show create table ' + table)
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
            with open('./ddl/' + table + '.ddl', 'w') as f:
                f.write(new_string)
                f.close()
            print 'exported ddl of table ' + table
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()


def format_partition(str):
    str_split = str.split('=')
    return str_split[0] + '=' + '"' + str_split[1] + '"'


def export_data():
    try:
        with open('./table/tables.txt', 'r') as f:
            tables = f.readline().split(',')
        for table in tables[int(start):int(end)]:
            # cursor.execute('select count(1) from ' + table)
            # count = cursor.fetchone()[0]
            cursor.execute('show create table ' + table)
            ddl_list = cursor.fetchall()
            ddl_string = ''
            for ddl_line in ddl_list:
                ddl_string += ddl_line[0] + '\n'
            is_partition = re.search('PARTITIONED BY', ddl_string, re.I)
            if not os.path.exists('data'):
                os.mkdir('data')
            root = os.path.abspath('data')
            # 最多导出max条数据,只导出有数据的表
            if is_partition is not None:
                if not os.path.exists(root + '/' + table):
                    os.mkdir(root + '/' + table)
                table_path = os.path.abspath(root + '/' + table)
                cursor.execute('show partitions ' + table)
                partitions = cursor.fetchall()
                if len(partitions) != 0:
                    # 最新的指定个数分区列表
                    latest_partitions = partitions[-partition_count:]
                    for cur_partition in latest_partitions:
                        # 重置当前路径到表文件夹所在目录
                        cur_path = table_path
                        latest_partition_split = str(cur_partition[0]).split('/')
                        new_latest_partition_split = []
                        for s in latest_partition_split:
                            if not os.path.exists(cur_path + '/' + s):
                                os.mkdir(cur_path + '/' + s)
                            cur_path = os.path.abspath(cur_path + '/' + s)
                            new_latest_partition_split.append(format_partition(s))
                        if len(new_latest_partition_split) > 1:
                            final_partition_str = ' and '.join(new_latest_partition_split)
                        else:
                            final_partition_str = new_latest_partition_split[0]
                        sql = 'select * from ' + table + ' where ' + final_partition_str + ' limit ' + max
                        cursor.execute(sql)
                        rows = cursor.fetchall()
                        df = pandas.DataFrame(rows)
                        df.to_csv(cur_path + '/' + table + '.dat', sep=',', header=False, index=False)
                    print 'exported data of table ' + table
            else:
                cursor.execute('select * from ' + table + ' limit ' + max)
                rows = cursor.fetchall()
                if len(rows) != 0:
                    df = pandas.DataFrame(rows)
                    df.to_csv(root + '/' + table + '.dat', sep=',', header=False, index=False)
                    print 'exported data of table ' + table
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
                table_name = file[:len(file) - 4]
                # 修改表字段分隔符为逗号
                cursor.execute("alter table " + table_name + " set serdeproperties('field.delim'=',')")
                # 修改hive表inputformat和outputformat
                cursor.execute(
                    "alter table " + table_name + " set fileformat INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'")
                print 'created table ' + table_name
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()


# 递归得到文件路径
def get_file_path(parent):
    list = []
    cur = os.path.abspath(parent)
    listdir = os.listdir(parent)
    for dir in listdir:
        if os.path.isdir(cur + '/' + dir):
            child = get_file_path(cur + '/' + dir)
            list += child
        else:
            list.append(cur + '/' + dir)
    return list


def import_data2(in_path):
    try:
        root = os.path.abspath(in_path)
        for file in os.listdir(root):
            if os.path.isdir(root + '/' + file):
                partition_datas = get_file_path(root + '/' + file)
                for partition_data in partition_datas:
                    splits = partition_data.split('/')
                    partition_str = partition_data[len(root + '/' + file) + 1:-len(splits[-1]) - 1]
                    partition_str_split = partition_str.split('/')
                    new_partitions = []
                    for p in partition_str_split:
                        new_partitions.append(format_partition(p))
                    new_partition_str = ','.join(new_partitions)
                    sql = "load data local inpath '" + partition_data + "' overwrite into table " + file + ' partition(' + new_partition_str + ')'
                    cursor.execute(sql)
                    print 'loaded data into table ' + file
            else:
                table = file[:len(file) - 4]
                cursor.execute(
                    "load data local inpath '" + os.path.abspath(
                        in_path) + '/' + file + "' overwrite into table " + table)
                print 'loaded data into table ' + table
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    # 设置调用utf-8编码处理字符流
    reload(sys)
    sys.setdefaultencoding('utf-8')

    parser = configparser.ConfigParser()
    parser.read(filenames=['config.ini'])
    database = parser.get('hive', 'database')
    host = parser.get('hive', 'host')
    port = parser.get('hive', 'port')
    partition_count = int(parser.get('hive', 'partitions'))
    filter = parser.get('hive', 'filter')
    username = 'admin'
    password = 'admin'

    conn = hive.Connection(host=host,
                           port=int(port),
                           auth="CUSTOM",
                           database=database,
                           username=username,
                           password=password)
    cursor = conn.cursor()
    if not os.path.exists('./table/tables.txt'):
        get_tables()

    action, target, in_path, start, end, max = sys.argv[1:]
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
