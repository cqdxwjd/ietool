import configparser
import os

if __name__ == '__main__':
    parser = configparser.ConfigParser()
    parser.read(filenames=['../config.ini'])
    print parser.get('hive', 'database')
    print parser.get('hive', 'host')
    print parser.get('hive', 'port')
