# coding=utf-8

import os

lines = ['aaa,zhangsan\n', 'bbb,lisi\n', 'ccc,wangwu\n']
with open('data/test2.dat', 'a') as f:
    for i in range(1000000):
        f.writelines(lines)
    f.close()
