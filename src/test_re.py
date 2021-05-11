import re

if __name__ == '__main__':
    str = 'fields terminated by'
    search = re.search('fields terminated', str, re.I)
    print search.start()
    print search.end()
