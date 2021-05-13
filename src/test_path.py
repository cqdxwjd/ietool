import os

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


if __name__ == '__main__':
    path = os.path.abspath('../data')
    file_path = get_file_path(path)
    print file_path
