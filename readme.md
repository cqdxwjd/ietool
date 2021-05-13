### 1.解压

### 2.初始化运行环境

`sudo sh init.sh`

### 3.修改配置文件config.ini

### 4.批量导出ddl

`python src/hive_csv.py export ddl ../ 0 10 100`

### 5.批量导出数据，空表会跳过

`python src/hive_csv.py export data ../ 0 10 100`

### 6.压缩数据并下载ietool-export.tar.gz

`tar -zcvf ietool-export.tar.gz ietool-0.0.1`


### 7.解压缩ietool-export.tar.gz，修改config.ini

### 8.批量导入ddl

`python src/hive_csv.py import ddl ddl/ 0 10 100`

### 9.批量导入数据

`python src/hive_csv.py import data data/ 0 10 100`

## 命令参数说明
* table/table.txt中包含了源hive库所有的表名称
* 各参数分别表示：操作类型(导入/导出)，操作对象(ddl/data)，table/table.txt中起始位置(从0开始)，结束位置，每个表导出的最大数据条数(如果是分区表，指导出最新分区的最大条数)