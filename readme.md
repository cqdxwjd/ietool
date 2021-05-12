### 1.解压

### 2.初始化运行环境

`sudo sh init.sh`

### 3.修改配置文件config.ini

### 4.批量导出ddl

`python src/hive_csv.py export ddl ../ 0 10 100`

### 4.批量导出数据

`python src/hive_csv.py export data ../ 0 10 100`

### 5.批量导入ddl

`python src/hive_csv.py import ddl ddl/ 0 10 100`

### 6.批量导入数据

`python src/hive_csv.py import data data/ 0 10 100`