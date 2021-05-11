### 1.解压
### 2.初始化运行环境
`sudo sh init.sh`
### 3.导出全部ddl
`python src/hive_csv.py export ddl ../`
### 4.导出全部数据,单表最多只能导出100万条数据
`python src/hive_csv.py export data ../`
### 5.导入全部ddl
`python src/hive_csv.py import ddl ddl/`
### 6.导入全部数据
`python src/hive_csv.py import data data/`