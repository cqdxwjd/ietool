from pyhive import hive

conn = hive.Connection(host='172.30.1.233',
                       port=10000,
                       auth="CUSTOM",
                       database='ietool',
                       username='admin',
                       password='admin')
cursor = conn.cursor()
cursor.execute('show tables')
for result in cursor.fetchall():
    print(result)
cursor.close()
conn.close()
