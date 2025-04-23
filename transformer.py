import ijson
import re
from db_config import db_config
from mysql_tables import mysql_tables
import mysql.connector

"""try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    print("Connected Successfully")
    sql = "SHOW TABLES"
    cursor.execute(sql)
    tables = cursor.fetchall()
except Exception as e:
    print(f"Error: {e}")

with open("mysql_tables.py", "w", encoding='utf-8') as f:
    f.write("mysql_tables = [\n")
    for table in list(tables):
        f.write(f"'{table[0]}',\n")
    f.write("]")"""

for table in mysql_tables:
    print(f"{table}")