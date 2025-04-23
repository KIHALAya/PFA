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

def extract_types(tables):
    types = []
    patterns = r"^[^_]+"
    for table in tables:
        types.append(re.match(patterns, table).group(0))
    
    return types

extracted_patterns = set(extract_types(mysql_tables))
with open("mysql_tables.py", "a", encoding='utf-8') as f:
    f.write("types = [\n")
    for i in extracted_patterns:
        f.write(f"'{i}',\n")
    f.write("]")

print("Types extracted and written to mysql_tables.py")
    



"""extracted_patterns = extract_types(mysql_tables)
for i in extracted_patterns:
    print(i)

print("Unique types:")
unique_types = set(extracted_patterns)
for i in unique_types:
    print(i)"""