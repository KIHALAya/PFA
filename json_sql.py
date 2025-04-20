import ijson
from decimal import Decimal
import mysql.connector
import os


config = {
    "host": "127.0.0.1",
    "port": 3307,
    "user": os.environ.get("MYSQL_USER"),
    "password": os.environ.get("MYSQL_PASSWORD"),
    "database": os.environ.get("MYSQL_DATABASE"),
}


try:
     print("Connection to Mysql DB")
     conn = mysql.connector.connect(**config)
     cursor = conn.cursor()
     print("Connected Successfully")
except Exception as e:
     print(f"Error: {e}")


file_path  = "etl_process_backup.json"

with open(file_path, "rb") as f:
    parser = ijson.parse(f)
    table_names = set()

    for prefix, event, value in parser:
        if prefix == '' and event == 'map_key':
            table_names.add(value)


def get_rows(file_path, key, n=None):
        rows = []
        with open(file_path, "rb") as f:
            items = ijson.items(f, f'{key}.item')
            if n is None:
                 for item in items:
                      rows.append(item)
            else:
                 for i, item in enumerate(items):
                      if i >= n :
                           break
                      rows.append(item)   

        return rows  

  
def infer_sql_schema(table_name, rows):
        cols = {}
        for row in rows:
            for k, v in row.items():
                if k not in cols:
                    if isinstance(v, int):
                        cols[k] = 'INT'
                    elif isinstance(v, float):
                        cols[k] = 'FLOAT'
                    elif isinstance(v, bool):
                        cols[k] = 'BOOLEAN'
                    elif isinstance(v, str) and len(v) > 255:
                        cols[k] = 'TEXT'
                    elif isinstance(v, Decimal):    
                        cols[k] = 'DECIMAL(15, 5)'
                    else:
                        cols[k] = 'VARCHAR(225)'

        columns_sql = ',\n '.join([f"`{k}` {v}" for k, v in cols.items()])
        return f"CREATE TABLE `{table_name}`(\n {columns_sql}\n);"


def stream_insert_data(table_name, rows):
     cols = rows[0].keys()
     placeholders = ', '.join(['%s'] * len(cols))
     insert_query = f"INSERT INTO `{table_name}`({', '.join(cols)}) VALUES ({placeholders})"
     for row in rows:
          values = [row.get(col) for col in cols]
          try:
               cursor.execute(insert_query, values)
          except Exception as e:
               print(f"Error inserting data into {table_name}: {e}")
     conn.commit()

for table_name in table_names:
     print(f"Processing: {table_name}")
     sample = get_rows(file_path, table_name, 1)
     sql_schema = infer_sql_schema(table_name, sample)
     print("SQL Schema:")
     print(sql_schema)
     print("\n")
     cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
     cursor.execute(sql_schema)
     rows = get_rows(file_path, table_name)
     stream_insert_data(table_name, rows)
          

        
cursor.close()
conn.close()



    

"""parser = ijson.parse(f)
    for prefix, event, value in parser:
        if prefix == '' and event == 'map_key':
            print('Top level key:', value)

    caism = ijson.items(f, 'caism2mgw_s01_a2025.item')
    for i, each in enumerate(caism):
        print(each)  
        if i >= 4:
            break"""