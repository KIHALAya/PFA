import json
import time

start_time = time.time()

try:
    print("Opening JSON file...")
    with open('etl_process_backup.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    print("JSON file successfully loaded.")

    table_count = len(data)
    print(f"Total tables found: {table_count}")

    with open("data_parser.sql", "w", encoding='utf-8') as f:
        for table_index, (table_name, rows) in enumerate(data.items(), start=1):
            if isinstance(rows, list) and rows:
                print(f"[{table_index}/{table_count}] Processing table '{table_name}' with {len(rows)} rows...")

                columns = rows[0].keys()
                f.write(f"CREATE TABLE {table_name} ({', '.join(f'{col}' for col in columns)});\n")

                for row_index, row in enumerate(rows, start=1):
                    values = "', '".join(str(row.get(col, '')) for col in columns)
                    f.write(f"INSERT INTO {table_name} VALUES ('{values}');\n")

                    if row_index % 1000 == 0:
                        print(f"  --> {row_index} rows written for '{table_name}'")

    print("✅ SQL file 'data_parser.sql' has been successfully created.")
    print(f"⏱️ Total time taken: {round(time.time() - start_time, 2)} seconds")

except Exception as e:
    print("❌ An error occurred:", str(e))
