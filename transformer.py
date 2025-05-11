import json
import re
from mysql_tables import mysql_tables
from mysql_tables import types
from utils import db_config, dest_config,db_connector
from collections import defaultdict
from pprint import pprint
import mysql.connector


# connect to source database
conn, cursor = db_connector(db_config)
conn_dest, cursor_dest = db_connector(dest_config)


SUFFIX_OPERATOR_MAPPING = {
    'nw': 'Inwi',
    'mt': 'Maroc Telecom',
    'ie': 'International',
    'is': 'International',
    'bs': 'Orange 2G',
    'be': 'Orange 2G',
    'ne': 'Orange 3G',
    'ns': 'Orange 3G'
}

pattern = r'mgw'
noeud_5_15 = [each for each in types if not re.search(pattern, each)]
noeud_MGW = [each for each in types if re.search(pattern, each)]

fixed_columns = [
    "Date DATE",
    "Type VARCHAR(225)",
    "Noeud VARCHAR(225)",
    "Operator VARCHAR(225)",
    "Status VARCHAR(225)"
]


last_column = ["Valeur DOUBLE "]


#Get KPI data
with open("kpis_data.json", 'r', encoding='utf-8') as f:
    kpi_data = json.load(f)

def get_status_from_kpi_name(kpi_name):
    kpi_name_lower = kpi_name.lower()
    if 'in' in kpi_name_lower or kpi_name.endswith('_E'):
        return 'In'
    elif 'out' in kpi_name_lower or kpi_name.endswith('_S'):
        return 'Out'
    return None


def extract_counters_suffixes(kpi_info):
    counters = []
    suffixes = []
    for counter in kpi_info["counters"]:
        has_suffix = kpi_info["counters"][counter]["has_suffix"]
        if has_suffix:
            counters.append(counter)
            suffixes.append(f"{counter}_suffix")
        else:
            counters.append(counter)
    return counters, suffixes


def create_sql_tables(kpi_name, kpi_info):
    table_name = f"{kpi_name}"

    counter_columns = [f"`{counter}` DOUBLE" for counter in extract_counters_suffixes(kpi_info)[0]]
    suffix_columns = [f"`{suffix}` VARCHAR(225)" for suffix in extract_counters_suffixes(kpi_info)[1]]

    total_columns  = fixed_columns + counter_columns + suffix_columns + last_column

    sql_columns = ',\n'.join(total_columns)

    sql_schema = f"""
      CREATE TABLE IF NOT EXISTS {table_name} (
      {sql_columns}
      )
    """

    try:
        cursor_dest.execute(sql_schema)
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        return


def apply_formula(formula, grouped_data, kpi, noeud):
    for date, row_data in grouped_data.items():
        local_vars = {}
        missing_counter = False

        for counter in extract_counters_suffixes(kpi)[0]:
            val = row_data.get(counter)
            if val is None:
                missing_counter = True
                break
            local_vars[counter] = val

        if missing_counter:
            print(f"Missing counter for date {date}, skipping calculation.")
            continue

        try:
            return eval(formula, {}, local_vars)
        
        except Exception as e:
            print(f"Error evaluating formula for date {date}: {e}")
            return None

        
def pivot_table(table, kpi):

    grouped_data = defaultdict(dict)
    counters, _ = extract_counters_suffixes(kpi)

    for counter in counters:
        query = f"SELECT * FROM {table} WHERE `indicateur` LIKE '{counter}%' "
        try : 
            cursor.execute(query)
            #print(f"Executing query: {query}")
            raw_data = cursor.fetchall()

            if not raw_data:
                print("No data found for the query.")
                continue
             
            print("Data found")
            for row in raw_data:
                indicator = row.get("indicateur", "")
                value = row.get("valeur")
                date = row.get("Date")

                if not date:
                    #print("Date is None, skipping row.")
                    continue

                if '.' in indicator:
                    counter, suffix = indicator.split('.',1)
                else:
                    counter, suffix = indicator, None

                
                grouped_data[date][counter] = value
                if suffix:
                    if 'suffix' not in grouped_data[date]:
                        grouped_data[date]['suffix'] = {}
                    grouped_data[date]['suffix'][counter] = suffix

        except Exception as e:
                print(f"Error executing query: {e}")

    return grouped_data
            

def build_row(date, row_data,kpi_name, kpi_info,kpi_value, noeud):
    counters, suffixes = extract_counters_suffixes(kpi_info)
    operator =  'Unknown'

    suffix_dict  = row_data.get('suffix', {})
    for suffix in suffix_dict.values():
        operator = SUFFIX_OPERATOR_MAPPING.get(suffix, 'Unknown')
        if operator != 'Unknown':
            break

    suffix_values = {}
    for counter, suffix in suffix_dict.items():
        suffix_values[f'{counter}_suffix'] = suffix

    status = get_status_from_kpi_name(kpi_name)


    return {
        "Date": date,
        "Type": kpi_info["type"],
        "Noeud": noeud,
        "Operator": operator,
        "status": status,
        **{counter: row_data.get(counter) for counter in counters},
        **suffix_values,
        "Valeur": kpi_value
    }


def insert_row(data_dict, kpi_key, cursor, db_connection):
    columns = ",".join(f"`{col}`" for col in data_dict)
    """placeholders = ",".join(["%s"] * len(data_dict))
    values = tuple(data_dict.values())
    sql = f"INSERT INTO {kpi_key} ({columns}) VALUES {placeholders}"
    try:
        cursor.execute(sql, values)
        db_connection.commit()
        print(f"Inserted into {kpi_key} : {values}")
    except Exception as e:
        print(f"Error inserting row: {e}")"""
    
    def format_values(val):
        if val is None:
            return "NULL"
        elif isinstance(val, str):
            return f"'{val}'"
        elif isinstance(val, (int, float)):
            return str(val)
        elif hasattr(val, 'quantize'):
            return f"'{val}'"
        else:
            return f"'{str(val)}'"
        
    formatted_values = ",".join(format_values(v) for v in data_dict.values())
    sql = f"INSERT INTO {kpi_key} ({columns}) VALUES ({formatted_values})"
    print(sql)
    """try:
        cursor.execute(sql)
        db_connection.commit()
        print(f"Inserted into {kpi_key} : {formatted_values}")
    except Exception as e:
        print(f"Error inserting row: {e}")"""
        


sample = list(kpi_data.items())[0]
key, info = sample

    #print(f"Processing table: {table}")
if re.match(f"{noeud_5_15[1]}_", "calis_apg43_5_s51_a2024"):
    #print(f"Table {table} matches the pattern {noeud_5_15[0]}")
    grouped_data = pivot_table("calis_apg43_5_s51_a2024", info )
    counters, _ = extract_counters_suffixes(info)

    for date, row_data in grouped_data.items():
        kpi_value = apply_formula(info["expression"], grouped_data, info, noeud_5_15[0])
        insert_data = build_row(date, row_data, key, info, kpi_value, noeud_5_15[0])
        pprint(f"Insert data: {insert_data}")
        insert_row(insert_data, key, cursor_dest, conn_dest)
    

def process_kpi_for_table(table,formula, kpi_name, kpi_info, noeud_list): 
    for noeud in noeud_list:
        if re.match(rf"{noeud}_", table):
            print("Process Started")
            grouped_data = pivot_table(table, kpi_info)
            counters, _ = extract_counters_suffixes(kpi_info)

            for date, row_data in grouped_data.items():
                kpi_value = apply_formula(formula, grouped_data,kpi_info, noeud)
                insert_data = build_row(date, row_data,kpi_name, kpi_info, kpi_value, noeud)
                print(f"Insert data: {insert_data}")
                insert_row(insert_data, kpi_name, cursor_dest, conn_dest)


"""for kpi_name, kpi_info in kpi_data.items():
    kpi_type = kpi_info["type"]
    formula = kpi_info["expression"]
    counters = kpi_info["counters"] 

    #create_sql_tables(kpi_name, kpi_info)
    print(f"Creating table for {kpi_name}")

    if kpi_type in ["5_min_KPI", "15_min_KPI"]:
        for table in mysql_tables:
            process_kpi_for_table(table, formula, kpi_name, kpi_info, noeud_5_15)
            print(f"Processed KPI for table {table} and KPI {kpi_name}")
"""
          
conn.close()
cursor.close()

conn_dest.close()
cursor_dest.close()