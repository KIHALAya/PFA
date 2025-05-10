import pandas as pd
import pymysql
import json
from mysql_tables import mysql_tables, types
import re
from utils import db_config, dest_config, db_connector, source_engine, dest_engine


#conn, cursor = db_connector(db_config)
#conn_dest, cursor_dest = db_connector(dest_config)

table = "calis_apg43_5_s51_a2024"
df = pd.read_sql(f"SELECT * FROM {table}", source_engine)
#print(df.head())

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


sample = list(kpi_data.items())[0]
key, info = sample


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


def compute_kpi(row, formula):
    try: 
        return eval(formula, {}, row.to_dict())
    except Exception as e:
        #print(f"Error evaluating formula: {e}")
        return None
    

def flatten_rows(df, target_columns):
    flattened_rows = []
    df[['counter','suffix']] = df['indicateur'].str.split('.',expand=True)
    df_filtered = df[df['counter'].isin(target_columns)]
    for _, row in df_filtered.iterrows():
        new_row = {
            'Date': row['Date'],
            row['counter']: row['valeur'],
            f"{row['counter']}_suffix": row['suffix'],
        }
        flattened_rows.append(new_row)
    
    final_df = pd.DataFrame(flattened_rows)
    return final_df


flattened_table = flatten_rows(df, extract_counters_suffixes(info)[0])
flattened_table['KPI_value'] =  flattened_table.apply(lambda row: compute_kpi(row, info["expression"]), axis=1) 
non_null_rows = flattened_table[flattened_table['KPI_value'].notna()]


flattened_table.to_sql(key,
    dest_engine,
    if_exists='replace',
    index=False
)
    
#print(flattened_table.sample(5))
#conn.close()
#cursor.close()

#conn_dest.close()
#cursor_dest.close()