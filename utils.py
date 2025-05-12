import ijson
import re
from mysql_tables import mysql_tables
from mysql_tables import types
import mysql.connector
from  sqlalchemy import create_engine
import pymysql
import os
import pandas as pd 
import numpy as np
from itertools import combinations
import json

db_config = {
    "host": "127.0.0.1",
    "port": 3307,
    "user": "root",
    "password": os.environ.get("MYSQL_ROOT_PASSWORD"),
    "database": os.environ.get("MYSQL_DATABASE"),
}


dest_config = {
    "host": "127.0.0.1",
    "port": 3307,
    "user": "root",
    "password": os.environ.get("MYSQL_ROOT_PASSWORD"),
    "database": os.environ.get("DEST_DATABASE"),
}

def db_connector(config):
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor(buffered=True, dictionary=True)
        print("Connected Successfully")
        return conn, cursor
    except Exception as e:
        print(f"Error: {e}")
        return None, None


source_engine = create_engine(
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)


dest_engine = create_engine(
    f"mysql+pymysql://{dest_config['user']}:{dest_config['password']}@{dest_config['host']}:{dest_config['port']}/{dest_config['database']}"
)

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

"""with open("mysql_tables.py", "w", encoding='utf-8') as f:
    f.write("mysql_tables = [\n")
    for table in list(tables):
        f.write(f"'{table[0]}',\n")
    f.write("]")
"""

def extract_types(tables):
    types = []
    patterns = r"^[^_]+"
    for table in tables:
        types.append(re.match(patterns, table).group(0))
    
    return types

"""extracted_patterns = set(extract_types(mysql_tables))
with open("mysql_tables.py", "a", encoding='utf-8') as f:
    f.write("types = [\n")
    for i in extracted_patterns:
        f.write(f"'{i}',\n")
    f.write("]")"""


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

def merge_rows(rows, target_columns):
    merged_results = []

    row_dicts = [row.dropna().to_dict() for _, row in rows.iterrows()]
    n = len(target_columns)

    for combo in combinations(row_dicts, n):
        merged = {}
        conflict = False
        for d in combo:
            for k, v in d.items():
                if k in merged and merged[k] != v:
                    break
                merged[k] = v
            if conflict:
                break
        if not conflict:
            merged_results.append(merged)

    return merged_results

def process_kpi_table(key, formula, noeud, kpi_type, target_columns, table, source_engine, dest_engine):
    if re.match(rf"{noeud}_", table):
        df = pd.read_sql(f"SELECT * FROM {table}", source_engine)
        print("Fetched Raw Data")
        print(df.head())
        flattened_table = flatten_rows(df,target_columns)
        if flattened_table.empty:
            print(f"No data found for {table}.")
            return
        print("Flattened Data")
        print(flattened_table.head())
        final_rows = []
        for date, group in flattened_table.groupby('Date'):
            merged = merge_rows(group, target_columns)
            for row in merged:
                row['Date'] = date
                final_rows.append(row)


        final_df = pd.DataFrame(final_rows).drop_duplicates()
        final_df = final_df.dropna(subset=target_columns)
        try:
            final_df['KPI_value'] = pd.eval(formula, engine='numexpr', local_dict=final_df)
        except Exception as e:  
            print(f"Error evaluating formula: {e}")
            final_df['KPI_value'] = None


        final_df = final_df[~final_df['KPI_value'].isin([float('inf'), -float('inf')])]
        final_df = final_df.dropna(subset=['KPI_value'])
        final_df['Type'] = kpi_type
        final_df['Status'] = final_df.apply(lambda x: get_status_from_kpi_name(key), axis=1)
        final_df['Noeud'] =  noeud 


        final_df.to_sql(key,
            dest_engine,
            if_exists='append',
            index=False
        )