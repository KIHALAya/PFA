import pandas as pd
import pymysql
import json
import numpy as np
from mysql_tables import mysql_tables, types
import re
from itertools import combinations
from utils import *


pattern = r'mgw'
noeud_5_15 = [each for each in types if not re.search(pattern, each)]
noeud_MGW = [each for each in types if re.search(pattern, each)]


with open("kpis_data.json", 'r', encoding='utf-8') as f:
    kpi_data = json.load(f)


for key, info in kpi_data.items():
    kpi_type = info['type']
    formula = info['expression']
    target_columns = extract_counters_suffixes(info)[0]
    print(target_columns)

    if kpi_type in ["5_min_KPI", "15_min_KPI"]:
        print(f"Processing  {key} table")
        for noeud in noeud_5_15:
            print(f"Processing {key} for {noeud}")
            for table in mysql_tables:
                process_kpi_table(key,formula, noeud,kpi_type,target_columns,table,source_engine, dest_engine )
           
        print(f"Table {key} created successfully.")