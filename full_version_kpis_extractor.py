import csv
import re
import json
import ast
import pprint
from  collections import defaultdict

"""
We're going through each row in our CSV and :
    * Getting an KPI name.
    * Extracting a list of counters used in its formula.
    * Storing all those counters under corresponding KPI in a dictionary.
"""

EQUIPEMENT_TYPE_MAP = {
    "BC 5 minutes": "5_min_KPI",
    "BC 15 minutes": "15_min_KPI",
    "MGW 15 minutes": "MGW_KPI"
}

class KPIProcessor(ast.NodeVisitor):
    """Subclass ast.nodeVistor to define
    what happens when we visit different parts of the tree."""
    def __init__(self):
        self.variables = set()

    def visit_Name(self, node):
        self.variables.add(node.id)
        self.generic_visit(node)

    def infer_type(self, equipement):
        for key, val in EQUIPEMENT_TYPE_MAP.items():
            if key in equipement:
                return val
        return "UNKNOWN"

    def transform_formula(self,raw_formula, keep_suffix=False):

        cleaned = raw_formula

        if  keep_suffix:
            cleaned = re.sub(r'\.\[([^\]]+)\]',lambda i: '_' + i.group(1).replace('.','_'),cleaned )
        else :
            cleaned = re.sub(r'\.\[[^\]]*\]','',cleaned)
            

        operator_map = {
            r'\{\+\}' : '+',
            r'\{\-\}': '-',
            r'\{\*\}' : '*',
            r'\{/\}' : '/',
            r'\{\\\}' : '/'
        }

        for pattern, replacement in operator_map.items():
            cleaned = re.sub(pattern, replacement, cleaned)

        #cleaned = re.sub(r'[\{\}\[\]]', '', cleaned)
        cleaned = cleaned.strip()

        return cleaned
    

    def extract_counters(self, formula):
        self.variables.clear()

        try:
            tree = ast.parse(formula, mode='eval')
            self.visit(tree)
        except Exception as e:
            print(f"Error parsing formula: {formula}. Error: {e}")
            return {}
        
        # pattern = r'\b([^_()\s]+)(?:_([^()\s]+))?\b'
        # matches = re.findall(pattern, formula)

        counters = {}

        for var in self.variables:
            parts = var.split('_',1)
            counter = parts[0]
            has_suffix = len(parts) > 1

            if counter not in counters:
                counters[counter] = {"has_suffix": has_suffix}

        return counters  




    def build_kpi_dict(self, csv_path):
        kpi_dict = defaultdict(list)

        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                kpi_name = row['KPI']
                equipement = row["Equipement"]
                raw_formula = row["Formule"]

                kpi_type = self.infer_type(equipement)

                formula = self.transform_formula(raw_formula, keep_suffix=True)
                counters = self.extract_counters(formula)
                
                kpi_dict[kpi_name] = {
                    "type": kpi_type,
                    "expression": formula,
                    "counters": counters,
                }

        return dict(kpi_dict)


if __name__ == "__main__":
    csv_path = 'MGW_5_15.csv'
    kpi_processor = KPIProcessor()
    kpi_dict = kpi_processor.build_kpi_dict(csv_path)
    

    try:

        with open("kpis_data.json", "w", encoding='utf-8') as f:            
            json.dump(kpi_dict, f, indent=4)

    except Exception as e:
        print(f"Error writing to file: {e}")

    print("kpis data json file created successfully")