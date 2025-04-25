import csv
import re
import ast
import pprint
import json
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

class CounterExtractor(ast.NodeVisitor):
    """Subclass ast.nodeVistor to define
    what happens when we visit different parts of the tree."""
    def __init__(self):
        self.counters = {}

    def infer_type(self, equipement):
        for key, val in EQUIPEMENT_TYPE_MAP.items():
            if key in equipement:
                return val
        return "UNKNOWN"

    def visit_Name(self, node):
        #override the visit_Name which is called every time we find a variable
        if node.id not in self.counters:
            self.counters[node.id] = {"has_suffix": False}

    def visit_Attribute(self, node):
        full_attr = self._get_full_attr(node)
        root, * suffix = full_attr.split('.')
        suffix_str = '_'.join(suffix) if suffix else None
        if root not in self.counters:
            self.counters[root] = {"has_suffix": False}
        if suffix:
            self.counters[root]["has_suffix": True]
        
        self.generic_visit(node)

    def _get_full_attr(self, node):
        if isinstance(node.value, ast.Name):
            return f"{node.value.id}.{node.attr}"
        elif isinstance(node.value, ast.Attribute):
            return f"{self._get_full_attr(node.value)}.{node.attr}"
        else:
            return node.attr

    def transform_formula(self,raw_formula, keep_suffix=False, valid_expression=False):

        cleaned = raw_formula

        if not keep_suffix:
            cleaned = re.sub(r'\.\[[^\]]*\]','',cleaned)
        elif valid_expression:
            cleaned = re.sub(r'\.\[([^\]]+)\]',lambda i: '_' + i.group(1).replace('.','_'),cleaned )

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
        try:
            tree = ast.parse(formula, mode='eval')
            extractor = CounterExtractor()
            extractor.visit(tree)
            return extractor.counters.copy()
        except Exception as e:
            print(f"Error parsing formula: {formula}. Error: {e}")
            return {}
        
    def build_kpi_dict(self, csv_path):
        kpi_dict = defaultdict(list)

        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                kpi_name = row['KPI']
                equipement = row["Equipement"]
                kpi_type = self.infer_type(equipement)
                raw_formula = row["Formule"]

                formula = self.transform_formula(raw_formula, keep_suffix=True, valid_expression=True)
                counters = self.extract_counters(formula)
                
                kpi_dict[kpi_name] = {
                    "type": kpi_type,
                    "expression": formula,
                    "counters": counters,
                }

        return dict(kpi_dict)



if __name__ == "__main__":
    csv_path = 'MGW_5_15.csv'
    extractor = CounterExtractor()
    kpi_dict = extractor.build_kpi_dict(csv_path)

    print(json.dumps(kpi_dict, indent=2, ensure_ascii=False))

    """try:

        with open("full_kpis_doc.py", "w", encoding='utf-8') as f:
            f.write("\n\n# This file is auto-generated using the counters_extractor.py script.\n")
            f.write("KPI Counters \\\n")
            pprint.pprint(kpi_dict, stream=f, width=120)
            print("KPI Counters written to utils.py")
    except Exception as e:
        print(f"Error writing to file: {e}")"""