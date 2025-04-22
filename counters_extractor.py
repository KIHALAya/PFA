import csv
import re
import ast
import pprint
from  collections import defaultdict

"""
We're going through each row in our CSV and :
    * Getting an KPI name.
    * Extracting a list of counters used in its formula.
    * Storing all those counters under corresponding KPI in a dictionary.
"""

class CounterExtractor(ast.NodeVisitor):
    """Subclass ast.nodeVistor to define
    what happens when we visit different parts of the tree."""
    def __init__(self):
        self.names = set()

    def visit_Name(self, node):
        #override the visit_Name which is called every time we find a variable
        self.names.add(node.id)

    def clean_formula(self,raw_formula):
        cleaned = raw_formula

        # Remove the dot + [] 
        cleaned = re.sub(r'\.\[[^\]]*\]', '', cleaned)

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
            return list(extractor.names)
        except Exception as e:
            print(f"Error parsing formula: {formula}. Error: {e}")
            return []
        
    def build_kpi_dict(self, csv_path):
        kpi_dict = defaultdict(list)

        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                kpi = row['KPI']
                formula = self.clean_formula(row['Formule'])
                counters = self.extract_counters(formula)
                kpi_dict[kpi].extend(counters)

        return dict(kpi_dict)



if __name__ == "__main__":
    csv_path = 'BC-Compteur.csv'
    extractor = CounterExtractor()
    kpi_dict = extractor.build_kpi_dict(csv_path)

    try:

        with open("kpi_data", "w", encoding='utf-8') as f:
            f.write("\n\n# This file is auto-generated using the counters_extractor.py script.\n")
            f.write("KPI Counters \\\n")
            pprint.pprint(kpi_dict, stream=f, width=120)
            print("KPI Counters written to utils.py")
    except Exception as e:
        print(f"Error writing to file: {e}")