import json
from biocypher import BioCypher
import logging
import yaml
import os
from pathlib import Path
from dotenv import load_dotenv

# Setup basic logging
logging.basicConfig(level=logging.DEBUG)

# Load environmental variables
load_dotenv()

class SchemaManager:
    def __init__(self):
        self.schema_path = Path(__file__).parent / ".." / ".." / "config" / "schema"
        self.schema_list = self.get_schema_list()
        self.schema_representation = self.get_schema_representation(self.schema_list)
    
    def get_schema_list(self):
        schema_list = []
        
        for file in os.listdir(self.schema_path):
            file_name = os.path.splitext(file)[0]
            schema_list.append(file_name)
            
        return schema_list
    
    def get_schema_representation(self, schema_list: list):
        schema_representation = {"nodes": {}, "edges": {}}
        
        for schema in schema_list:
            schema_abs_path = str((self.schema_path / f"{schema}.yaml").resolve())
            with open(schema_abs_path, 'r') as file:
                file_output = yaml.safe_load(file)
                nodes = file_output.get('nodes', {})
                edges = file_output.get('relationships', {})
                name = file_output.get('name', None).upper()
                
                if name:
                    if name not in schema_representation:
                        schema_representation[name]= {'nodes': set(), 'edges': {}}

                    for _, value in nodes.items():
                        if 'input_label' in value:
                            key = value['input_label']
                        else:
                            key = value['output_label']

                        key = key
                        schema_representation[name]['nodes'].add(key)
                        if key not in schema_representation['nodes']:
                            schema_representation['nodes'][key] = {}
                        node_props = {
                            "label": value.get("input_label", ''),
                            "properties": value.get("properties", {}),
                        }
                        schema_representation['nodes'][key].update(node_props)

                    for _, value in edges.items():
                        if 'output_label' in value:
                            key = value['output_label']
                        else:
                            key = value['input_label']
                        # Global edge definition (accumulated across all schemas)
                        if key not in schema_representation['edges']:
                            schema_representation['edges'][key] = {}

                        # Schema-specific edge definition (per schema name)
                        if key not in schema_representation[name]['edges']:
                            schema_representation[name]['edges'][key] = {'source': '', 'target': ''}

                        schema_representation['edges'][key].update(value)
                        schema_representation[name]['edges'][key]['source'] = value.get('source', '')
                        schema_representation[name]['edges'][key]['target'] = value.get('target', '')

        return schema_representation   
