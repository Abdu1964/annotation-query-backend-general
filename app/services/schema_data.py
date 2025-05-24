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
        self.schema = {'nodes': {}, 'edges': {}}

    def load_schema(self, schema_path):
        self.scheam = {'nodes': {}, 'edges': {}}
        try:
            with open(schema_path, 'r') as file:
                raw_schema = json.load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        vertex_label = raw_schema.get('vertex_labels', None)
        edge_label = raw_schema.get('edge_labels', None)

        if vertex_label is None:
            raise ValueError("Vertex label is missing in the schema")
        if edge_label is None:
            raise ValueError("Edge label is missing in the schema")

        for node in vertex_label:
            key = node['name']
            self.schema['nodes'][key] = {}
            self.schema['nodes'][key]['properties'] = node.get('properties', {})

        for edge in edge_label:
            key = edge['name']
            self.schema['edges'][key] = {}
            self.schema['edges'][key]['properties'] = edge.get('properties', {})
            self.schema['edges'][key]['source'] = edge.get('source_label', None)
            self.schema['edges'][key]['target'] = edge.get('target_label', None)
