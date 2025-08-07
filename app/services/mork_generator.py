from mork.mork_client import MORK, ManagedMORK
import os
import glob
from pathlib import Path
from hyperon import MeTTa, SymbolAtom, ExpressionAtom, GroundedAtom
from metta.metta_seralizer import metta_seralizer

class MorkQueryGenerator:
    def __init__(self, dataset_path):
        self.server = self.connect()
        self.metta = MeTTa()
        self.clear_space()
        self.load_dataset(dataset_path)

    def connect(self):
        server = ManagedMORK.connect(url='http://127.0.0.1:8000')
        return server

    def clear_space(self):
        self.server.clear()

    def load_dataset(self, path):
        if not os.path.exists(path):
            raise ValueError(f"Dataset path '{path}' does not exist.")
        paths = glob.glob(os.path.join(path, "**/*.metta"), recursive=True)
        if not paths:
            raise ValueError(f"No .metta files found in dataset path '{path}'.")
        with self.server.work_at("annotation") as annotation:
            for path in paths:
                path = Path(path)
                file_url = path.resolve().as_uri()
                try:
                    annotation.sexpr_import_(file_url).block()
                    print("Data loaded successfully")
                except Exception as e:
                    print(f"Error loading data: {e}")

    def generate_id(self):
        import uuid
        return str(uuid.uuid4())[:8]

    def construct_node_representation(self, node, identifier):
        node_type = node['type']
        node_representation = ''
        for key, value in node['properties'].items():
            node_representation += f' ({key} ({node_type + " " + identifier}) {value})'
        return node_representation

    def run_query(self, query):
        pattern, template = query

        with self.server.work_at("annotation") as annotation:
            annotation.transform(pattern, template).block()
            result = annotation.download("(tmp $x)", "($x)")
            with annotation.work_at("tmp") as tmp:
                tmp.clear()

        metta_result = self.metta.parse_all(result.data)
        return metta_result

    def query_Generator(self, requests, node_map, limit=None, node_only=False):
        # this will do only transfomration
        nodes = requests['nodes']
        predicate_map = {}

        pattern = []
        template = []

        node_representation = ''

        if "predicates" in requests and len(requests["predicates"]) > 0:
            predicates = requests["predicates"]

            init_pred = predicates[0]

            if 'predicate_id' not in init_pred:
                for idx, pred in enumerate(predicates):
                    pred['predicate_id'] = f'p{idx}'
                for predicate in predicates:
                    predicate_map[predicate['predicate_id']] = predicate
            else:
                for predicate in predicates:
                    predicate_map[predicate['predicate_id']] = predicate
        else:
            predicates = None

        #if there is no predicate
        if not predicates:
            for node in nodes:
                node_type = node["type"]
                node_id = node["node_id"]
                node_identifier = '$' + node["node_id"]

                if node["id"]:
                    essemble_id = node["id"]
                    pattern.append(f'({node_type} {essemble_id})')
                    template.append(f'(tmp ({node_type} {essemble_id}))')
                else:
                    if len(node["properties"]) == 0:
                        pattern.append(f'({node_type} ${node_id})')
                    else:
                        pattern.append(self.construct_node_representation(node, node_identifier))
                    template.append(f'(tmp ({node_type} {node_identifier}))')
            query = (tuple(pattern), tuple(template))

            return query
        for predicate in predicates:
            predicate_type = predicate['type'].replace(" ", "_")
            source_id = predicate['source']
            target_id = predicate['target']

            # Handle source node
            source_node = node_map[source_id]
            if not source_node['id']:
                node_identifier = "$" + source_id
                node_representation = self.construct_node_representation(source_node, node_identifier)
                if node_representation != '':
                    pattern.append(node_representation)
                source = f'({source_node["type"]} {node_identifier})'
            else:
                source = f'({str(source_node["type"])} {str(source_node["id"])})'


            # Handle target node
            target_node = node_map[target_id]
            if not target_node['id']:
                target_identifier = "$" + target_id
                node_representation = self.construct_node_representation(target_node, target_identifier)
                if node_representation != '':
                    pattern.append(node_representation)
                target = f'({target_node["type"]} {target_identifier})'
            else:
                target = f'({str(target_node["type"])} {str(target_node["id"])})'


            # Add relationship
            pattern.append(f'({predicate_type} {source} {target})')
            template.append(f'(tmp ({predicate_type} {source} {target}))')

        query = (tuple(pattern), tuple(template))
        return query
