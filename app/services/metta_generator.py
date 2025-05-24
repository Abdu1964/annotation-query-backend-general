import glob
import os
from hyperon import MeTTa, SymbolAtom, ExpressionAtom, GroundedAtom
import logging
from .query_generator_interface import QueryGeneratorInterface
from .metta import Metta_Ground, metta_seralizer
# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MeTTa_Query_Generator(QueryGeneratorInterface):
    def __init__(self, dataset_path: str):
        self.metta = MeTTa()
        self.initialize_space()
        self.dataset_path = dataset_path
        self.load_dataset(self.dataset_path)
        self.initialize_grounatoms()

    def initialize_space(self):
        self.metta.run("!(bind! &space (new-space))")

    def initialize_grounatoms(self):
        Metta_Ground(self.metta)

    def load_dataset(self, path: str) -> None:
        if not os.path.exists(path):
            raise ValueError(f"Dataset path '{path}' does not exist.")
        paths = glob.glob(os.path.join(path, "**/*.metta"), recursive=True)
        if not paths:
            raise ValueError(f"No .metta files found in dataset path '{path}'.")
        for path in paths:
            logging.info(f"Start loading dataset from '{path}'...")
            try:
                self.metta.run(f'''
                    !(load-ascii &space {path})
                    ''')
            except Exception as e:
                logging.error(f"Error loading dataset from '{path}': {e}")
        logging.info(f"Finished loading {len(paths)} datasets.")

    def generate_id(self):
        import uuid
        return str(uuid.uuid4())[:8]

    def construct_node_representation(self, node, identifier):
        node_type = node['type']
        node_representation = ''
        for key, value in node['properties'].items():
            node_representation += f' ({key} ({node_type + " " + identifier}) {value})'
        return node_representation

    def query_Generator(self, requests ,node_map, limit=None, node_only=False):
        nodes = requests['nodes']
        predicate_map = {}

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


        match_preds = []
        return_preds = []
        node_representation = ''

        match_clause = '''!(match &space (,'''
        return_clause = ''' ('''
        metta_output = ''

        # if there is no predicate
        if not predicates:
            for node in nodes:
                node_type = node["type"]
                node_id = node["node_id"]
                node_identifier = '$' + node["node_id"]

                if node["id"]:
                    essemble_id = node["id"]
                    match_preds.append(f'({node_type} {essemble_id})')
                    return_preds.append(f'({node_type} {essemble_id})')
                else:
                    if len(node["properties"]) == 0:
                        match_preds.append(f'({node_type} ${node_id})')
                    else:
                        match_preds.append(self.construct_node_representation(node, node_identifier))
                    return_preds.append(f'({node_type} {node_identifier})')

            query_clause = {
                "match_preds": match_preds,
                "return_preds": return_preds
            }

            if node_only:
                queries = []
                for i, match_query in enumerate(match_preds):
                    queries.append(f'{match_clause} {match_query}) ({return_preds[i]}))')

                queries = ' '.join(queries)
                return [queries, None, None]

            count_query = self.count_query_generator(query_clause, node_only=True)
            match_clause += ' '.join(match_preds)
            return_clause += ' '.join(return_preds)
            metta_output += f'{match_clause}){return_clause}))'

            return [metta_output, count_query[0], count_query[1]]

        for predicate in predicates:
            predicate_type = predicate['type'].replace(" ", "_")
            source_id = predicate['source']
            target_id = predicate['target']

            # Handle source node
            source_node = node_map[source_id]
            if not source_node['id']:
                node_identifier = "$" + source_id
                node_representation += self.construct_node_representation(source_node, node_identifier)
                source = f'({source_node["type"]} {node_identifier})'
            else:
                source = f'({str(source_node["type"])} {str(source_node["id"])})'


            # Handle target node
            target_node = node_map[target_id]
            if not target_node['id']:
                target_identifier = "$" + target_id
                node_representation += self.construct_node_representation(target_node, target_identifier)
                target = f'({target_node["type"]} {target_identifier})'
            else:
                target = f'({str(target_node["type"])} {str(target_node["id"])})'


            # Add relationship
            match_preds.append(f'{node_representation} ({predicate_type} {source} {target})')
            return_preds.append((predicate_type, source, target))

        query_clause = {
            "match_preds": match_preds,
            "return_preds": return_preds
        }
        count = self.count_query_generator(query_clause, node_only=False)
        match_clause += ' '.join(match_preds)
        return_output = []
        for returns in return_preds:
            predicate_type, source, target = returns
            return_output.append(f'({predicate_type} {source} {target})')
        return_clause += ' '.join(return_output)
        metta_output += f'{match_clause}){return_clause}))'

        return [metta_output, count[0], count[1]]

    def count_query_generator(self, query_clauses, node_only):
        metta_output = '''(match &space (,'''
        output = ''' ('''

        match_clause = ' '.join(query_clauses['match_preds'])
        return_clause = []

        for returns in query_clauses['return_preds']:
            if node_only:
                return_clause.append(f'(node {returns})')
            else:
                predicate_type, source, target = returns
                return_clause.append(f'((edge {predicate_type}) (node {source}) (node {target}))')


        output += ' '.join(return_clause)

        metta_output += f'{match_clause}){output}))'

        total_count_query = f'''!(total_count (collapse {metta_output}))'''
        lable_count_query = f'''!(label_count (collapse {metta_output}))'''

        return [total_count_query, lable_count_query]


    def run_query(self, query_code, stop_event=True):
        return self.metta.run(query_code)

    def parse_and_serialize(self, input, schema, graph_components, result_type):
        if result_type == 'graph':
            query, result = self.prepare_query_input(input, schema)
            tuples = metta_seralizer(result[0])

            if not tuples:
                nodes, edges = self.parse_and_seralize_no_properties(query)
                return {"nodes": nodes, "edges": edges,
                        "node_count": 0,
                        "edge_count": 0,
                        "node_count_by_label": [],
                        "edge_count_by_label": []
                }
            else:
                result = self.parse_and_serialize_properties(result, graph_components, result_type)
            return result
        else:
            (_,_,_,_, meta_data) = self.process_result(input, graph_components, result_type)
            return {
                "node_count": meta_data.get('node_count', 0),
                "edge_count": meta_data.get('edge_count', 0),
                "node_count_by_label": meta_data.get('node_count_by_label', []),
                "edge_count_by_label": meta_data.get('edge_count_by_label', []),
            }

    def parse_and_seralize_no_properties(self, results):
        nodes = set()
        edges = []

        for result in results:
            nodes.add(result['source'])
            nodes.add(result['target'])

            source_label = result['source'].split(' ')[0]
            target_label = result['target'].split(' ')[0]
            edges.append({
                "data": {
                    "id": self.generate_id(),
                    "edge_id": f'{source_label}_{result["predicate"]}_{target_label}',
                    "label": result['predicate'],
                    "source": result['source'],
                    "target": result['target']
                }
            })

        nodes_list = []

        for node in nodes:
            nodes_list.append({
                "data": {
                    "id": node,
                    "type": node.split(' ')[0]
                }
            })

        return nodes_list, edges

    def parse_and_serialize_properties(self, input, graph_components, result_type):
        (nodes, edges, _, _, meta_data) = self.process_result(input, graph_components, result_type)
        return {"nodes": nodes[0], "edges": edges[0],
                "node_count": meta_data.get('node_count', 0),
                "edge_count": meta_data.get('edge_count', 0),
                "node_count_by_label": meta_data.get('node_count_by_label', []),
                "edge_count_by_label": meta_data.get('edge_count_by_label', [])
        }

    def get_node_properties(self, results, schema):
        metta = ('''!(match &space (,''')
        output = (''' (,''')
        nodes = set()
        for result in results:
            source = result['source']
            source_node_type = result['source'].split(' ')[0]

            if source not in nodes:
                for property in schema['nodes'][source_node_type]['properties']:
                    id = self.generate_id()
                    metta += " " + f'({property} ({source}) ${id})'
                    output += " " + f'(node {property} ({source}) ${id})'
                nodes.add(source)

            if "target" in result and "predicate" in result:
                target = result['target']
                target_node_type = result['target'].split(' ')[0]
                if target not in nodes:
                    for property in schema['nodes'][target_node_type]['properties']:
                        id = self.generate_id()
                        metta += " " + f'({property} ({target}) ${id})'
                        output += " " + f'(node {property} ({target}) ${id})'
                    nodes.add(target)

                predicate = result['predicate']
                for property in schema['edges'][predicate]['properties']:
                    random = self.generate_id()
                    metta += " " + f'({property} ({predicate} ({source}) ({target})) ${random})'
                    output +=  " " + f'(edge {property} ({predicate} ({source}) ({target})) ${random})'

        metta+= f" ) {output}))"

        return metta

    def convert_to_dict(self, results, schema=None):
        result = self.prepare_query_input(results, schema)
        (_, node_dict, edge_dict) = self.process_result(result[0], True)
        return (node_dict, edge_dict)

    # Won't work because of we don't try to parse node count and count by labels
    def process_result(self, results, graph_components, result_type):
        node_and_edge_count = {}
        count_by_label = {}
        nodes = []
        edges = []
        node_to_dict = {}
        edge_to_dict = {}
        meta_data = {}

        if result_type == 'graph':
            nodes, edges, node_to_dict, edge_to_dict = self.process_result_graph(
                    results[0], graph_components)

        if result_type == 'count':
            if len(results) > 0:
                node_and_edge_count = results[0]

            if len(results) > 1:
                count_by_label = results[1]

            meta_data = self.process_result_count(
                node_and_edge_count, count_by_label, graph_components)

        return (nodes, edges, node_to_dict, edge_to_dict, meta_data)

    def process_result_graph(self, results, graph_components):
        nodes = {}
        relationships_dict = {}
        node_result = []
        edge_result = []
        node_to_dict = {}
        edge_to_dict = {}
        node_type = set()
        edge_type = set()
        tuples = metta_seralizer(results)

        for match in tuples:
            graph_attribute = match[0]
            match = match[1:]

            if graph_attribute == "node":
                if len(match) > 4:
                    predicate = match[0]
                    src_type = match[1]
                    src_value = match[2]
                    tgt = list(match[3:])
                else:
                    predicate, src_type, src_value, tgt = match
                if (src_type, src_value) not in nodes:
                    nodes[(src_type, src_value)] = {
                        "id": f"{src_type} {src_value}",
                        "type": src_type,
                    }

                if graph_components['properties']:
                     nodes[(src_type, src_value)][predicate] = tgt

                if src_type not in node_type:
                    node_type.add(src_type)
                    node_to_dict[src_type] = []
                node_data = {}
                node_data["data"] = nodes[(src_type, src_value)]
                node_to_dict[src_type].append(node_data)
            elif graph_attribute == "edge":
                property_name, predicate, source, source_id, target, target_id = match[:6]
                value = ' '.join(match[6:])

                key = (predicate, source, source_id, target, target_id)
                if key not in relationships_dict:
                    relationships_dict[key] = {
                        "edge_id": f'{source}_{predicate}_{target}',
                        "label": predicate,
                        "source": f"{source} {source_id}",
                        "target": f"{target} {target_id}",
                    }

                if property_name == "source":
                    relationships_dict[key]["source_data"] = value
                else:
                    relationships_dict[key][property_name] = value

                if predicate not in edge_type:
                    edge_type.add(predicate)
                    edge_to_dict[predicate] = []
                edge_data = {}
                edge_data['data'] = relationships_dict[key]
                edge_to_dict[predicate].append(edge_data)
        node_list = [{"data": node} for node in nodes.values()]
        relationship_list = [{"data": relationship} for relationship in relationships_dict.values()]

        node_result.append(node_list)
        edge_result.append(relationship_list)
        return (node_result, edge_result, node_to_dict, edge_to_dict)

    def process_result_count(self, node_and_edge_count, count_by_label, graph_components):
        if len(node_and_edge_count) != 0:
            node_and_edge_count = node_and_edge_count[0].get_object().value
        node_count_by_label = []
        edge_count_by_label = []

        if len(count_by_label) != 0:
            count_by_label = count_by_label[0].get_object().value
            node_label_count = count_by_label['node_label_count']
            edge_label_count = count_by_label['edge_label_count']

            # update the way node count by label and edge count by label are represented
            for key, value in node_label_count.items():
                node_count_by_label.append(
                    {'label': key, 'count': value['count']})
            for key, value in edge_label_count.items():
                edge_count_by_label.append(
                    {'label': key, 'count': value['count']})

        meta_data = {
            "node_count": node_and_edge_count.get('total_nodes', 0),
            "edge_count": node_and_edge_count.get('total_edges', 0),
            "node_count_by_label": node_count_by_label if node_count_by_label else [],
            "edge_count_by_label": edge_count_by_label if edge_count_by_label else []
        }

        return meta_data

    def prepare_query_input(self, inputs, schema):
        result = []

        for input in inputs:
            if len(input) == 0:
                continue
            tuples = metta_seralizer(input)
            for tuple in tuples:
                if len(tuple) == 2:
                    src_type, src_id = tuple
                    result.append({
                        "source": f"{src_type} {src_id}"
                    })
                else:
                    predicate, src_type, src_id, tgt_type, tgt_id = tuple
                    result.append({
                    "predicate": predicate,
                    "source": f"{src_type} {src_id}",
                    "target": f"{tgt_type} {tgt_id}"
                    })
        query = self.get_node_properties(result, schema)

        result_properties = self.run_query(query)
        return (result, result_properties)
