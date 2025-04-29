from flask import copy_current_request_context, request, jsonify, \
    Response, send_from_directory
import logging
import json
import os
import threading
from app import app, schema_manager, db_instance, socketio, redis_client
from app.lib import validate_request
from flask_cors import CORS
from flask_socketio import disconnect, join_room, send
# from app.lib import limit_graph
from dotenv import load_dotenv
from distutils.util import strtobool
import datetime
from app.lib import Graph, heuristic_sort
from app.annotation_controller import handle_client_request, requery
from app.constants import TaskStatus
from app.workers.task_handler import get_annotation_redis
from app.persistence import AnnotationStorageService, SourceStorageService

# Load environmental variables
load_dotenv()

# set mongo logging
logging.getLogger('pymongo').setLevel(logging.CRITICAL)

# set redis logging
logging.getLogger('flask_redis').setLevel(logging.CRITICAL)

llm = app.config['llm_handler']
EXP = os.getenv('REDIS_EXPIRATION', 3600) # expiration time of redis cache
CORS(app)

@app.route('/schema-list', methods=['GET'])
def get_schema_list():
    schema_list = schema_manager.schema_list
    response = {
        "schemas": schema_list,
    }
    return Response(json.dumps(response, indent=4), mimetype='application/json')

@app.route('/schema', methods=['GET'])
def get_schema_by_source():
    try:
        schema = schema_manager.schema_representation

        response = {'nodes': [], 'edges': []}

        query_string = request.args.getlist("source")

        try:
            source = SourceStorageService.get()
            print(source)
        except Exception as e:
            print(type(e))
        
        if not source:
            query_string = 'all'
        else:
            query_string = source['data_source']

        if query_string == 'all':
            response['nodes'] = schema['nodes']
            response['edges'] = schema['edges']

            return Response(json.dumps(response, indent=4), mimetype='application/json')

        for schema_type in query_string:
            source = schema_type.upper()
            sub_schema = schema.get(source, None)

            if sub_schema is None:
                return jsonify({"error": "Invalid schema source"}), 400

            for key, _ in sub_schema['edges'].items():
                edge = sub_schema['edges'][key]
                edge_data = {
                    "label": schema['edges'][key]['output_label'],
                    **edge
                }
                response['edges'].append(edge_data)
                response['nodes'].append(schema['nodes'][edge['source']])
                response['nodes'].append(schema['nodes'][edge['target']])

            if len(response['edges']) == 0:
                for node in sub_schema['nodes']:
                    response['nodes'].append(schema['nodes'][node])

        return Response(json.dumps(response, indent=4), mimetype='application/json')
    except Exception as e:
        logging.error(f"Error fetching schema: {e}")
        return jsonify({"error": str(e)}), 500

@socketio.on('connect')
def on_connect(current_source_id, args):
    logging.info("source connected")
    send('source is connected')

@socketio.on('disconnect')
def on_disconnect():
    logging.info("source disconnected")
    send("source Disconnected")
    disconnect()

@socketio.on('join')
def on_join(data):
    room = data['room']
    join_room(room)
    logging.info(f"source join a room with {room}")
        # send(f'connected to {room}', to=room)
    cache = get_annotation_redis(room)
    
    if cache != None:
        status = cache['status']
        graph = cache['graph']
        graph_status = True if graph is not None else False
        
        if status == TaskStatus.COMPLETE.value:
            socketio.emit('update', {'status': status, 'update': {'graph': graph_status}},
                  to=str(room))

@app.route('/query', methods=['POST'])  # type: ignore
def process_query():
    data = request.get_json()
    if not data or 'requests' not in data:
        return jsonify({"error": "Missing requests data"}), 400

    limit = request.args.get('limit')
    properties = request.args.get('properties')

    if properties:
        properties = bool(strtobool(properties))
    else:
        properties = True

    if limit:
        try:
            limit = int(limit)
        except ValueError:
            return jsonify(
                {"error": "Invalid limit value. It should be an integer."}
            ), 400
    else:
        limit = None
    try:
        requests = data['requests']

        # Validate the request data before processing
        node_map = validate_request(requests, schema_manager.schema_representation)
        if node_map is None:
            return jsonify(
                {"error": "Invalid node_map returned by validate_request"}
            ), 400

        # sort the predicate based on the the edge count
        if os.getenv('HURISTIC_SORT', 'False').lower() == 'true':
            requests = heuristic_sort(requests, node_map)

        # Generate the query code
        query = db_instance.query_Generator(
            requests, node_map, limit)

        # Extract node types
        nodes = requests['nodes']
        node_types = set()

        for node in nodes:
            node_types.add(node["type"])

        node_types = list(node_types)
        
        return handle_client_request(query, requests, node_types)
    except Exception as e:
        logging.error(f"Error processing query: {e}")
        return jsonify({"error": (e)}), 500

@app.route('/history', methods=['GET'])
def process_source_history():
    page_number = request.args.get('page_number')
    if page_number is not None:
        page_number = int(page_number)
    else:
        page_number = 1
    return_value = []
    cursor = AnnotationStorageService.get(page_number)

    if cursor is None:
        return jsonify('No value Found'), 200

    for document in cursor:
        return_value.append({
            'annotation_id': str(document['_id']),
            "request": document['request'],
            'title': document['title'],
            'node_count': document['node_count'],
            'edge_count': document['edge_count'],
            'node_types': document['node_types'],
            'status': document['status'],
            "created_at": document['created_at'].isoformat(),
            "updated_at": document["updated_at"].isoformat()
        })
    return Response(json.dumps(return_value, indent=4),
                    mimetype='application/json')


@app.route('/annotation/<id>', methods=['GET'])
def get_by_id(id):
    response_data = {}
    cursor = AnnotationStorageService.get_by_id(id)
    
    if cursor is None:
        return jsonify('No value Found'), 404

    limit = request.args.get('limit')
    properties = request.args.get('properties')

    if properties:
        properties = bool(strtobool(properties))
    else:
        properties = False

    if limit:
        try:
            limit = int(limit)
        except ValueError:
            return jsonify(
                {"error": "Invalid limit value. It should be an integer."}
            ), 400

    json_request = cursor.request
    query = cursor.query
    title = cursor.title
    summary = cursor.summary
    annotation_id = cursor.id
    node_count = cursor.node_count
    edge_count = cursor.edge_count
    node_count_by_label = cursor.node_count_by_label
    edge_count_by_label = cursor.edge_count_by_label
    status = cursor.status
    file_path = cursor.path_url

    # Extract node types
    nodes = json_request['nodes']
    node_types = set()
    for node in nodes:
        node_types.add(node["type"])
    node_types = list(node_types)

    try:
        response_data["annotation_id"] = str(annotation_id)
        response_data["request"] = json_request
        response_data["title"] = title
        
        if summary is not None:
            response_data["summary"] = summary
        if node_count is not None:
            response_data["node_count"] = node_count
            response_data["edge_count"] = edge_count
        if node_count_by_label is not None:
            response_data["node_count_by_label"] = node_count_by_label
            response_data["edge_count_by_label"] = edge_count_by_label
        response_data["status"] = status
        
        cache = redis_client.get(str(annotation_id))

        if cache is not None:
            cache = json.loads(cache)
            graph = cache['graph']
            if graph is not None:
                response_data['nodes'] = graph['nodes']
                response_data['edges'] = graph['edges']

            return Response(json.dumps(response_data, indent=4), mimetype='application/json')

        if status in [TaskStatus.PENDING.value, TaskStatus.COMPLETE.value]:
            if status == TaskStatus.COMPLETE.value:
                if os.path.exists(file_path):
                    # open the file and read the graph
                    with open(file_path, 'r') as file:
                        graph = json.load(file)

                    response_data['nodes'] = graph['nodes']
                    response_data['edges'] = graph['edges']
                else:
                    response_data['status'] = TaskStatus.PENDING.value
                    requery(annotation_id, query, json_request)
            formatted_response = json.dumps(response_data, indent=4)
            return Response(formatted_response, mimetype='application/json')
        
        # Run the query and parse the results
        result = db_instance.run_query(query)
        graph_components = {"properties": properties}
        response_data = db_instance.parse_and_serialize(
            result, schema_manager.schema_representation,
            graph_components, result_type='graph')
        graph = Graph()
        if (len(response_data['edges']) == 0):
            response_data = graph.group_node_only(response_data)
        else:
            grouped_graph = graph.group_graph(response_data)
        response_data['nodes'] = grouped_graph['nodes']
        response_data['edges'] = grouped_graph['edges']

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype='application/json')
    except Exception as e:
        logging.error(f"Error processing query: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/annotation/<id>', methods=['DELETE'])
def delete_by_id(id):
    try:
        # check if the source have access to delete the resource
        annotation = AnnotationStorageService.get_by_id(id)
        
        if annotation is None:
            return jsonify('No value Found'), 404

        # first check if there is any running running annoation
        with app.config['annotation_lock']:
            thread_event = app.config['annotation_threads']
            stop_event = thread_event.get(id, None)
        
            # if there is stop the running annoation
            if stop_event is not None:
                stop_event.set()

                response_data = {
                    'message': f'Annotation {id} has been cancelled.'
                }

                formatted_response = json.dumps(response_data, indent=4)
                return Response(formatted_response, mimetype='application/json')
        
        # else delete the annotation from the db
        existing_record = AnnotationStorageService.get_by_id(id)

        if existing_record is None:
            return jsonify('No value Found'), 404

        deleted_record = AnnotationStorageService.delete(id)

        if deleted_record is None:
            return jsonify('Failed to delete the annotation'), 500


        response_data = {
            'message': 'Annotation deleted successfully'
        }

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype='application/json')
    except Exception as e:
        logging.error(f"Error deleting annotation: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/annotation/<id>/title', methods=['PUT'])
def update_title(id):
    data = request.get_json()

    if 'title' not in data:
        return jsonify({"error": "Title is required"}), 400

    title = data['title']

    try:
        existing_record = AnnotationStorageService.get_by_id(id)

        if existing_record is None:
            return jsonify('No value Found'), 404

        AnnotationStorageService.update(id, {'title': title})

        response_data = {
            'message': 'title updated successfully',
            'title': title,
        }

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype='application/json')
    except Exception as e:
        logging.error(f"Error updating title: {e}")
        return jsonify({"error": str(e)}), 500
    
@app.route('/annotation/delete', methods=['POST'])
def delete_many():
    data = request.data.decode('utf-8').strip()  # Decode and strip the string of any extra spaces or quotes

    # Ensure that data is not empty or just quotes
    if not data or data.startswith("'") and data.endswith("'"):
        data = data[1:-1]  # Remove surrounding quotes

    try:
        data = json.loads(data)  # Now parse the cleaned string
    except json.JSONDecodeError:
        return {"error": "Invalid JSON"}, 400  # Return 400 if the JSON is invalid
    
    if 'annotation_ids' not in data:
        return jsonify({"error": "Missing annotation ids"}), 400
        
    annotation_ids = data['annotation_ids']
    
    #check if source have access to delete the resource
    for annotation_id in annotation_ids:
        annotation = AnnotationStorageService.get_by_id(annotation_id)
        if annotation is None:
            return jsonify('No value Found'), 404
    
    if not isinstance(annotation_ids, list):
        return jsonify({"error": "Annotation ids must be a list"}), 400
    
    if len(annotation_ids) == 0:
        return jsonify({"error": "Annotation ids must not be empty"}), 400
    
    try:
        delete_count = AnnotationStorageService.delete_many_by_id(annotation_ids)
        
        response_data = {
            'message': f'Out of {len(annotation_ids)}, {delete_count} were successfully deleted.'
        }
        
        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype='application/json')
    except Exception as e:
        logging.error(f"Error deleting annotations: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/settings/data-source', methods=['POST'])
def update_settings():
    data = request.get_json()
    
    data_source = data.get('data_source', None)
    
    if data_source is None:
        return jsonify({"error": "Missing data source"}), 400
    
    if isinstance(data_source, str):
        if data_source.lower() == 'all':
            SourceStorageService.upsert_by_id({'data_source': 'all'})

            response_data = {
                'message': 'Data source updated successfully',
                'data_source': 'all'
            }
            formatted_response = json.dumps(response_data, indent=4)
            return Response(formatted_response, mimetype='application/json')
        else:
            return jsonify({"error": "Invalid data source format"}), 400
    
    # check if the data source is valid
    for ds in data_source:
        if ds.upper() not in schema_manager.schema_representation:
            return jsonify({"error": f"Invalid data source: {ds}"}), 400
    
    try:
        SourceStorageService.upsert_by_id({'data_source': data_source})
        
        response_data = {
            'message': 'Data source updated successfully',
            'data_source': data_source
        }
        
        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype='application/json')
    except Exception as e:
        logging.error(f"Error updating data source: {e}")
        return jsonify({"error": str(e)}), 500
