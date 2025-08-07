def get_total_counts(graph_data):
    """
    Calculate the total number of nodes and edges.

    Args:
        graph_data (dict): The input graph data containing nodes and edges.

    Returns:
        dict: A dictionary with total node count and edge count.
    """
    node_count = len(graph_data.get('nodes', []))
    edge_count = len(graph_data.get('edges', []))
    return {"node_count": node_count, "edge_count": edge_count}


def get_count_by_label(graph_data):
    """
    Calculate the count of nodes and edges grouped by their labels.

    Args:
        graph_data (dict): The input graph data containing nodes and edges.

    Returns:
        dict: A dictionary with node counts and edge counts grouped by labels.
    """
    # Count nodes by label
    node_count_by_label = {}
    for node in graph_data.get('nodes', []):
        label = node['data'].get('type', 'unknown')
        node_count_by_label[label] = node_count_by_label.get(label, 0) + 1

    # Convert node counts to the desired format
    node_count_by_label_list = [
        {"label": label, "count": count} for label, count in node_count_by_label.items()
    ]

    # Count edges by label
    edge_count_by_label = {}
    for edge in graph_data.get('edges', []):
        label = edge['data'].get('label', 'unknown')
        edge_count_by_label[label] = edge_count_by_label.get(label, 0) + 1

    # Convert edge counts to the desired format
    edge_count_by_label_list = [
        {"label": label, "count": count} for label, count in edge_count_by_label.items()
    ]

    return {
        "node_count_by_label": node_count_by_label_list,
        "edge_count_by_label": edge_count_by_label_list,
    }
