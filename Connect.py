import mysql.connector

# --- DB Credentials ---
USER = "dbuser"
PASSWORD = "nGERH3tcswdCpXr7vTYFDB4M"

# --- Node Configurations ---
nodes = {
    "Node 1": {"host": "ccscloud.dlsu.edu.ph", "port": 60793, "user": "dbuser", "password": "nGERH3tcswdCpXr7vTYFDB4M", "database": "titles_db"},
    "Node 2": {"host": "ccscloud.dlsu.edu.ph", "port": 60794, "user": "dbuser", "password": "nGERH3tcswdCpXr7vTYFDB4M", "database": "titles_db2"},
    "Node 3": {"host": "ccscloud.dlsu.edu.ph", "port": 60795, "user": "dbuser", "password": "nGERH3tcswdCpXr7vTYFDB4M", "database": "titles_db3"},
}

# --- Node Connector ---
def connect_node(nodes):
    connections = {}
    ping_results = {}

    for node_name, node_info in nodes.items():
        try:
            conn = mysql.connector.connect(
                host=node_info["host"],
                port=node_info["port"],
                user=node_info["user"],      # use user from nodes dict
                password=node_info["password"],
                database=node_info["database"],
                connection_timeout=3
            )
            connections[node_name] = conn
            ping_results[node_name] = "Reachable"
        except mysql.connector.Error:
            connections[node_name] = None
            ping_results[node_name] = "Unreachable"

    return connections, ping_results

# --- Replication Function ---
def replicate_update(source_node, target_nodes, sql):
    success, failure = [], []
    for node in target_nodes:
        try:
            cfg = nodes[node]
            conn_target = mysql.connector.connect(
                host=cfg["host"],
                port=cfg["port"],
                user=cfg["user"],
                password=cfg["password"],
                database=cfg["database"]
            )
            cursor_target = conn_target.cursor()
            cursor_target.execute(sql)
            conn_target.commit()
            cursor_target.close()
            conn_target.close()
            success.append(node)
        except Exception as e:
            failure.append(node)
    return success, failure
