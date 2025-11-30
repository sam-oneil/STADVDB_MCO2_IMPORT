import mysql.connector
import time

# --- DB Credentials ---
USER = "dbuser"
PASSWORD = "nGERH3tcswdCpXr7vTYFDB4M"

# --- Node Configurations ---
nodes = {
    "Node 1": {"host": "10.2.14.93", "port": 3306, "user": "dbuser", "password": "nGERH3tcswdCpXr7vTYFDB4M", "database": "titles_db"},
    "Node 2": {"host": "10.2.14.94", "port": 3306, "user": "dbuser", "password": "nGERH3tcswdCpXr7vTYFDB4M", "database": "titles_db2"},
    "Node 3": {"host": "10.2.14.95", "port": 3306, "user": "dbuser", "password": "nGERH3tcswdCpXr7vTYFDB4M", "database": "titles_db3"},
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

# --- Replication Helpers ---

def replicate_update(source_node, target_nodes, sql_text):
    """
    Execute a raw SQL string on each target node (skips source_node).
    Returns (success_nodes, failed_nodes, errors_dict)
    """
    success = []
    failed = []
    errors = {}
    for node in target_nodes:
        if node == source_node:
            continue
        try:
            cfg = nodes[node]
            conn_target = mysql.connector.connect(
                host=cfg["host"],
                port=cfg["port"],
                user=cfg["user"],
                password=cfg["password"],
                database=cfg["database"],
                connection_timeout=5
            )
            cursor_target = conn_target.cursor()
            cursor_target.execute("SET AUTOCOMMIT = 1")
            cursor_target.execute(sql_text)
            conn_target.commit()
            cursor_target.close()
            conn_target.close()
            success.append(node)
        except Exception as e:
            failed.append(node)
            errors[node] = str(e)
    return success, failed, errors

def insert_replication_log(local_cfg, tconst, sql_text, op_type, target_nodes, last_error=None):
    """
    Insert a pending replication task into the local replication_log table.
    local_cfg should be nodes[source_node].
    target_nodes should be a list (will be stored as comma-separated).
    """
    try:
        conn = mysql.connector.connect(
            host=local_cfg["host"],
            port=local_cfg["port"],
            user=local_cfg["user"],
            password=local_cfg["password"],
            database=local_cfg["database"]
        )
        cursor = conn.cursor()
        sql = """
        INSERT INTO replication_log (tconst, sql_text, op_type, target_nodes, last_error)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (tconst, sql_text, op_type, ",".join(target_nodes), last_error))
        conn.commit()
        cursor.close()
        conn.close()
        return True, None
    except Exception as e:
        return False, str(e)

def fetch_pending_logs(local_cfg, limit=100):
    try:
        conn = mysql.connector.connect(
            host=local_cfg["host"],
            port=local_cfg["port"],
            user=local_cfg["user"],
            password=local_cfg["password"],
            database=local_cfg["database"]
        )
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM replication_log WHERE status = 'PENDING' LIMIT %s", (limit,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception as e:
        return []