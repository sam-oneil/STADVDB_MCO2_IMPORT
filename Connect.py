import mysql.connector

nodes = {
    "Node0": {"host": "ccscloud.dlsu.edu.ph", "port": 60793, "user": "dbuser", "password": "nGERH3tcswdCpXr7vTYFDB4M", "database": "titles_db"},
    "Node1": {"host": "ccscloud.dlsu.edu.ph", "port": 60794, "user": "dbuser", "password": "nGERH3tcswdCpXr7vTYFDB4M", "database": "titles_db2"},
    "Node2": {"host": "ccscloud.dlsu.edu.ph", "port": 60795, "user": "dbuser", "password": "nGERH3tcswdCpXr7vTYFDB4M", "database": "titles_db3"},
}

def connect_node(node_name):
    try:
        config = nodes[node_name]
        conn = mysql.connector.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )
        return conn
    except Exception as e:
        print(f"Connection Error to {node_name}: {e}")
        return None

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
