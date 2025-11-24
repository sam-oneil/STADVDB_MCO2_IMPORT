import mysql.connector

nodes = {
    "Node0": {"host": "10.2.14.93", "user": "root", "password": "nGERH3tcswdCpXr7vTYFDB4M", "database": "main_db"},
    "Node1": {"host": "10.2.14.94", "user": "root", "password": "nGERH3tcswdCpXr7vTYFDB4M", "database": "main_db"},
    "Node2": {"host": "10.2.14.95", "user": "root", "password": "nGERH3tcswdCpXr7vTYFDB4M", "database": "main_db"}
}

def connect_node(node_name):
    try:
        config = nodes[node_name]
        conn = mysql.connector.connect(**config)
        return conn
    except Exception as e:
        print(f"Connection Error to {node_name}: {e}")
        return None    

def replicate_update(source_node, target_nodes, sql_update_query):
    successful_replications = []
    failed_replications = []

    for target_node in target_nodes:
        if target_node == source_node:
            continue
            
        conn = connect_node(target_node)
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute(sql_update_query)
                conn.commit()
                cursor.close()
                successful_replications.append(target_node)
            except Exception as e:
                conn.rollback()
                failed_replications.append(f"{target_node} ({e})")
            finally:
                conn.close()
        else:
            failed_replications.append(f"{target_node} (No connection)")
            
    return successful_replications, failed_replications