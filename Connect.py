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
            print(f"Replication to {node} failed: {e}")
            failed.append(node)
            errors[node] = str(e)

    return success, failed, errors

def insert_replication_log(node_cfg, tconst, sql_text, op_type, target_nodes, last_error=None, txn_stage="PRE_COMMIT"):
    try:
        conn = mysql.connector.connect(
            host=node_cfg["host"],
            port=node_cfg["port"],
            user=node_cfg["user"],
            password=node_cfg["password"],
            database=node_cfg["database"]
        )
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO replication_log
            (tconst, sql_text, op_type, target_nodes, status, last_error, txn_stage)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            tconst,
            sql_text,
            op_type,
            ",".join(target_nodes),
            "PENDING" if last_error else "REPLICATED",
            last_error,
            txn_stage
        ))
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

def update_replication_log(node_cfg, log_id, status, last_error=None):
    """
    Update existing replication log entry status, retry count, and last attempt timestamp
    """
    try:
        conn = mysql.connector.connect(
            host=node_cfg["host"],
            port=node_cfg["port"],
            user=node_cfg["user"],
            password=node_cfg["password"],
            database=node_cfg["database"]
        )
        cursor = conn.cursor()
        
        # Update status, increment retry_count, and set last_attempt to current timestamp
        if last_error is not None:
            cursor.execute("""
                UPDATE replication_log 
                SET status = %s, last_error = %s, retry_count = retry_count + 1, last_attempt = NOW()
                WHERE id = %s
            """, (status, last_error, log_id))
        else:
            cursor.execute("""
                UPDATE replication_log 
                SET status = %s, last_error = NULL, retry_count = retry_count + 1, last_attempt = NOW()
                WHERE id = %s
            """, (status, log_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True, None
    except Exception as e:
        return False, str(e)
    
def update_replication_log(node_cfg, log_id, status, last_error=None):
    """
    Update an existing replication log row with new status, error, last_attempt, and increment retry_count.
    """
    try:
        conn = mysql.connector.connect(
            host=node_cfg["host"],
            port=node_cfg["port"],
            user=node_cfg["user"],
            password=node_cfg["password"],
            database=node_cfg["database"]
        )
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE replication_log
            SET status=%s,
                last_error=%s,
                last_attempt=NOW(),
                retry_count=IFNULL(retry_count,0)+1
            WHERE id=%s
        """, (status, last_error, log_id))
        conn.commit()
        cursor.close()
        conn.close()
        return True, None
    except Exception as e:
        return False, str(e)

def recover_pending_transactions(curr_node):
    """
    Automatically retry all PENDING replication logs for this node.
    Returns a summary dict.
    """
    pending_logs = fetch_pending_logs(nodes[curr_node], limit=1000)  # fetch all
    recovery_summary = {"recovered": [], "still_pending": [], "failed": []}

    for log in pending_logs:
        tconst = log["tconst"]
        sql_text = log["sql_text"]
        target_nodes = log["target_nodes"].split(",")

        succ, fail, errs = replicate_update(curr_node, target_nodes, sql_text)

        # Update the existing log entry based on overall result
        if fail:
            # Still has failures, keep as PENDING with error details
            error_msg = "; ".join([f"{node}: {errs[node]}" for node in fail])
            ok, ierr = update_replication_log(
                nodes[curr_node],
                log["id"],
                "PENDING",
                error_msg
            )
            if not ok:
                recovery_summary["failed"].append(log)
            else:
                recovery_summary["still_pending"].append(log)
        else:
            # All replications succeeded
            ok, ierr = update_replication_log(
                nodes[curr_node],
                log["id"],
                "REPLICATED",
                None
            )
            if not ok:
                recovery_summary["failed"].append(log)
            else:
                recovery_summary["recovered"].append(log)

    return recovery_summary

def auto_recovery_on_startup(curr_node):
    """
    Run automatic recovery when node starts up.
    Simply retry all pending replications - let target nodes handle acceptance.
    """
    print(f"Starting auto-recovery for {curr_node}...")
    
    # Get all pending logs from this node
    pending_logs = fetch_pending_logs(nodes[curr_node], limit=1000)
    processed = 0
    recovered = 0
    
    for log in pending_logs:
        target_nodes = log["target_nodes"].split(",")
        succ, fail, errs = replicate_update(curr_node, target_nodes, log["sql_text"])
        
        # Update log status
        if fail:
            error_msg = "; ".join([f"{node}: {errs[node]}" for node in fail])
            update_replication_log(nodes[curr_node], log["id"], "PENDING", error_msg)
        else:
            update_replication_log(nodes[curr_node], log["id"], "REPLICATED", None)
            recovered += 1
        
        processed += 1
    
    print(f"Auto-recovery completed: {processed} logs processed, {recovered} recovered")
    return {"processed": processed, "recovered": recovered}