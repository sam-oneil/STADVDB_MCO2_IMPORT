import streamlit as st
import socket
from Connect import nodes, connect_node, replicate_update, insert_replication_log, fetch_pending_logs, update_replication_log, recover_pending_transactions, auto_recovery_on_startup
import mysql.connector

# --- Node Definitions ---
curr_host = socket.gethostname()
host_node = {
    "STADVDB31-Server0": "Node 1",
    "STADVDB31-Server1": "Node 2",
    "STADVDB31-Server2": "Node 3"
}

curr_node = host_node.get(curr_host, "Unknown Node")

st.set_page_config(layout="wide")
st.markdown("<h1 style='text-align: center;'>Distributed Database Management System</h1>", unsafe_allow_html=True)

if "refresh" not in st.session_state:
    st.session_state["refresh"] = False

if "auto_recovery_done" not in st.session_state:
    st.session_state["auto_recovery_done"] = False

if curr_node == "Unknown Node":
    st.error("This application must be run on one of the designated nodes.")
    st.stop()
else:
    st.success(f"Running on {curr_node}")

# --- Auto Recovery on Startup ---
if not st.session_state["auto_recovery_done"]:
    try:
        recovery_result = auto_recovery_on_startup(curr_node)
        if recovery_result["processed"] > 0:
            st.info(f"Auto-recovery completed: {recovery_result['processed']} logs processed, {recovery_result['recovered']} recovered")
        st.session_state["auto_recovery_done"] = True
    except Exception as e:
        st.warning(f"Auto-recovery failed: {e}")
        st.session_state["auto_recovery_done"] = True

# --- Session State Initialization ---
if "session_id" not in st.session_state:
    import uuid
    # Generate a truly unique session ID for each browser tab
    st.session_state["session_id"] = str(uuid.uuid4())
    # Force complete session isolation by clearing everything
    st.session_state["in_transaction"] = False
    st.session_state["txn_conn"] = None
    st.session_state["pending_replications"] = []
    st.session_state["id"] = None
    st.session_state["read_conn_cache"] = {}  # Cache read connections per session

if "in_transaction" not in st.session_state:
    st.session_state["in_transaction"] = False

if "id" not in st.session_state:
    st.session_state["id"] = None

if "txn_conn" not in st.session_state:
    st.session_state["txn_conn"] = None

if "read_conn_cache" not in st.session_state:
    st.session_state["read_conn_cache"] = {}

if "pending_replications" not in st.session_state:
    st.session_state["pending_replications"] = []

# Display session info for debugging after all session state is initialized
st.info(f"Session ID: {st.session_state['session_id'][:8]}...")  # Show first 8 chars for debugging

# --- Helper Functions ---
def new_conn(curr_node):
    cfg = nodes[curr_node]
    return mysql.connector.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"]
    )

def get_conn(curr_node):
    if st.session_state["in_transaction"]:
        return st.session_state["txn_conn"]
    else:
        conn = new_conn(curr_node)
        cursor = conn.cursor()

        cursor.execute("SET AUTOCOMMIT = 0")
        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        cursor.execute("START TRANSACTION")
        
        # Set session-specific connection name for isolation
        cursor.execute(f"SET @session_id = '{st.session_state['session_id']}'")
        cursor.close()

        st.session_state["in_transaction"] = True
        st.session_state["txn_conn"] = conn
        return conn

def get_read_conn(curr_node):
    """Get a separate read-only connection for READ COMMITTED isolation"""
    conn = new_conn(curr_node)
    cursor = conn.cursor()
    
    # Set session-specific connection identifier
    cursor.execute(f"SET @session_id = '{st.session_state['session_id']}'")
    
    # Always use READ COMMITTED - can only see committed changes from other sessions
    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    cursor.execute("SET AUTOCOMMIT = 1")  # Each read sees latest committed state
    
    cursor.close()
    return conn

def get_session_read_conn(curr_node):
    """Get read connection that sees own uncommitted changes if in transaction"""
    # If we're in a transaction, use the transaction connection to see our own changes
    if st.session_state["in_transaction"] and st.session_state["txn_conn"]:
        return st.session_state["txn_conn"]
    
    # Otherwise, use a separate read connection that only sees committed data
    session_key = f"{curr_node}_read_committed"
    
    if session_key not in st.session_state["read_conn_cache"]:
        conn = get_read_conn(curr_node)
        st.session_state["read_conn_cache"][session_key] = conn
    
    return st.session_state["read_conn_cache"][session_key]

def cleanup_session_connections():
    """Clean up session read connections"""
    for conn in st.session_state["read_conn_cache"].values():
        try:
            if conn.is_connected():
                conn.close()
        except:
            pass
    st.session_state["read_conn_cache"] = {}
    
left_col, right_col = st.columns([1, 2], gap="large")

with left_col:
    # --- Node Status ---
    connections, ping_results = connect_node(nodes)
    conn = connections[curr_node] 

    st.header("NODE STATUS")
    cols = st.columns(len(nodes), gap="large")
    for i, (node_name, status) in enumerate(ping_results.items()):
        with cols[i]:
            st.subheader(node_name)
            if status == "Reachable":
                st.success("● Reachable")
            else:
                st.error("● Unreachable")

    # --- Isolation Level ---
    st.header("ISOLATION LEVEL")
    st.info("Fixed to READ COMMITTED: See your own uncommitted changes, but not others'")
    st.text("Current Level: READ COMMITTED")

    # --- REPLICATION LOG ---
    st.header("REPLICATION LOGS")

    # Filter dropdown
    log_stage = st.selectbox(
        "Show logs for:",
        ["BOTH", "PRE_COMMIT", "POST_COMMIT"],
        index=0
    )

    try:
        conn_debug = get_session_read_conn(curr_node)  # Use session-specific read connection
        cursor = conn_debug.cursor(dictionary=True)

        LIMIT_NUM = 5

        # Build query based on filter
        if log_stage == "BOTH":
            cursor.execute(f"SELECT * FROM replication_log ORDER BY id DESC LIMIT {LIMIT_NUM}")
        else:
            cursor.execute(
                f"SELECT * FROM replication_log WHERE txn_stage = %s ORDER BY id DESC LIMIT {LIMIT_NUM}",
                (log_stage,)
            )

        rows = cursor.fetchall()
        st.dataframe(rows)

        cursor.close()
        # Don't close the connection - it's cached for the session
    except Exception as e:
        st.error(f"Failed to load replication log: {e}")

    # --- Retry Pending Replications ---
    st.header("PENDING REPLICATIONS")

    # Fetch pending logs from local node
    pending_logs = fetch_pending_logs(nodes[curr_node], limit=50)

    if pending_logs:
        st.warning(f"{len(pending_logs)} pending replication(s).")
        
        # Show pending logs in table
        display_logs = [
            {
                "ID": log["id"],
                "tconst": log["tconst"],
                "Operation": log["op_type"],
                "Targets": log["target_nodes"],
                "Status": log["status"],
                "Last Error": log["last_error"],
                "Txn Stage": log["txn_stage"],
                "Created At": log["created_at"],
                "Last Attempt": log["last_attempt"]
            }
            for log in pending_logs
        ]
        st.dataframe(display_logs)

        if st.button("Retry Pending Replications"):
            for log in pending_logs:
                tconst = log["tconst"]
                sql_text = log["sql_text"]
                target_nodes = log["target_nodes"].split(",")
                
                succ, fail, errs = replicate_update(curr_node, target_nodes, sql_text)
                
                # Update the existing log entry based on overall result
                if fail:
                    # Still has failures, keep as PENDING with error details
                    error_msg = "; ".join([f"{node}: {errs[node]}" for node in fail])
                    ok, ierr = update_replication_log(nodes[curr_node], log["id"], "PENDING", error_msg)
                else:
                    # All replications succeeded
                    ok, ierr = update_replication_log(nodes[curr_node], log["id"], "REPLICATED", None)
                
                if not ok:
                    st.error(f"Failed to update replication log: {ierr}")
            
            st.success("Pending replications retried!")
            st.session_state["refresh"] = not st.session_state.get("refresh", False)

    else:
        st.info("No pending replications.")

with right_col:
    # --- CRUD Operations ---
    st.markdown("<h2 style='text-align: center;'>CRUD OPERATIONS</h2>", unsafe_allow_html=True) 

    # --- Helper Functions ---

    def get_row_by_tconst(tconst):
        try:
            conn = get_session_read_conn(curr_node)  # Use session-specific read connection
            cursor = conn.cursor(dictionary=True)

            query = "SELECT * FROM titles WHERE tconst = %s"
            cursor.execute(query, (tconst,))
            row = cursor.fetchone()
            cursor.close()
            # Don't close the connection - it's cached for the session

            return row
        except Exception as e:
            st.error(f"Failed to fetch row: {e}")
            return None
        
    def get_nodes_from_title(title: str) -> list:
        if not title or not title.strip():
            return ["Node 1"]
        
        first_char = title.strip()[0].upper()
        if 'A' <= first_char <= 'M':
            return ['Node 1', 'Node 2']
        else:
            return ['Node 1', 'Node 3']
        
    def is_title_in_node(title: str, curr_node: str) -> bool:
        return curr_node in get_nodes_from_title(title)
    
    def build_insert_sql(tconst, title, year, genre):
        escaped_title = title.replace('"', '\"')
        escaped_genre = genre.replace('"', '\"')
        return (
            "INSERT INTO titles (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, runtimeMinutes, genres) "
            f"VALUES ('{tconst}','movie',\"{escaped_title}\",\"{escaped_title}\",0,{int(year)},'\\\\N',\"{escaped_genre}\") "
            "ON DUPLICATE KEY UPDATE primaryTitle=VALUES(primaryTitle), originalTitle=VALUES(originalTitle), startYear=VALUES(startYear), genres=VALUES(genres)"
        )

    def build_update_sql(tconst, updates: dict):
        parts = []
        for k,v in updates.items():
            if v is None:
                continue
            if isinstance(v, int):
                parts.append(f"{k} = {v}")
            else:
                s = str(v).replace('"', '\"')
                parts.append(f'{k} = "{s}"')
        if not parts:
            return None
        set_clause = ", ".join(parts)
        return f'UPDATE titles SET {set_clause} WHERE tconst = \'{tconst}\''

    def build_delete_sql(tconst):
        return f"DELETE FROM titles WHERE tconst = '{tconst}'"

    def show_surrounding_rows(conn, tconst):
        try:
            # Use session-specific read connection for proper isolation
            read_conn = get_session_read_conn(curr_node)
            cursor = read_conn.cursor(dictionary=True)

            num = int(tconst[2:])
            lower = "tt" + str(max(num - 5, 1)).zfill(7)
            upper = "tt" + str(num + 5).zfill(7)

            query = """
                SELECT *
                FROM titles
                WHERE tconst BETWEEN %s AND %s
                ORDER BY tconst
            """
            cursor.execute(query, (lower, upper))
            rows = cursor.fetchall()
            cursor.close()
            # Don't close the connection - it's cached for the session

            st.subheader("UPDATED DATABASE")
            st.dataframe(rows)

        except Exception as e:
            st.error(f"Failed loading surrounding rows: {e}")
    
    st.markdown("<h3 style='text-align: center;'> Search Title </h2>", unsafe_allow_html=True)

    search_term = st.text_input("Enter Title ID (tconst):", key="search_term")

    if st.button("Search", type = "primary"):
        if search_term.strip() != "":
            try:
                row = get_row_by_tconst(search_term.strip())
                if row:
                    st.dataframe([row])
                else:
                    st.warning(f"No record found with ID {search_term.strip()}")
            except Exception as e:
                st.error(f"Search failed: {e}")

    col1, col2, col3 = st.columns(3, gap="large")
    
    with col1:
        st.markdown("<h3 style='text-align: center;'>Add Title</h3>", unsafe_allow_html=True) 
        add_title = st.text_input("Title", key="add_title")
        add_year = st.number_input("Year", min_value=1900, max_value=2100, step=1, key="add_year")
        add_genre = st.text_input("Genre", key="add_genre")
             
    with col2:
        st.markdown("<h3 style='text-align: center;'>Update Title</h3>", unsafe_allow_html=True) 
        upd_id = st.text_input("ID", key="upd_id")
        upd_title = st.text_input("Title", key="upd_title")
        upd_year = st.number_input("Year", min_value=1900, max_value=2100, step=1, key="upd_year")
        upd_genre = st.text_input("Genre", key="upd_genre")
        
    with col3:
        st.markdown("<h3 style='text-align: center;'>Delete Title</h3>", unsafe_allow_html=True) 
        del_id = st.text_input("ID", key="del_id")
        
    col1, col2, col3 = st.columns(3, gap="large")

    # Add
    with col1:
        if st.button("Add", type="primary", use_container_width=True):
            if add_title == "":
                st.error("Title cannot be empty.")
            elif not is_title_in_node(add_title, curr_node):
                st.error(f"Title '{add_title}' does not belong to {curr_node}.")
            else:
                try:
                    conn = get_conn(curr_node)
                    cursor = conn.cursor()

                    RANGES = {
                        "Node 1": (1, 999_999),
                        "Node 2": (1_000_000, 1_999_999),
                        "Node 3": (2_000_000, 2_999_999)
                    }
                    min_id, max_id = RANGES[curr_node]

                    cursor.execute("""
                        SELECT MAX(CAST(SUBSTRING(tconst, 3) AS UNSIGNED)) 
                        FROM titles 
                        WHERE CAST(SUBSTRING(tconst, 3) AS UNSIGNED) BETWEEN %s AND %s
                    """, (min_id, max_id))

                    result = cursor.fetchone()
                    last_num = result[0] if result[0] else (min_id - 1)
                    new_num = last_num + 1

                    if new_num > max_id:
                        st.error("ID range exhausted for this node!")
                        conn.close()
                    else:
                        new_tconst = "tt" + str(new_num).zfill(7)

                        sql = """
                            INSERT INTO titles 
                            (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, runtimeMinutes, genres)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        val = (new_tconst, "movie", add_title, add_title, 0, add_year, "\\N", add_genre)

                        cursor.execute(sql, val)

                        # Store replication info for after commit
                        sql_text = build_insert_sql(new_tconst, add_title, add_year, add_genre)
                        targets = get_nodes_from_title(add_title)  # returns ['Node 1', 'Node 2'] or ['Node 1','Node 3']
                        # Always ensure central Node 1 is included
                        if 'Node 1' not in targets:
                            targets.insert(0, 'Node 1')

                        st.session_state["pending_replications"].append({
                            "tconst": new_tconst,
                            "sql_text": sql_text,
                            "targets": targets,
                            "op_type": "INSERT"
                        })

                        st.session_state["txn_conn"] = conn
                        st.session_state["in_transaction"] = True
                        st.session_state["id"] = new_tconst

                        st.success(f"'{add_title}' added successfully with ID {new_tconst}")

                except Exception as e:
                    st.error(f"Add failed: {e}")
    
    # Update
    with col2:
          if st.button("Update", type = "primary", width = "stretch"):
            try:
                conn = get_conn(curr_node)
                if conn:
                    if upd_id.strip() == "":
                        st.error("tconst cannot be empty")
                    else: 
                        row = get_row_by_tconst(upd_id.strip()) # Check if record exists in the node
                        if not row:
                            st.error(f"No record found with ID {upd_id.strip()}")
                        
                        elif upd_title != "" or upd_year != 0 or upd_genre != "":
                            effective_title = upd_title if upd_title != "" else row["primaryTitle"]

                            if not is_title_in_node(effective_title, curr_node):
                                st.error(f"Title '{effective_title}' does not belong to {curr_node}.")
                            else:
                                cursor = conn.cursor()

                                if upd_title != "":
                                    sql = "UPDATE titles SET primaryTitle = %s WHERE tconst = %s"
                                    val = (upd_title, upd_id)
                                    cursor.execute(sql, val)
                                    
                                    # Store replication info for after commit
                                    update_sql = build_update_sql(upd_id, {"primaryTitle": upd_title})
                                    if update_sql:
                                        targets = get_nodes_from_title(upd_title if upd_title != "" else row["primaryTitle"])
                                        if 'Node 1' not in targets:
                                            targets.insert(0, 'Node 1')
                                        
                                        st.session_state["pending_replications"].append({
                                            "tconst": upd_id,
                                            "sql_text": update_sql,
                                            "targets": targets,
                                            "op_type": "UPDATE"
                                        })

                                if upd_year != 0:
                                    sql = "UPDATE titles SET startYear = %s WHERE tconst = %s"
                                    val = (upd_year, upd_id)
                                    cursor.execute(sql, val)

                                    # Store replication info for after commit
                                    update_sql = build_update_sql(upd_id, {"startYear": upd_year})
                                    if update_sql:
                                        targets = get_nodes_from_title(upd_title if upd_title != "" else row["primaryTitle"])
                                        if 'Node 1' not in targets:
                                            targets.insert(0, 'Node 1')
                                        
                                        st.session_state["pending_replications"].append({
                                            "tconst": upd_id,
                                            "sql_text": update_sql,
                                            "targets": targets,
                                            "op_type": "UPDATE"
                                        })

                                if upd_genre != "":
                                    sql = "UPDATE titles SET genres = %s WHERE tconst = %s"
                                    val = (upd_genre, upd_id)
                                    cursor.execute(sql, val)

                                    # Store replication info for after commit
                                    update_sql = build_update_sql(upd_id, {"genres": upd_genre})
                                    if update_sql:
                                        targets = get_nodes_from_title(upd_title if upd_title != "" else row["primaryTitle"])
                                        if 'Node 1' not in targets:
                                            targets.insert(0, 'Node 1')
                                        
                                        st.session_state["pending_replications"].append({
                                            "tconst": upd_id,
                                            "sql_text": update_sql,
                                            "targets": targets,
                                            "op_type": "UPDATE"
                                        })

                                st.session_state["txn_conn"] = conn
                                st.session_state["in_transaction"] = True
                                st.session_state["id"] = upd_id

                                st.success(f"'{upd_id}' updated successfully")
                        else:
                            st.error("At least one must be filled")
                else:
                    st.error(f"No connection to {curr_node}")
            except Exception as e:
                st.error(f"Update failed: {e}")

    # Delete
    with col3:
        if st.button("Delete", type = "primary", width = "stretch"):
            try:
                conn = get_conn(curr_node)
                if conn:
                    if del_id.strip() == "":
                        st.error("tconst cannot be empty")
                    else:
                        row = get_row_by_tconst(del_id.strip())
                        if not row:
                            st.error(f"No record found with ID {del_id.strip()}")
                        else:
                            cursor = conn.cursor()
                            
                            sql = "DELETE FROM titles WHERE tconst = %s"
                            cursor.execute(sql, (del_id,))

                            # Store replication info for after commit
                            del_sql = build_delete_sql(del_id)
                            targets = get_nodes_from_title(row["primaryTitle"])
                            if 'Node 1' not in targets:
                                targets.insert(0, 'Node 1')
                    
                            st.session_state["pending_replications"].append({
                                "tconst": del_id,
                                "sql_text": del_sql,
                                "targets": targets,
                                "op_type": "DELETE"
                            })

                            st.session_state["txn_conn"] = conn
                            st.session_state["in_transaction"] = True
                            st.session_state["id"] = del_id

                            st.success(f"Movie with ID {del_id} deleted")
                else:
                    st.error(f"No connection to {curr_node}")
            except Exception as e:
                st.error(f"Delete failed: {e}")

if st.session_state["id"]:
    show_surrounding_rows(None, st.session_state["id"])  # Pass None since function uses its own read connection

# --- Commit / Rollback ---
if st.session_state["in_transaction"]:
    col4, col5 = st.columns(2, gap="large")

    clicked_commit = False
    clicked_rollback = False

    with col4:
        if st.button("Confirm", key="txn_confirm", type="primary", use_container_width=True):
            clicked_commit = True

    with col5:
        if st.button("Cancel", key="txn_rollback", use_container_width=True):
            clicked_rollback = True

    if clicked_commit:
        conn = st.session_state["txn_conn"]
        conn.commit()

        # Now execute all pending replications after commit
        for replication in st.session_state["pending_replications"]:
            succ, fail, errs = replicate_update(curr_node, replication["targets"], replication["sql_text"])
            
            # Log as POST_COMMIT since we already committed
            ok, ierr = insert_replication_log(
                nodes[curr_node],
                replication["tconst"],
                replication["sql_text"],
                replication["op_type"],
                replication["targets"],
                last_error=str(errs) if fail else None,
                txn_stage="POST_COMMIT"
            )
            if not ok:
                st.error(f"Failed to insert replication log: {ierr}")

            if fail:
                st.warning(f"Replication to {fail} failed for {replication['op_type']} {replication['tconst']}; task saved for retry.")
            else:
                st.success(f"Replicated {replication['op_type']} {replication['tconst']} to: {succ}")

        # Clear pending replications
        st.session_state["pending_replications"] = []

        # Log POST_COMMIT for this transaction
        ok, ierr = insert_replication_log(
            nodes[curr_node],
            st.session_state["id"],
            "COMMIT TRANSACTION",
            "OTHER",
            [curr_node],
            txn_stage="POST_COMMIT"
        )
        if not ok:
            st.error(f"Failed to insert post-commit log: {ierr}")

        st.session_state["in_transaction"] = False
        st.session_state["txn_conn"] = None
        conn.close()  # Close the connection to ensure proper isolation cleanup
        
        # Clean up read connections to ensure fresh reads after commit
        cleanup_session_connections()
        
        st.success("Transaction committed!")
        st.rerun()

    if clicked_rollback:
        conn = st.session_state["txn_conn"]
        conn.rollback()
        conn.close()  # Close the connection to ensure proper isolation cleanup
        
        # Clear pending replications since transaction was rolled back
        st.session_state["pending_replications"] = []
        
        st.session_state["in_transaction"] = False
        st.session_state["txn_conn"] = None
        
        # Clean up read connections to ensure fresh reads after rollback
        cleanup_session_connections()
        
        st.warning("Transaction rolled back!")
        st.rerun()
