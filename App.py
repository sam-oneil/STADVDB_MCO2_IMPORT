import streamlit as st
import socket
from Connect import *

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

if curr_node == "Unknown Node":
    st.error("This application must be run on one of the designated nodes.")
    st.stop()
else:
    st.success(f"Running on {curr_node}")

# --- Session State Initialization ---
if "in_transaction" not in st.session_state:
    st.session_state["in_transaction"] = False

if "id" not in st.session_state:
    st.session_state["id"] = None

if "iso_level" not in st.session_state:
    st.session_state["iso_level"] = "READ UNCOMMITTED"

if "txn_conn" not in st.session_state:
    st.session_state["txn_conn"] = None

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
    isolation_levels = ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]
    selected_level = st.selectbox("Transaction Isolation Level", isolation_levels, index=isolation_levels.index(st.session_state["iso_level"]))

    if st.button("Confirm", type = "primary", width = "stretch"):
        if conn:
            st.session_state["iso_level"] = selected_level
            st.success(f"Isolation level confirmed: {st.session_state['iso_level']}")
        else:
            st.error("No connection to Node 1")

with right_col:
    # --- CRUD Operations ---
    st.markdown("<h2 style='text-align: center;'>CRUD OPERATIONS</h2>", unsafe_allow_html=True) 

    # --- Helper Functions ---
    def start_transaction():
        if not st.session_state["in_transaction"]:
            conn = connections[curr_node]
            cursor = conn.cursor()
            cursor.execute("SET AUTOCOMMIT = 0")
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {st.session_state['iso_level']}")
            cursor.execute("START TRANSACTION")
            cursor.close()

            st.session_state["in_transaction"] = True
            st.session_state["txn_conn"] = conn

    def get_row_by_tconst(tconst):
        try:
            conn = connections[curr_node]
            cursor = conn.cursor(dictionary=True)

            query = "SELECT * FROM titles WHERE tconst = %s"
            cursor.execute(query, (tconst,))
            row = cursor.fetchone()
            cursor.close()

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
    
    def show_surrounding_rows(tconst):
        try:
            conn = connections[curr_node]
            cursor = conn.cursor(dictionary=True)

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

    with col1:
        if st.button("Add", type = "primary", width = "stretch"):
            try:
                if connections[curr_node]:
                    if add_title != "" and is_title_in_node(add_title, curr_node):
                        start_transaction()    
                        conn = st.session_state["txn_conn"]

                        RANGES = {
                            "Node 1": (1, 999_999),
                            "Node 2": (1_000_000, 1_999_999),
                            "Node 3": (2_000_000, 2_999_999)
                        }

                        min_id, max_id = RANGES[curr_node]

                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT MAX(CAST(SUBSTRING(tconst, 3) AS UNSIGNED)) 
                            FROM titles 
                            WHERE CAST(SUBSTRING(tconst, 3) AS UNSIGNED) BETWEEN %s AND %s
                        """, (min_id, max_id))
                        result = cursor.fetchone()
                        cursor.close()

                        last_num = result[0] if result[0] else (min_id - 1)
                        new_num = last_num + 1

                        if new_num > max_id:
                            st.error("ID range exhausted for this node!")
                        else:
                            new_tconst = "tt" + str(new_num).zfill(7)

                        sql = "INSERT INTO titles (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, runtimeMinutes, genres) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                        val = (new_tconst, "movie", add_title, add_title, 0, add_year, "\\N", add_genre)
                        cursor.execute(sql, val)

                        cursor.close()

                        st.success(f"'{add_title}' added successfully with ID {new_tconst}")
                        st.session_state["id"] = new_tconst
                    elif add_title != "":
                        st.error(f"Title '{add_title}' does not belong to {curr_node}.")
                    else:
                        st.error("Title cannot be empty")
                else:
                    st.error(f"No connection to {curr_node}.")
            except Exception as e:
                st.error(f"Add failed: {e}")
    
    with col2:
          if st.button("Update", type = "primary", width = "stretch"):
            try:
                if connections[curr_node]:
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
                                start_transaction()
                                conn = st.session_state["txn_conn"]

                                cursor = conn.cursor()

                                if upd_title != "":
                                    sql = "UPDATE titles SET primaryTitle = %s WHERE tconst = %s"
                                    val = (upd_title, upd_id)
                                    cursor.execute(sql, val)

                                if upd_year != 0:
                                    sql = "UPDATE titles SET startYear = %s WHERE tconst = %s"
                                    val = (upd_year, upd_id)
                                    cursor.execute(sql, val)

                                if upd_genre != "":
                                    sql = "UPDATE titles SET genres = %s WHERE tconst = %s"
                                    val = (upd_genre, upd_id)
                                    cursor.execute(sql, val)

                                cursor.close()

                                st.success(f"'{upd_id}' updated successfully")
                                st.session_state["id"] = upd_id
                        else:
                            st.error("At least one must be filled")
                else:
                    st.error(f"No connection to {curr_node}")
            except Exception as e:
                st.error(f"Update failed: {e}")

    with col3:
        if st.button("Delete", type = "primary", width = "stretch"):
            try:
                if connections[curr_node]:
                    if del_id.strip() == "":
                        st.error("tconst cannot be empty")
                    else:
                        row = get_row_by_tconst(conn, del_id.strip())
                        if not row:
                            st.error(f"No record found with ID {del_id.strip()}")
                        else:
                            start_transaction()
                            conn = st.session_state["txn_conn"]

                            cursor = conn.cursor()
                            sql = "DELETE FROM titles WHERE tconst = %s"
                            cursor.execute(sql, (del_id,))

                            cursor.close()

                            st.success(f"Movie with ID {del_id} deleted")
                            st.session_state["id"] = del_id
                else:
                    st.error(f"No connection to {curr_node}")
            except Exception as e:
                st.error(f"Delete failed: {e}")

if st.session_state["id"]:
    show_surrounding_rows(st.session_state["id"])

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
        st.session_state["in_transaction"] = False
        st.session_state["txn_conn"] = None
        st.success("Transaction committed!")
        st.rerun()  

    if clicked_rollback:
        conn = st.session_state["txn_conn"]
        conn.rollback()
        st.session_state["in_transaction"] = False
        st.session_state["txn_conn"] = None
        st.warning("Transaction rolled back!")
        st.rerun()

