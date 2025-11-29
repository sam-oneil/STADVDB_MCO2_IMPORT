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
    def start_transaction(conn):
        """Start a new transaction if none is active."""
        if not st.session_state["in_transaction"]:
            cursor = conn.cursor()
            cursor.execute("SET AUTOCOMMIT = 0")
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {st.session_state['iso_level']}")
            cursor.execute("START TRANSACTION")
            cursor.close()

            st.session_state["in_transaction"] = True

    def show_surrounding_rows(conn, tconst):
        try:
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

    if st.button("Search", type = "secondary"):
        if conn and search_term.strip() != "":
            try:
                cursor = conn.cursor(dictionary=True)
                query = "SELECT * FROM titles WHERE tconst = %s"
                cursor.execute(query, (search_term,))
                row = cursor.fetchall()
                cursor.close()

                if row:
                    st.subheader("SEARCH RESULT")
                    st.json(row)
                else:
                    st.info(f"No record found with tconst: {search_term}")

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
                if conn:
                    if add_title != "":
                        start_transaction(conn)    
                    
                        cursor = conn.cursor()
                        cursor.execute("SELECT tconst FROM titles ORDER BY tconst DESC LIMIT 1")
                        row = cursor.fetchone()

                        if row:
                            last_id = row[0]
                            num_part = int(last_id[2:]) + 1
                            new_tconst = "tt"+str(num_part).zfill(7)
                        else:
                            new_tconst = 'tt0000001'

                        sql = "INSERT INTO titles (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, runtimeMinutes, genres) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                        val = (new_tconst, "movie", add_title, add_title, 0, add_year, "\\N", add_genre)
                        cursor.execute(sql, val)

                        cursor.close()

                        st.success(f"'{add_title}' added successfully")
                        st.session_state["id"] = new_tconst
                    else:
                        st.error("Title cannot be empty")
                else:
                    st.error("No connection to Node 1")
            except Exception as e:
                st.error(f"Add failed: {e}")
    
    with col2:
          if st.button("Update", type = "primary", width = "stretch"):
            try:
                if conn:
                    if upd_id.strip() == "":
                        st.error("tconst cannot be empty")
                    elif upd_title != "" or upd_year != 0 or upd_genre != "":
                        start_transaction(conn)

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
                    st.error("No connection to Node 1")
            except Exception as e:
                st.error(f"Update failed: {e}")

    with col3:
        if st.button("Delete", type = "primary", width = "stretch"):
            try:
                if conn:
                    if del_id.strip() == "":
                        st.error("tconst cannot be empty")
                    else:
                        start_transaction(conn)

                        cursor = conn.cursor()
                        sql = "DELETE FROM titles WHERE tconst = %s"
                        cursor.execute(sql, (del_id,))

                        cursor.close()

                        st.success(f"Movie with ID {del_id} deleted")
                        st.session_state["id"] = del_id
                else:
                    st.error("No connection to Node 1")
            except Exception as e:
                st.error(f"Delete failed: {e}")

if st.session_state["id"]:
    show_surrounding_rows(conn, st.session_state["id"])

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
        conn.commit()
        st.session_state["in_transaction"] = False
        st.success("Transaction committed!")
        st.rerun()  

    if clicked_rollback:
        conn.rollback()
        st.session_state["in_transaction"] = False
        st.warning("Transaction rolled back!")
        st.rerun()

