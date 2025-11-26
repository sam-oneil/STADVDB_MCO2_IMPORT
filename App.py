import streamlit as st
import pandas as pd
from Connect import connect_node, replicate_update
import mysql.connector

# CONFIG
database = "YOUR_DATABASE"  # your database
table = "YOUR_TABLE"        # your table
node_ports = {"Node0": 60793, "Node1": 60794, "Node2": 60795}
isolation_levels = ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]

# SESSION STATE
for key in ["txn_active", "txn_sql", "txn_node", "isolation_level"]:
    if key not in st.session_state:
        st.session_state[key] = False if key == "txn_active" else None
if st.session_state.isolation_level is None:
    st.session_state.isolation_level = "READ COMMITTED"

# PAGE
st.set_page_config(layout="wide")
st.title("Distributed Database Concurrency & Replication Test")
col1, col2 = st.columns([1,2])

# NODE SELECTION
with col1:
    st.header("Node & Isolation Level")
    node_choice = st.selectbox("Select Current Node", ["Node0", "Node1", "Node2"])
    st.session_state.isolation_level = st.selectbox("Select Isolation Level:", isolation_levels)
    st.markdown("---")
    st.info(f"Current Node: **{node_choice}** | Isolation: **{st.session_state.isolation_level}**")

# MYSQL CONNECTION
try:
    conn = mysql.connector.connect(
        host="ccscloud.dlsu.edu.ph",
        port=node_ports[node_choice],
        user="user",
        password="nGERH3tcswdCpXr7vTYFDB4M",
        database=database
    )
    cursor = conn.cursor()
    cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {st.session_state.isolation_level}")
    st.success("Connected to MySQL successfully.")
except Exception as e:
    st.error(f"Failed to connect to MySQL: {e}")
    conn = None

# HELPER
def get_target_nodes(node, item_id, partition_limit=50):
    if node == "Node0":
        return ["Node1"] if item_id <= partition_limit else ["Node2"]
    return ["Node0"]

# TRANSACTION SIMULATOR
with col2:
    st.header("Transaction Simulator")
    txn_col1, txn_col2, txn_col3 = st.columns(3)

    # START
    if not st.session_state.txn_active and txn_col1.button("START TRANSACTION", use_container_width=True):
        try:
            cursor.execute("START TRANSACTION")
            st.session_state.txn_active = True
            st.session_state.txn_node = node_choice
            st.toast("Transaction started!")
        except Exception as e:
            st.error(f"Error starting transaction: {e}")

    # TXN ACTIVE
    if st.session_state.txn_active:
        txn_col1.success("TXN Active")

        # COMMIT & REPLICATE
        if txn_col2.button("COMMIT & Replicate", use_container_width=True):
            try:
                cursor.execute("COMMIT")
                if st.session_state.txn_sql and st.session_state.txn_sql.startswith("UPDATE"):
                    item_id = int(st.session_state.txn_sql.split('WHERE item_id = ')[-1].split(';')[0].strip())
                    target_nodes = get_target_nodes(st.session_state.txn_node, item_id)
                    st.toast("Local commit successful. Replicating...")
                    success, failure = replicate_update(st.session_state.txn_node, target_nodes, st.session_state.txn_sql)
                    if failure:
                        st.warning(f"Replication failed on: {', '.join(failure)}")
                    else:
                        st.success(f"Replication successful on: {', '.join(success)}")
                else:
                    st.success("Local commit successful.")
                st.session_state.txn_active = False
                st.session_state.txn_sql = None
            except Exception as e:
                st.error(f"Commit/Replication error: {e}")

        # ROLLBACK
        if txn_col3.button("ROLLBACK", use_container_width=True):
            try:
                cursor.execute("ROLLBACK")
                st.session_state.txn_active = False
                st.session_state.txn_sql = None
                st.warning("Transaction rolled back.")
            except Exception as e:
                st.error(f"Rollback error: {e}")

    # READ
    item_id_read = st.number_input("Enter Item ID for READ/WRITE:", min_value=1, value=1, step=1)
    if st.button(f"READ Item ID {item_id_read}", disabled=not st.session_state.txn_active):
        try:
            query = f"SELECT * FROM {table} WHERE item_id = {item_id_read}"
            st.session_state.txn_sql = query
            cursor.execute(query)
            rows = cursor.fetchall()
            if cursor.description:
                df = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
                st.dataframe(df)
            else:
                st.info("No data found.")
            st.toast("READ executed.")
        except Exception as e:
            st.error(f"Read error: {e}")

    # WRITE
    new_value = st.text_input("New Value for 'value' column:")
    if st.button(f"WRITE Item ID {item_id_read}", disabled=not st.session_state.txn_active):
        sql_update = f"UPDATE {table} SET value = '{new_value}' WHERE item_id = {item_id_read}"
        st.session_state.txn_sql = sql_update
        try:
            cursor.execute(sql_update)
            st.success(f"WRITE executed locally: {sql_update}")
        except Exception as e:
            st.error(f"Write error: {e}")

# GLOBAL DATA CHECK
st.markdown("---")
st.header("Global Data Consistency")
if conn and st.button("Refresh Global View"):
    try:
        cursor.execute(f"SELECT * FROM {table} ORDER BY item_id ASC LIMIT 100")
        rows = cursor.fetchall()
        if cursor.description:
            df = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
            st.dataframe(df)
        else:
            st.warning("Table not found or empty.")
    except Exception as e:
        st.error(f"Error fetching data: {e}")

# CLEANUP
if conn:
    cursor.close()
    conn.close()
