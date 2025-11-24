import streamlit as st
import pandas as pd
from Connect import connect_node, replicate_update
import time

if 'txn_active' not in st.session_state:
    st.session_state.txn_active = False
if 'txn_sql' not in st.session_state:
    st.session_state.txn_sql = None
if 'txn_node' not in st.session_state:
    st.session_state.txn_node = None
if 'isolation_level' not in st.session_state:
    st.session_state.isolation_level = "READ COMMITTED"

st.set_page_config(layout="wide")
st.title("Distributed Database Concurrency & Replication Test")

col1, col2 = st.columns([1, 2])

with col1:
    st.header("Node & Isolation Level:")

    node_choice = st.selectbox(
        "Select Current Node", 
        ["Node0", "Node1", "Node2"], 
        key="node_select"
    )

    st.session_state.isolation_level = st.selectbox(
        "Select Isolation Level:", 
        ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]
    )
    
    st.markdown("---")
    st.info(f"Current Node: **{node_choice}** | Isolation: **{st.session_state.isolation_level}**")
    
conn = connect_node(node_choice)

if conn:
    cursor = conn.cursor()
    try:
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {st.session_state.isolation_level}")
        st.success("Database connection successful and isolation level set.")
    except Exception as e:
        st.error(f"Failed to set isolation level: {e}")
        conn = None

def get_target_nodes(node, item_id, partition_limit=50):
    if node == "Node0":
        if item_id <= partition_limit:
            return ["Node1"]
        else:
            return ["Node2"]
    elif node == "Node1" or node == "Node2":
        return ["Node0"]
    return []

with col2:
    st.header("Transaction Simulator (for Concurrency Testing):")
    
    txn_col1, txn_col2, txn_col3 = st.columns(3)
    
    if not st.session_state.txn_active:
        if txn_col1.button("START TRANSACTION", use_container_width=True, type="primary"):
            try:
                cursor.execute("START TRANSACTION")
                st.session_state.txn_active = True
                st.session_state.txn_node = node_choice
                st.toast("Transaction Started!")
            except Exception as e:
                st.error(f"Error starting transaction: {e}")
                
    if st.session_state.txn_active:
        txn_col1.success("TXN Active")
        
        if txn_col2.button("COMMIT & Replicate", use_container_width=True):
            try:
                cursor.execute("COMMIT")
                
                if st.session_state.txn_sql and st.session_state.txn_sql.startswith("UPDATE"):                
                    item_id_str = st.session_state.txn_sql.split('WHERE item_id = ')[-1].split(';')[0].strip()
                    item_id = int(item_id_str)
                    
                    target_nodes = get_target_nodes(st.session_state.txn_node, item_id)
                    
                    st.toast("Local Commit Successful. Starting Replication...")
                    
                    success, failure = replicate_update(
                        st.session_state.txn_node, 
                        target_nodes, 
                        st.session_state.txn_sql
                    )
                    
                    if failure:
                         st.warning(f"Replication finished with errors. Failed on: {', '.join(failure)}")
                    else:
                         st.success(f"Replication successful on: {', '.join(success)}")
                    
                else:
                    st.success("Local Commit Successful.")
                
                st.session_state.txn_active = False
                st.session_state.txn_sql = None
                
            except Exception as e:
                st.error(f"Commit/Replication Error: {e}")
                
        if txn_col3.button("ROLLBACK", use_container_width=True, type="secondary"):
            try:
                cursor.execute("ROLLBACK")
                st.session_state.txn_active = False
                st.session_state.txn_sql = None
                st.warning("Transaction Rolled Back.")
            except Exception as e:
                st.error(f"Error during Rollback: {e}")
    
    item_id_read = st.number_input("Enter Item ID for READ/WRITE:", min_value=1, value=1, step=1, key="item_id_input")
    
    if st.button(f"Case #1: READ Item ID {item_id_read}", disabled=not st.session_state.txn_active):
        try:
            query = f"SELECT * FROM {mytable} WHERE item_id = {item_id_read}"
            st.session_state.txn_sql = query
            cursor.execute(query)
            rows = cursor.fetchall()
            if cursor.description:
                columns = [i[0] for i in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                st.markdown("#### Read Result (Transaction Output):")
                st.dataframe(df)
            else:
                st.info("No data found for this ID in the current node's partition.")
            st.toast("READ executed.")
        except Exception as e:
            st.error(f"Read Error: {e}")

    st.markdown("---")

    new_value = st.text_input("New Value for 'value' column (e.g., 99.99 or 'Updated'):", key="new_value_input")
    
    if st.button(f"Case #2/3: WRITE (UPDATE) Item ID {item_id_read}", disabled=not st.session_state.txn_active, type="secondary"):
        sql_update_query = f"UPDATE {mytable} SET value = '{new_value}' WHERE item_id = {item_id_read}"
        st.session_state.txn_sql = sql_update_query
        
        try:
            cursor.execute(sql_update_query)
            st.success(f"WRITE executed locally: {sql_update_query}")
            st.info("The transaction is now **pending** (uncommitted). Go to another node/tab to check concurrency effects (Case #2: Read, Case #3: Write).")
        except Exception as e:
            st.error(f"Write Error: {e}")

st.markdown("---")
st.header("Global Data Consistency Check")
if conn and mytable:
    if st.button("Refresh Global View"):
        try:
            query = f"SELECT * FROM {mytable} ORDER BY item_id ASC LIMIT 100"
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if cursor.description:
                columns = [i[0] for i in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                st.markdown(f"Data currently on **{node_choice}** (First 100 rows):")
                st.dataframe(df, use_container_width=True)
            else:
                st.warning("Table not found or empty.")
                
        except Exception as e:
            st.error(f"Error fetching data: {e}")

if conn:
    cursor.close()
    conn.close()