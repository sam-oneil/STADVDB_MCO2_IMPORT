import streamlit as st
from Connect import *

st.set_page_config(layout="wide")
st.markdown("<h1 style='text-align: center;'>Distributed Database Management System</h1>", unsafe_allow_html=True)
left_col, right_col = st.columns([1, 2], gap="large")

with left_col:
    # --- Node Status ---
    connections, ping_results = connect_node(nodes)
    conn = connections["Node 1"]  # Main connection

    st.header("Node Status")
    cols = st.columns(len(nodes), gap="large")
    for i, (node_name, status) in enumerate(ping_results.items()):
        with cols[i]:
            st.subheader(node_name)
            if status == "Reachable":
                st.success("● Reachable")
            else:
                st.error("● Unreachable")

    # --- Isolation Level ---
    st.header("Isolation Level")
    isolation_levels = ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]
    selected_level = st.selectbox("Transaction Isolation Level", isolation_levels)

    if st.button("Confirm", type = "primary", width = "stretch"):
        if conn:
            isolation_choice = selected_level
            st.success(f"Isolation level confirmed: {isolation_choice}")
        else:
            st.error("No connection to Node 1")

with right_col:
    # --- CRUD Operations ---
    st.markdown("<h2 style='text-align: center;'>CRUD OPERATIONS</h2>", unsafe_allow_html=True) 

    col1, col2, col3 = st.columns(3, gap="large")
    
    with col1:
        st.markdown("<h3 style='text-align: center;'>Add Title</h3>", unsafe_allow_html=True) 
        add_title = st.text_input("Title", key="add_title")
        add_year = st.number_input("Year", min_value=1900, max_value=2100, key="add_year")
        add_genre = st.text_input("Genre", key="add_genre")
             
    with col2:
        st.markdown("<h3 style='text-align: center;'>Update Title</h3>", unsafe_allow_html=True) 
        upd_id = st.number_input("ID", key="upd_id")
        upd_title = st.text_input("Title", key="upd_title")
        upd_year = st.number_input("Year", min_value=1900, max_value=2100, key="upd_year")
        upd_genre = st.text_input("Genre", key="upd_genre")
        
    with col3:
        st.markdown("<h3 style='text-align: center;'>Delete Title</h3>", unsafe_allow_html=True) 
        del_id = st.number_input("ID", key="del_id")
        
    col1, col2, col3 = st.columns(3, gap="large")

    with col1:
        if st.button("Add", type = "primary", width = "stretch"):
            try:
                if conn:
                    if add_title != "":
                        #insert logic here
                        st.success(f"'{add_title}' added successfully")
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
                    if upd_id <= 0:
                        st.error("ID must be greater than 0")
                    elif upd_title != "" and upd_year != 0 and upd_genre != "":
                        #insert logic here
                        st.success(f"'{upd_title}' updated successfully")
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
                    if del_id <= 0:
                        st.error("ID must be greater than 0")
                    else:
                        #insert logic here
                        st.success(f"Movie with ID {del_id} deleted")
                else:
                    st.error("No connection to Node 1")
            except Exception as e:
                st.error(f"Delete failed: {e}")
