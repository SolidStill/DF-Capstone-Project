import streamlit as st
import pandas as pd
import psycopg2  # Import for database connection

@st.cache_resource
def create_connection():
    db_info = st.secrets
    try:
        return psycopg2.connect(
            database="pagila",  # Replace with your database name
            user=db_info["SQL_USER"],
            password=db_info["SQL_PASS"],
            host=db_info["SQL_HOST"],
            port=5432
        )
    except psycopg2.Error as e:
        st.error(f"Error connecting to database: {e}")
        st.stop()

@st.cache_data(ttl=3600)
def get_bond_data(table_name):
    """Fetches bond data from the specified table"""
    with create_connection() as conn:
        return pd.read_sql_query(f"SELECT * FROM student.\"{table_name}\"", conn)  # Replace 'student' with your actual schema name

@st.cache_data(ttl=3600)
def get_bond_summary_data(symbol):
    """Fetches summary data for the selected bond symbol"""
    with create_connection() as conn:
        return pd.read_sql_query(
            f"SELECT * FROM student.de10_cdw_bond_summary WHERE symbol = '{symbol}'",  
            conn
        )
