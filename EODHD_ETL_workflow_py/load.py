
import psycopg2
from config import EODHD_API_KEY, SQL_USER, SQL_PASS, SQL_HOST, bond_symbols_dict

"""
Helper functions in this .py file:
create_connection(), 
create_database_tables(symbol),
load_data_into_database(df, symbol)
"""

def create_connection():
    """Creates a connection to the PostgreSQL database. Helps me use graceful 'with' statements when handling DB connections"""

    try:
        my_user = SQL_USER
        my_password = SQL_PASS
        my_host = SQL_HOST

        conn = psycopg2.connect(
            database="pagila",
            user=my_user,
            host=my_host,
            password=my_password,
            port=5432  
        )

        return conn

    except psycopg2.Error as e:
        pass # print(f"Error connecting to database: {e}")
        raise  # Re-raise the error for potential error handling in 'higher-level' calling functions



def create_database_tables(symbol):
    """Creates the required tables in the PostgreSQL database if they don't exist"""

    maturity_class, country = bond_symbols_dict[symbol]
    table_name = f"de10_cdw_{country}_{maturity_class}_gbond"

    # using 'with' statements to help handle connection/cursor objects gracefully
    try:
      with create_connection() as conn:
        with conn.cursor() as cur:
          
          # creates table under "student" schema
          create_table_sql = f"""
              CREATE TABLE IF NOT EXISTS student.{table_name} (
                  date DATE PRIMARY KEY,
                  open NUMERIC,
                  high NUMERIC,
                  low NUMERIC,
                  close NUMERIC,
                  adjusted_close NUMERIC
              )
          """
          cur.execute(create_table_sql)
          conn.commit()
    
    except psycopg2.Error as e:
        pass # print(f"Error creating table '{table_name}': {e}")
        raise  # Re-raise the error for potential error handling in 'higher-level' calling functions

def load_data_into_database(df, symbol):
    """Loads data into the specified 'symbol' table in the database."""

    maturity_class, country = bond_symbols_dict[symbol]
    try:
      with create_connection() as conn:
          create_database_tables(symbol)  # Ensure tables exist before loading
          with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(f"""
                    INSERT INTO student.de10_cdw_{country}_{maturity_class}_gbond (date, open, high, low, close, adjusted_close)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (row["date"],
                      row["open"],
                      row["high"],
                      row["low"],
                      row["close"],
                      row["adjusted_close"])
                )
                # '%s' place holders in INSERT help with: 
                # 1)Prevent SQL Injection, 
                # 2)Type Safety: psycopg2 will automatically convert the Python data types into their corresponding PostgreSQL types
            conn.commit()
    
    except psycopg2.Error as error:
        pass # print(f"Database error: {error}")
        raise  # Re-raise the error for potential handling in 'higher-level' calling functions
