
import pandas as pd
import psycopg2

from extract_transform import get_eod_data
from load import create_connection, load_data_into_database
from config import EODHD_API_KEY, SQL_USER, SQL_PASS, SQL_HOST, bond_symbols_dict

def fetch_latest_ingested_date(symbol):
    """This function exists to help me determine the delta that requires inserting to fulfill the update. It returns the latest data in DB table, or 'None' if table doesn't exist or is empty."""
    ## I have an ongoing concern that this fn may return 'None' in the event of a connection error or similar - this could mess with the logic of the fn that calls this one and cause an attempt to INSERT already existing data, leading to "duplicate primary key" errors from DB

    maturity_class, country = bond_symbols_dict[symbol]
    table_name = f"de10_cdw_{country}_{maturity_class}_gbond"
    pass # print(f"Checking for table: {table_name}")  # Debug print

    try:
        with create_connection() as conn:
            with conn.cursor() as cur:
                # Check if the table exists
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'student' AND table_name = %s)",
                    (table_name,) #trailing comma ensures arg read as a tuple and not a string
                )
                table_exists = cur.fetchone()[0]
                pass # print(f"Table exists: {table_exists}")  # Debug print

                if table_exists:
                    # Check if the table is empty
                    cur.execute(f"SELECT COUNT(*) FROM student.{table_name};")
                    row_count = cur.fetchone()[0]
                    pass # print(f"Row count: {row_count}")  # Debug print

                    if row_count > 0:
                        cur.execute(f"SELECT MAX(date) FROM student.{table_name};")
                        latest_date = cur.fetchone()[0]
                        pass # print(f"Latest date found: {latest_date}")  # Debug print
                        return latest_date
                    else:
                        pass # print("Table is empty.")  # Debug print
                        return None
                else:
                    pass # print("Table does not exist.")  # Debug print
                    return None
    except psycopg2.Error as e:
        pass # print(f"Database error while fetching latest date: {e}")
        raise

def update_data(symbol_dict):
    """Fetches, transforms, and loads the latest data for all specified symbols; if there is no existing data in the corresponding table (or the table does not exist), ALL existing data will be fetched and loaded"""
    
    # this loop iterates through each bond type, fetches all API data for that bond and filters to keep all rows that are newer than in the current DB table - if there are in fact any new rows, they are loaded into the DB
    for symbol, (maturity_class, country) in symbol_dict.items():
        # using a 'while' loop below to allow 3 tries at each symbol update, to build-in falut-tolerance in the event that there is an intermittent connection error or similar
        retries = 0
        while retries < 3:
            try:
              latest_date_in_db = fetch_latest_ingested_date(symbol)

              # Current method fetches all available API data before filtering, only around 30KB per symbol for entire historical feed
              all_data_df = get_eod_data(symbol, EODHD_API_KEY)

              if latest_date_in_db: #true if there exists a "latest date" in table
                  # Filter out data already in the database
                  latest_date_in_db = pd.Timestamp(latest_date_in_db)
                  new_data_df = all_data_df[all_data_df['date'] > latest_date_in_db]
              else: #in the event the above if statement is false, ALL data from API loaded.
                  new_data_df = all_data_df     
              # NB must be careful about a fleeting error during fetch_latest_ingested_date(): if latest_date_in_db is returned as 'None', this will cause a load of all API data during update_data() (ie duplicate data) - have tried re-raising the exception in order to force the process to terminate

              if not new_data_df.empty:
                  load_data_into_database(new_data_df, symbol)
                  pass # print(f"Successfully updated data for {symbol}")
              else:
                  pass # print(f"No new data found for {symbol}")
              break  # Exit the loop after successful update
              
            except Exception as e:
              pass # print(f"Error updating data for {symbol}: {e}")
              retries += 1
              if retries >= 3:
                  raise  # Re-raise the error for potential error handling in 'higher-level' calling functions
    pass # print("***API retrieval completed for all symbols***")
