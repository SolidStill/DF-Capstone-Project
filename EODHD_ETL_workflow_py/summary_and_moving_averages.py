
import psycopg2
from config import SQL_USER, SQL_PASS, SQL_HOST, bond_symbols_dict
from load import create_connection

def get_bond_table_names():
    try:
        with create_connection() as conn:
            with conn.cursor() as cur:
                # Query for tables in the 'student' schema that start with 'de10_cdw_'
                # but EXCLUDE the summary table with AND table_name != 'de10_cdw_bond_summary'
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'student'
                      AND table_name LIKE 'de10_cdw_%'
                      AND table_name != 'de10_cdw_bond_summary'  
                    """
                )
                tables = [row[0] for row in cur.fetchall()]
                return tables
    except psycopg2.Error as error:
        pass # print(f"Database error: {error}")
        raise

def create_summary_moving_averages_table():
    try:
        bond_table_names = get_bond_table_names()
        table_subquery = " UNION ALL ".join(
            [
                f"SELECT '{symbol}' AS symbol, * FROM {f'de10_cdw_{bond_symbols_dict[symbol][1]}_{bond_symbols_dict[symbol][0]}_gbond'}"
                for symbol in bond_symbols_dict.keys()
            ]
        )

        with create_connection() as conn:
            with conn.cursor() as cur:
                # Drop existing summary table if it exists
                cur.execute("DROP TABLE IF EXISTS student.de10_cdw_bond_summary;")
                conn.commit()

                # Create the table and insert data in a single query
                query = f"""
                    CREATE TABLE student.de10_cdw_bond_summary (
                        symbol TEXT PRIMARY KEY,
                        date DATE,
                        num_yield_reports INT,
                        latest_yield NUMERIC,
                        ma5 NUMERIC,
                        diff_ma5 NUMERIC,  
                        ma20 NUMERIC,
                        diff_ma20 NUMERIC, 
                        ma100 NUMERIC,
                        diff_ma100 NUMERIC
                    );

                    INSERT INTO student.de10_cdw_bond_summary (
                        symbol, date, num_yield_reports, latest_yield, 
                        ma5, diff_ma5, ma20, diff_ma20, ma100, diff_ma100
                    )
                    SELECT
                        symbol,
                        MAX(date) AS date,
                        COUNT(*) AS num_yield_reports,
                        MAX(last_value) AS latest_yield,
                        MAX(avg_5) AS ma5,
                        MAX(last_value - avg_5) AS diff_ma5,
                        MAX(avg_20) AS ma20,
                        MAX(last_value - avg_20) AS diff_ma20,
                        MAX(avg_100) AS ma100,
                        MAX(last_value - avg_100) AS diff_ma100 
                    FROM (
                        SELECT 
                            symbol,
                            date, 
                            adjusted_close,
                            LAST_VALUE(adjusted_close) OVER (PARTITION BY symbol ORDER BY date) AS last_value,
                            AVG(adjusted_close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS avg_5,
                            AVG(adjusted_close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_20,
                            AVG(adjusted_close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS avg_100
                        FROM (
                            {table_subquery}
                        ) AS all_bond_data
                    ) AS subquery_with_window_functions
                    GROUP BY symbol;
                """
                
                cur.execute(query)
                conn.commit()

    except psycopg2.Error as error:
        pass # print(f"Database error: {error}")
        raise
