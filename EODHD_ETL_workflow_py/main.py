
from config import EODHD_API_KEY, SQL_USER, SQL_PASS, SQL_HOST, bond_symbols_dict
from update import update_data
from summary_and_moving_averages import create_summary_moving_averages_table

if __name__ == "__main__":
    try:
        update_data(bond_symbols_dict)
    except Exception as e:
        pass # print(f"Error occurred: {e}")
    else:  # Execute if no exceptions
        create_summary_moving_averages_table() 
    
