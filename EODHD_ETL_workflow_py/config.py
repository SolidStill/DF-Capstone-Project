
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

EODHD_API_KEY = os.getenv("EODHD_API_KEY")
SQL_USER = os.getenv("SQL_USER")
SQL_PASS =  os.getenv("SQL_PASS")
SQL_HOST = os.getenv("SQL_HOST")

# Dictionary with  UK/US government bond symbols as keys and with list of maturity classes and countries as values 
# *** I had subtantial issues as a result of the values in this dict being UPPER
# CASE initially ***
bond_symbols_dict = {
    'UK1Y.GBOND': ['1y', 'uk'],
    'UK2Y.GBOND': ['2y', 'uk'],
    'UK3Y.GBOND': ['3y', 'uk'],
    'UK5Y.GBOND': ['5y', 'uk'],
    'UK10Y.GBOND': ['10y', 'uk'],
    'UK30Y.GBOND': ['30y', 'uk'],
    'US1Y.GBOND': ['1y', 'us'],
    'US2Y.GBOND': ['2y', 'us'],
    'US3Y.GBOND': ['3y', 'us'],
    'US5Y.GBOND': ['5y', 'us'],
    'US10Y.GBOND': ['10y', 'us'],
    'US30Y.GBOND': ['30y', 'us'],
    'DE1Y.GBOND': ['1y', 'de'],  # German 1-year bond
    'DE2Y.GBOND': ['2y', 'de'],
    'DE5Y.GBOND': ['5y', 'de'],
    'DE10Y.GBOND': ['10y', 'de'],
    'DE30Y.GBOND': ['30y', 'de'],
}
