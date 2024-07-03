
import requests
import pandas as pd
from config import EODHD_API_KEY

# from config import EODHD_API_KEY
def get_eod_data(symbol, api_token):
    """Fetches EOD (End of Day) data for a given symbol from the EODHD API."""

    base_url = "https://eodhd.com/api/eod"
    url = f"{base_url}/{symbol}?api_token={api_token}&fmt=json"

    response = requests.get(url)
    response.raise_for_status()  # Raise an exception if the request failed

    data = response.json()

    # Create DataFrame and filter for the specified number of days
    df = pd.DataFrame(data)
    df = df.drop("volume", axis=1) # Drop the 'volume' column because the data is not relevant
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", ascending=False, inplace=True)

    return df
