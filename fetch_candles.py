# fetch_candles.py
import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone

# --- Configuration ---
BASE_URL = "https://api.coinbase.com/api/v3/brokerage/market/products"
PRODUCT_ID = "BTC-USD"
START_DATE = datetime(2025, 11, 1, 0, 0, 0, tzinfo=timezone.utc) # UTC timezone
END_DATE = datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)   # UTC timezone

# Format dates for filename (safe for filesystem)
start_str = START_DATE.strftime("%Y%m%d_%H%M")
end_str = END_DATE.strftime("%Y%m%d_%H%M")
OUTPUT_FILENAME = f"BTC-USD_OHLCV_1min_{start_str}_{end_str}.csv"


def fetch_candle_batch(start: str, end: str) -> list:
    """Fetches a single batch of candles from Coinbase API for BTC-USD at ONE_MINUTE granularity."""
    url = f"{BASE_URL}/{PRODUCT_ID}/candles"
    params = {
        "start": start,
        "end": end,
        "granularity": "ONE_MINUTE"
    }
    headers = {
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        return data.get("candles", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

def main():
    all_candles = []
    current_start = START_DATE
    granularity_seconds = 60 # Hardcoded for ONE_MINUTE
    max_batch_minutes = 299 # Hardcoded for 300 candles at ONE_MINUTE granularity

    print(f"Fetching {PRODUCT_ID} OHLCV data from {START_DATE} to {END_DATE} with ONE_MINUTE granularity...")

    while current_start < END_DATE:
        # Max batch duration (e.g., 299 minutes for 300 candles at 1-min granularity)
        # Ensure it's at least 1 minute to avoid issues when granularity is very large
        max_batch_minutes = max(1, 299 * (granularity_seconds // 60))

        # Calculate the end time for the current API request, limited by END_DATE.
        # This will be `current_start` + `max_batch_minutes`.
        batch_end = min(current_start + timedelta(minutes=max_batch_minutes), END_DATE)

        # If current_start is already at or past END_DATE, break before making a request
        if current_start >= END_DATE:
            break

        # Ensure batch_end is strictly after current_start if possible, or set to END_DATE
        # This handles cases where END_DATE is very close to current_start.
        if batch_end <= current_start:
            if current_start < END_DATE:
                batch_end = END_DATE
            else:
                break # Should be caught by the current_start >= END_DATE check

        print(f"  Fetching batch from {current_start} to {batch_end}...")

        start_ts = str(int(current_start.timestamp()))
        end_ts = str(int(batch_end.timestamp()))

        candles = fetch_candle_batch(start_ts, end_ts)
        if candles:
            all_candles.extend(candles)
            # Advance current_start to the end of the fetched batch, plus one granularity step
            # to ensure the next fetch starts immediately after the last one.
            # The API's 'end' is inclusive, so we move past it.
            current_start = batch_end + timedelta(seconds=granularity_seconds)
        else:
            # If no candles returned for a range, advance current_start anyway to avoid infinite loop
            # and to progress through time for sparse data periods.
            current_start = batch_end + timedelta(seconds=granularity_seconds)
        
        # Ensure current_start does not accidentally go beyond END_DATE
        current_start = min(current_start, END_DATE)

        # Be nice to the API
        time.sleep(0.1)

    if not all_candles:
        print("No candles fetched.")
        return

    # Process and save data
    df = pd.DataFrame(all_candles)
    # The API returns 'start' as a Unix timestamp string
    df['start'] = pd.to_numeric(df['start']).apply(lambda x: datetime.fromtimestamp(x, tz=timezone.utc))
    df = df.rename(columns={
        'start': 'timestamp',
        'low': 'low',
        'high': 'high',
        'open': 'open',
        'close': 'close',
        'volume': 'volume'
    })

    # Convert OHLCV columns to numeric
    for col in ['low', 'high', 'open', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col])

    # Sort by timestamp in ascending order
    df = df.sort_values(by='timestamp').reset_index(drop=True)

    # Remove duplicates that might occur due to API behavior or edge cases in time windows
    df.drop_duplicates(subset=['timestamp'], inplace=True)

    # Format timestamp to 'yyyy-mm-dd hh:mm:ss'
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Save to CSV
    df.to_csv(OUTPUT_FILENAME, index=False)
    print(f"\nSuccessfully downloaded {len(df)} candles and saved to {OUTPUT_FILENAME}")

if __name__ == "__main__":
    main()
