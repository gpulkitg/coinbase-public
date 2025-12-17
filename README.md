# Coinbase Public Product Candles Downloader

A Python script to download historical OHLCV (Open, High, Low, Close, Volume) data from the Coinbase Advanced Trade API.

## Features

*   **Public API:** Uses the Coinbase public product candles endpoint (no API key required).
*   **Granularity:** Fetches 1-minute interval data.
*   **Pagination:** Automatically handles pagination to download data over long time ranges.
*   **CSV Export:** Saves data to a CSV file with formatted timestamps (`yyyy-mm-dd hh:mm:ss`).
*   **Duplicate Handling:** Ensures no duplicate records are saved.

## Requirements

*   Python 3.x
*   `requests`
*   `pandas`

## Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/gpulkitg/coinbase-public.git
    cd coinbase-public
    ```

2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1.  Open `fetch_candles.py` and configure the `START_DATE` and `END_DATE` variables as needed.
2.  Run the script:
    ```bash
    python fetch_candles.py
    ```
3.  The data will be saved to a CSV file named `BTC-USD_OHLCV_1min_<START>_<END>.csv`.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
