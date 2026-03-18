import requests
import pandas as pd
import sqlite3
import logging
from datetime import datetime, timezone, timedelta

# Set the timezone to UTC +7 TH
th_tz = timezone(timedelta(hours=7))

# Get the current time in UTC +7 TH
logging.Formatter.converter = staticmethod(lambda ts: datetime.fromtimestamp(ts, th_tz).timetuple())

# Configure logging
logging.basicConfig(
    level = logging.INFO, # Set the logging level
    format = '%(asctime)s - %(levelname)s - %(message)s', # Set the log message format
    datefmt = '%Y-%m-%d %H:%M:%S'
)

# Create a logger instance
logger = logging.getLogger(__name__)

# Extract Process
def extract_crypto_data():
    logger.info("Starting crypto data extraction...")
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 15,
        "page": 1,
        "sparkline": False
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Extract success Retrieved {len(data)} coins.")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during extraction: {e}", exc_info = True)
        return None

# Transform Process
def transform_crypto_data(raw_data):
    logger.info("Starting Transform Process...")
    if not raw_data:
        logger.warning("No data to transform.(raw_data is empty or none)")
        return None

    df = pd.DataFrame(raw_data)
    columns_to_keep = ['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume', 'last_updated']
    df = df[columns_to_keep]
    df = df.dropna()

    df['last_updated'] = pd.to_datetime(df['last_updated'])
    logger.info(f"Transform success. DataFrame shape: {df.shape[0]} rows, {df.shape[1]} columns.")
    return df

# Load Process
def load_crypto_data(df, db_name='crypto_data.db', table_name='crypto_prices'):
    logger.info("Starting Load Process...")
    if df is None or df.empty:
        logger.warning("No data to load. (DataFrame is empty or None))")
        return False

    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        logger.info(f"Data loaded successfully to table '{table_name}' in database '{db_name}'.")

        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        logger.info(f"Verified Found '{row_count}' rows in the database table.")

        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error during loading: {e}", exc_info = True)
        return False

# Start Process Run
if __name__ == "__main__":
    logger.info("Starting ETL Process...")

    # Extract: E
    raw_data = extract_crypto_data()

    if raw_data:
        # Transform: T
        transformed_data = transform_crypto_data(raw_data)

        if transformed_data is not None:
            # Load: L
            load_success = load_crypto_data(transformed_data)
            if  load_success:
                logger.info("ETL Process completed successfully.")
            else:
                logger.error("ETL Process failed during the load phase.")
        else:
             logger.error("ETL Process failed during the transform phase.")
    else:
        logger.error("ETL Process failed during the extract phase.")
