import requests
import pandas as pd
import sqlite3

# Extract Process
def extract_crypto_data():
    print("Starting crypto data extraction...")
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
        print(f"Extract success Retrieved {len(data)} coins.")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error during extraction: {e}")
        return None

# Transform Process
def transform_crypto_data(raw_data):
    print("Starting Transform Process...")
    if not raw_data:
        print("No data to transform.")
        return None

    df = pd.DataFrame(raw_data)
    columns_to_keep = ['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume', 'last_updated']
    df = df[columns_to_keep]
    df = df.dropna()

    df['last_updated'] = pd.to_datetime(df['last_updated'])
    print(f"Transform success. DataFrame shape: {df.shape[0]} rows, {df.shape[1]} columns.")
    return df

# Load Process
def load_crypto_data(df, db_name='crypto_data.db', table_name='crypto_prices'):
    print("Starting Load Process...")
    if df is None or df.empty:
        print("No data to load.")
        return False

    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Data loaded successfully to table '{table_name}' in database '{db_name}'.")

        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"Verified Found '{row_count}' rows in the database table.")

        conn.close()
        return True
    except Exception as e:
        print(f"Error during loading: {e}")
        return False

# Start Process Run
if __name__ == "__main__":
    print("Starting ETL Process...")

    # Extract: E
    raw_data = extract_crypto_data()
    print("-" * 32)

    if raw_data:
        # Transform: T
        transformed_data = transform_crypto_data(raw_data)
        print("-" * 32)

        if transformed_data is not None:
            # Load: L
            load_success = load_crypto_data(transformed_data)

    print("\nETL Process Completed.")
