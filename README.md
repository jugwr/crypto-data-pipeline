# Crypto Market Data Pipeline (PySpark ETL)

## Project Overview
This project is an end-to-end Data Pipeline (ETL) designed to extract, transform, and load cryptocurrency market data. Built with **Apache Spark (PySpark)** and **Python**, it demonstrates the ability to handle data extraction from a public API, perform data cleansing and transformation, and securely load the structured data into a relational database.



## Architecture & Workflow
The pipeline follows a standard ETL (Extract, Transform, Load) process:

1. **Extract**: Retrieves real-time cryptocurrency data (Top 10 by Market Cap) using the free CoinGecko API.
2. **Transform**: Utilizes **PySpark** DataFrames to process the raw JSON data.
   - Filters necessary columns (ID, Symbol, Current Price, Market Cap, Volume, Last Updated).
   - Handles missing values (Null/NaN dropping).
   - Standardizes data types (casting strings to timestamps).
3. **Load**: Safely loads the cleaned, structured data into a **SQLite** database for easy querying and downstream analysis.

## Tech Stack
* **Language:** Python 3.x
* **Big Data Processing:** Apache Spark (PySpark)
* **Data Manipulation:** Pandas (for intermediate bridging)
* **Database:** SQLite
* **API:** CoinGecko API

## How to Run (Google Colab)
You can test and run this pipeline directly in your browser without any local setup:
1. Open the notebook via Google Colab: [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/) *(Note: Replace this link with your actual Colab notebook link)*
2. Run the cells sequentially.
3. The final output will generate a `crypto_pyspark.db` file containing the cleaned data.

## Next Steps & Use Cases
* **Data Visualization**: Connect the resulting SQLite database to business intelligence tools like **Looker** or **Power BI** to create interactive dashboards tracking crypto trends.
* **Orchestration**: Implement **Apache Airflow** to schedule and automate this pipeline to run daily.
* **Cloud Storage**: Transition the storage layer from a local SQLite database to a Data Lake (saving as `.parquet` files) or a Cloud Data Warehouse (e.g., Google BigQuery).
