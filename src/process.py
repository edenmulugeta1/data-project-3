import pandas as pd
import json
import duckdb
import os

# Paths for files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  
RAW_JSON = os.path.join(BASE_DIR, "..", "data", "raw", "earthquakes_2020_2025.json")
PARQUET_FILE = os.path.join(BASE_DIR, "..", "data", "processed", "earthquakes_2020_2025.parquet")
DB_FILE = os.path.join(BASE_DIR, "..", "data", "processed", "earthquakes_2020_2025.duckdb")

os.makedirs(os.path.dirname(PARQUET_FILE), exist_ok=True)

# Check database for 2019 data
con = duckdb.connect(DB_FILE)
try:
    result = con.execute("SELECT MIN(EXTRACT(YEAR FROM time)) AS earliest_year FROM earthquakes").fetchone()
    print("Earliest year in the database:", result[0])

    count_2019 = con.execute("SELECT COUNT(*) FROM earthquakes WHERE EXTRACT(YEAR FROM time) = 2019").fetchone()[0]

    # Remove 2019 data if it exists
    if count_2019 > 0:
        con.execute("DELETE FROM earthquakes WHERE EXTRACT(YEAR FROM time) = 2019")
finally:
    con.close()

def process_earthquake_data():
    # Loading raw JSON data
    with open(RAW_JSON, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    if not raw_data:
        print("No data found in the JSON file.")
        return

    # Extracting important fields into a list of dictionaries
    events_list = []
    for event in raw_data:
        props = event.get("properties", {})
        events_list.append({
            "source_id": props.get("source_id"),
            "time": props.get("Time") or props.get("time"),
            "latitude": props.get("Lat") or props.get("lat"),
            "longitude": props.get("Lon") or props.get("lon"),
            "depth": props.get("Depth") or props.get("depth"),
            "magnitude": props.get("Mag") or props.get("mag"),
            "region": props.get("flynn_region") or props.get("Flynn_region") or props.get("region")
        })

    # Creating DataFrame from list of dictionaries
    df = pd.DataFrame(events_list)

    # Converting time column to datetime
    df['time'] = pd.to_datetime(df['time'], errors='coerce')

    # Removing rows with missing important values
    df = df.dropna(subset=['latitude', 'longitude', 'magnitude', 'time'])

    # Filtering out any years outside of 2020-2025 just in case
    df = df[(df['time'] >= '2020-01-01') & (df['time'] < '2026-01-01')]

    # Info about the data after cleaning
    print(f"Total events after cleaning: {len(df)}")
    print(df.head())
    print(df.describe())

    # Saving cleaned data as Parquet with compression
    df.to_parquet(PARQUET_FILE, index=False, compression="snappy")
    print(f"Saved Parquet file: {PARQUET_FILE}")

    # Append new data to DuckDB
    con = duckdb.connect(DB_FILE)
    con.register("events_view", df)
    con.execute("""
        CREATE TABLE IF NOT EXISTS earthquakes AS 
        SELECT * FROM events_view
    """)
    # Insert new cleaned data
    con.execute("INSERT INTO earthquakes SELECT * FROM events_view")
    con.close()
    print(f"Updated DuckDB database: {DB_FILE}")

if __name__ == "__main__":
    process_earthquake_data()