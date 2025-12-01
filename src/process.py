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

def process_earthquake_data():
    # Loading raw JSON data
    with open(RAW_JSON, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    if not raw_data:
        print("There is no data found in the JSON file.")
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
            "region": props.get("Flynn_region") or props.get("region")
        })

    # Creating DataFrame from list of dictionaries
    df = pd.DataFrame(events_list)

    # Converting time column to datetime
    df['time'] = pd.to_datetime(df['time'], errors='coerce')

    # Removing rows with missing important values
    df = df.dropna(subset=['latitude', 'longitude', 'magnitude', 'time'])

    # info about the data after cleaning
    print(f"Total events (after cleaning): {len(df)}")
    print(df.head())
    print(df.describe())

    # Saving cleaned data as Parquet with compression
    df.to_parquet(PARQUET_FILE, index=False, compression="snappy")
    print(f"Saved Parquet file: {PARQUET_FILE}")

    # Save data to DuckDB
    con = duckdb.connect(DB_FILE)
    con.register("events_view", df)
    con.execute("CREATE OR REPLACE TABLE earthquakes AS SELECT * FROM events_view")
    con.close()
    print(f"Saved DuckDB: {DB_FILE}")

if __name__ == "__main__":
    process_earthquake_data()