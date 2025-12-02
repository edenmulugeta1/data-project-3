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
       print("No data found in the JSON file.")
       return


   # Extracting important fields into a list of dictionaries
   events_list = []
   for event in raw_data:
       props = event.get("properties", {})
       geom = event.get("geometry", {})


       # Extract coordinates (lon, lat, depth)
       coords = geom.get("coordinates", [None, None, None])
       lon, lat, depth = coords if len(coords) == 3 else (None, None, None)


       events_list.append({
           "source_id": props.get("source_id"),
           "time": props.get("time"),          # ISO timestamp string
           "latitude": lat,
           "longitude": lon,
           "depth": depth,
           "magnitude": props.get("mag"),
           "region": props.get("flynn_region") or props.get("region")
       })


   # Creating DataFrame from list of dictionaries
   df = pd.DataFrame(events_list)


   # Converting time column to datetime (NO unit='ms')
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


   # Create / overwrite DuckDB table
   con = duckdb.connect(DB_FILE)
   con.register("events_view", df)


   con.execute("DROP TABLE IF EXISTS earthquakes")
   con.execute("""
       CREATE TABLE earthquakes AS
       SELECT * FROM events_view
   """)


   con.close()
   print(f"Updated DuckDB database: {DB_FILE}")


if __name__ == "__main__":
   process_earthquake_data()