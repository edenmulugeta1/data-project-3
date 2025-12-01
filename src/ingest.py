import requests
import json
import time
import os

# File to save raw JSON
RAW_FILE = "data/raw/earthquakes_2020_2025.json"
os.makedirs(os.path.dirname(RAW_FILE), exist_ok=True)

def fetch_earthquakes(start_year=2020, end_year=2025, max_per_request=20000):
    
    # Fetch earthquakes from SeismicPortal FDSN WS-EVENT API for given years
    
    all_events = []

    for year in range(start_year, end_year + 1):
        starttime = f"{year}-01-01T00:00:00"
        endtime = f"{year}-12-31T23:59:59"
        offset = 1

        print(f"\nFetching events for year {year}...")

        while True:
            params = {
                "starttime": starttime,
                "endtime": endtime,
                "format": "json",
                "limit": max_per_request,
                "offset": offset
            }

            try:
                response = requests.get(
                    "https://www.seismicportal.eu/fdsnws/event/1/query",
                    params=params
                )
                response.raise_for_status()
                data = response.json()
                events = data.get("features", [])

                if not events:
                    break

                all_events.extend(events)
                print(f"Fetched {len(events)} events (offset {offset})")
                offset += max_per_request

                time.sleep(0.5)  

            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                break
            except ValueError:
                print("Failed to decode JSON.")
                break

    with open(RAW_FILE, "w", encoding="utf-8") as f:
        json.dump(all_events, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(all_events)} earthquake events to {RAW_FILE}")


if __name__ == "__main__":
    fetch_earthquakes()
