import duckdb
import os
import pandas as pd

# Database path
DB_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "earthquakes_2020_2025.duckdb")

def analyze_earthquakes():
    con = duckdb.connect(DB_FILE)

    # 1) Strong earthquakes per year 
    print("=== Strong Earthquakes (Magnitude ≥ 5.0) by Year ===")
    strong_df = con.execute("""
        SELECT EXTRACT(YEAR FROM time) AS year,
               COUNT(*) AS strong_quakes
        FROM earthquakes
        WHERE magnitude >= 5.0
          AND EXTRACT(YEAR FROM time) >= 2020
        GROUP BY year
        ORDER BY year
    """).fetchdf()
    print(strong_df)
    print("\n- Check if counts rise, fall, or stay stable each year.\n")

    # 2) Top regions per year 
    print("=== Top 10 Regions Per Year ===")
    top_regions_df = con.execute("""
        SELECT year, region, quake_count FROM (
            SELECT EXTRACT(YEAR FROM time) AS year,
                   region,
                   COUNT(*) AS quake_count,
                   ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM time) ORDER BY COUNT(*) DESC) AS rank
            FROM earthquakes
            WHERE EXTRACT(YEAR FROM time) >= 2020
            GROUP BY year, region
        ) 
        WHERE rank <= 10
        ORDER BY year, quake_count DESC
    """).fetchdf()
    print(top_regions_df)

    # Count in how many years each region appears in the top 10
    print("\n=== Consistently Frequent Regions (2020-2025) ===")
    consistent_df = top_regions_df.groupby('region')['year'].nunique().reset_index(name='years_in_top10') \
                               .query('years_in_top10 > 1')  
    consistent_df = consistent_df.sort_values(['years_in_top10', 'region'], ascending=[False, True])
    print(consistent_df)


if __name__ == "__main__":
    analyze_earthquakes()

