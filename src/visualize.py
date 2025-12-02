import duckdb
import os
import matplotlib.pyplot as plt

# Database path
DB_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "earthquakes_2020_2025.duckdb")

def generate_combined_visualization():
    con = duckdb.connect(DB_FILE)

    # strong earthquakes per year
    strong_df = con.execute("""
        SELECT 
            EXTRACT(YEAR FROM time) AS year,
            COUNT(*) AS strong_quakes
        FROM earthquakes
        WHERE magnitude >= 5.0
          AND EXTRACT(YEAR FROM time) BETWEEN 2020 AND 2025
        GROUP BY year
        ORDER BY year;
    """).fetchdf()

    # top regions per year
    top_regions_df = con.execute("""
        SELECT year, region, quake_count FROM (
            SELECT 
                EXTRACT(YEAR FROM time) AS year,
                COALESCE(region, 'UNKNOWN') AS region,
                COUNT(*) AS quake_count,
                ROW_NUMBER() OVER (
                    PARTITION BY EXTRACT(YEAR FROM time)
                    ORDER BY COUNT(*) DESC
                ) AS rank
            FROM earthquakes
            WHERE EXTRACT(YEAR FROM time) BETWEEN 2020 AND 2025
            GROUP BY year, region
        )
        WHERE rank <= 10
        ORDER BY year, quake_count DESC;
    """).fetchdf()

    # consistent regions across years
    consistent_df = (
        top_regions_df.groupby("region")["year"]
        .nunique()
        .reset_index(name="years_in_top10")
        .query("years_in_top10 > 1")
        .sort_values("years_in_top10", ascending=False)
    )

    # main 2-panel figure
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # line chart (strong earthquakes)
    axes[0].plot(strong_df["year"], strong_df["strong_quakes"], marker="o")
    axes[0].set_title("Strong Earthquakes (Magnitude ≥ 5.0) per Year")
    axes[0].set_xlabel("Year")
    axes[0].set_ylabel("Number of Strong Earthquakes")
    axes[0].grid(True, linestyle="--", alpha=0.4)

    # bar chart (consistent regions)
    axes[1].barh(
        consistent_df["region"],
        consistent_df["years_in_top10"]
    )
    axes[1].set_title("Regions Consistently in Top 10 (2020–2025)")
    axes[1].set_xlabel("Years in Top 10")
    axes[1].invert_yaxis()

    plt.tight_layout()
    plt.show()

    con.close()


def plot_seasonal_frequency():
    con = duckdb.connect(DB_FILE)

    # earthquake count by season
    seasonal_df = con.execute("""
        SELECT 
            CASE 
                WHEN EXTRACT(MONTH FROM time) IN (12, 1, 2) THEN 'Winter'
                WHEN EXTRACT(MONTH FROM time) IN (3, 4, 5) THEN 'Spring'
                WHEN EXTRACT(MONTH FROM time) IN (6, 7, 8) THEN 'Summer'
                ELSE 'Fall'
            END AS season,
            COUNT(*) AS quake_count
        FROM earthquakes
        WHERE EXTRACT(YEAR FROM time) BETWEEN 2020 AND 2025
        GROUP BY season
        ORDER BY quake_count DESC;
    """).fetchdf()

    con.close()

    # bar chart
    plt.figure(figsize=(8, 6))
    plt.bar(seasonal_df["season"], seasonal_df["quake_count"])
    plt.title("Earthquake Frequency by Season (2020–2025)")
    plt.xlabel("Season")
    plt.ylabel("Number of Earthquakes")
    plt.grid(axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # first: main 2-panel visualization
    generate_combined_visualization()

    # second: standalone seasonal plot
    plot_seasonal_frequency()
