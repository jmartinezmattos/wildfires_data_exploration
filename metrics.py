import pandas as pd
from sklearn.cluster import DBSCAN
import numpy as np
import datetime
import os
import matplotlib.pyplot as plt
import geopandas as gpd
import requests
import zipfile
import io

OUTPUT_DIR = f"data/metrics/{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_world_map():
    """
    Downloads Natural Earth '110m admin 0 countries' if not present,
    extracts it into data/world/, and loads it as a GeoDataFrame.
    """

    world_dir = "data/world"
    shapefile_path = os.path.join(world_dir, "ne_110m_admin_0_countries.shp")

    if not os.path.exists(shapefile_path):
        print("Downloading Natural Earth world map...")

        os.makedirs(world_dir, exist_ok=True)

        # ✅ This is the correct working URL from Natural Earth CDN
        url = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"

        r = requests.get(url)

        # Check if response is ZIP
        if r.status_code != 200 or r.content[:2] != b'PK':
            raise ValueError("Download failed, file is not a valid ZIP. URL may be down.")

        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(world_dir)

        print("World map downloaded and extracted successfully.")

    return gpd.read_file(shapefile_path)


def save_world_fire_map(df, output_dir):
    """
    Creates and saves a world map with fire locations as red points.
    """
    os.makedirs(output_dir, exist_ok=True)

    world = load_world_map()

    fire_points = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    fig, ax = plt.subplots(figsize=(12, 8))
    world.plot(ax=ax, color="lightgray", edgecolor="black")
    fire_points.plot(ax=ax, markersize=4, color="red", alpha=0.6)

    plt.title("Global Fire Locations")
    output_path = os.path.join(output_dir, "world_fire_map.png")

    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"World fire map saved to: {output_path}")
    """
    Creates and saves a world map with fire locations as red points.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Load world map
    world = load_world_map()

    # Create GeoDataFrame from fire points
    fire_points = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    # Plot
    fig, ax = plt.subplots(figsize=(12, 8))
    world.plot(ax=ax, color="lightgray", edgecolor="black")
    fire_points.plot(ax=ax, markersize=5, color="red", alpha=0.6)

    plt.title("Global Fire Locations")
    output_path = os.path.join(output_dir, "world_fire_map.png")

    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"World fire map saved to: {output_path}")

def assign_fire_ids(df, max_km=3):
    df = df.copy()
    df["FIRMS_date"] = pd.to_datetime(df["FIRMS_date"])
    df["day"] = df["FIRMS_date"].dt.date

    kms_per_radian = 6371.0088
    epsilon = max_km / kms_per_radian

    df["fire_id"] = -1
    current_fire_id = 0

    for day, day_df in df.groupby("day"):
        coords = day_df[["latitude", "longitude"]].to_numpy()
        rad = np.radians(coords)

        clustering = DBSCAN(
            eps=epsilon,
            min_samples=1,
            metric="haversine"
        ).fit(rad)

        labels = clustering.labels_

        for cluster_label in set(labels):
            mask = (labels == cluster_label)
            df.loc[day_df.index[mask], "fire_id"] = current_fire_id
            current_fire_id += 1

    return df

def save_country_bar_chart(df, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    country_counts = df['country'].value_counts()
    plt.figure(figsize=(10,6))
    country_counts.plot(kind='bar', color='skyblue', edgecolor='black')
    plt.xlabel("Country")
    plt.ylabel("Number of Observations")
    plt.title("Observations per Country")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    output_path = os.path.join(output_dir, "country_bar_chart.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"Country bar chart saved to: {output_path}")

def save_cloud_pct_histogram(df, output_dir):
    """Create and save histogram of cloud_pct with 10 bins, x-axis ticks every 10,
    and vertical lines for mean and ±1 std."""
    os.makedirs(output_dir, exist_ok=True)
    
    cloud_pct = df['cloud_pct'].dropna()
    mean = cloud_pct.mean()
    std = cloud_pct.std()
    
    plt.figure()
    plt.hist(cloud_pct, bins=10, color='skyblue', edgecolor='black')
    plt.xlabel("Cloud Percentage")
    plt.ylabel("Frequency")
    plt.title("Histogram of cloud_pct")
    
    # Set x-axis ticks every 10
    plt.xticks(range(0, 101, 10))
    
    # Add vertical lines for mean and ±1 std
    plt.axvline(mean, color='red', linestyle='-', linewidth=2, label=f"Mean: {mean:.2f}")
    plt.axvline(mean - std, color='green', linestyle='--', linewidth=1.5, label=f"-1 Std: {mean - std:.2f}")
    plt.axvline(mean + std, color='green', linestyle='--', linewidth=1.5, label=f"+1 Std: {mean + std:.2f}")
    plt.legend()
    
    output_path = os.path.join(output_dir, "cloud_pct_histogram.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Histogram saved to: {output_path}")

def get_monthly_fire_counts(df, output_dir):
    """
    Compute fire counts per month across all years,
    save a bar chart, and return the counts as a Series.
    """
    df = df.copy()
    df['FIRMS_date'] = pd.to_datetime(df['FIRMS_date'])
    df['month'] = df['FIRMS_date'].dt.month

    # Group by month across all years
    monthly_counts = df.groupby('month').size().sort_index()  # ensures months 1-12 order

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Plot
    plt.figure(figsize=(8, 5))
    monthly_counts.plot(kind='bar', color='orange', edgecolor='black')
    plt.xlabel("Month")
    plt.ylabel("Number of Fires")
    plt.title("Number of Fires per Month (all years)")
    plt.xticks(ticks=range(0, 12), labels=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'], rotation=45)
    plt.tight_layout()

    # Save figure
    output_path = os.path.join(output_dir, "fires_per_month_all_years.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"Monthly fire counts plot saved to: {output_path}")

    return monthly_counts
    """
    Create and save a bar chart of number of fires per month.
    """
    # Ensure year and month columns exist
    if 'year' not in df.columns or 'month' not in df.columns:
        df['FIRMS_date'] = pd.to_datetime(df['FIRMS_date'])
        df['year'] = df['FIRMS_date'].dt.year
        df['month'] = df['FIRMS_date'].dt.month

    # Group by year and month
    monthly_counts = df.groupby(['year', 'month']).size()

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Plot
    plt.figure(figsize=(10, 6))
    monthly_counts.plot(kind='bar', color='orange', edgecolor='black')
    plt.xlabel("Year-Month")
    plt.ylabel("Number of Fires")
    plt.title("Number of Fires per Month")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    # Save figure
    output_path = os.path.join(output_dir, "fires_per_month.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"Monthly fire counts plot saved to: {output_path}")

def get_hourly_fire_counts(df, output_dir):
    """
    Compute fire counts per hour of day across all dates,
    save a bar chart, and return the counts as a Series.
    """
    df = df.copy()
    df['FIRMS_date'] = pd.to_datetime(df['FIRMS_date'])
    df['hour'] = df['FIRMS_date'].dt.hour   # 0–23

    # Count fires per hour (0 to 23)
    hourly_counts = df.groupby('hour').size().reindex(range(24), fill_value=0)

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Plot
    plt.figure(figsize=(10, 5))
    hourly_counts.plot(kind='bar', color='purple', edgecolor='black')
    plt.xlabel("Hour of Day")
    plt.ylabel("Number of Fires")
    plt.title("Number of Fires per Hour of Day (all dates)")
    plt.xticks(rotation=0)
    plt.tight_layout()

    # Save
    output_path = os.path.join(output_dir, "fires_per_hour.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"Hourly fire counts plot saved to: {output_path}")

    return hourly_counts

def get_metrics(df):
    countries = df['country'].value_counts()
    print(countries)

    country_counts = df['country'].value_counts().reset_index()
    country_counts.columns = ['country', 'count']
    country_counts.to_csv(os.path.join(OUTPUT_DIR, "country_counts.csv"), index=False)

    cloud_pct_mean = round(df['cloud_pct'].mean(), 3)
    cloud_pct_std = round(df['cloud_pct'].std(), 3)
    print(cloud_pct_mean, cloud_pct_std)

    df_with_ids = assign_fire_ids(df, max_km=3)
    unique_wildfires = df_with_ids["fire_id"].nunique()
    df_with_ids.to_csv(os.path.join(OUTPUT_DIR, "firms_with_fire_id.csv"), index=False)


    metrics = {
        "cloud_pct_mean": cloud_pct_mean,
        "cloud_pct_std": cloud_pct_std,
        "unique_wildfires": unique_wildfires
    }

    metrics_df = pd.DataFrame([
        {"metric": k, "value": v} for k, v in metrics.items()
    ])

    metrics_df.to_csv(os.path.join(OUTPUT_DIR, "metrics.csv"), index=False)


    save_world_fire_map(df, OUTPUT_DIR)
    save_cloud_pct_histogram(df, OUTPUT_DIR)
    save_country_bar_chart(df, OUTPUT_DIR)
    get_monthly_fire_counts(df, OUTPUT_DIR)
    get_hourly_fire_counts(df, OUTPUT_DIR)

if __name__ == "__main__":
    df = pd.read_csv("data/fire/firms_features_merged.csv")

    get_metrics(df)