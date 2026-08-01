import sys
import os
from pathlib import Path
import pandas as pd
import ee
import requests

TEST_PNG_DIR = "fire_test_png"
BANDS_TO_EXTRACT = ["B4", "B3", "B2", "B8", "B11", "B12"]
WINDOW_HOURS = 100

def load_env_file(path: str) -> dict:
    env_values = {}
    if not os.path.exists(path):
        return env_values
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_values[key.strip()] = value.strip().strip('"').strip("'")
    return env_values

def gee_initialization(project: str | None = None) -> None:
    env_values = load_env_file(Path("config/.env"))
    os.environ.update(env_values)

    gee_project = project or os.environ.get("GEE_PROJECT")
    try:
        ee.Initialize(project=gee_project)
    except Exception:
        ee.Initialize()

def gee_download_thumb_local(point, image, output_filename: str, width: int = 1024, height: int = 1024) -> bool:
    region = point.buffer(2000).bounds()

    thumb_params = {
        "region": region,
        "dimensions": f"{width}x{height}",
        "format": "png",
        "bands": ["B4", "B3", "B2"],
        "min": 0,
        "max": 3000,
        "gamma": 1.2,
    }

    try:
        url = image.clip(region).getThumbURL(thumb_params)
        response = requests.get(url, timeout=60)

        if response.status_code != 200:
            print(f"  [ERROR] HTTP {response.status_code}")
            return False

        if "image/png" not in response.headers.get("Content-Type", ""):
            print("[ERROR] Not a PNG response")
            return False

        if len(response.content) < 1000:
            print("[ERROR] File too small — likely invalid")
            return False

        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        with open(output_filename, "wb") as f:
            f.write(response.content)

        return True

    except Exception as e:
        print(f"  [ERROR] {e}")
        return False

def main():
    if len(sys.argv) != 2:
        print("Usage: python test_fire_download.py <name>")
        print("  <name>: name field from fires_minimalist_updated.csv (e.g., Bolivia_0)")
        sys.exit(1)

    target_name = sys.argv[1]

    # Load fire data
    df = pd.read_csv("final_data/fires_minimalist_updated.csv")
    df = df[df["name"].notna()]

    row = df[df["name"] == target_name]
    if len(row) == 0:
        print(f"Error: name '{target_name}' not found in fires_minimalist_updated.csv")
        sys.exit(1)

    row = row.iloc[0]
    lat = float(row["latitude"])
    lon = float(row["longitude"])
    target_dt = pd.to_datetime(row["image_date"])

    print(f"Testing fire download for: {target_name}")
    print(f"  Location: ({lat}, {lon})")
    print(f"  Fire date: {target_dt.isoformat()}")
    print(f"  Search window: ±{WINDOW_HOURS}h")
    print()

    # Initialize Earth Engine
    gee_initialization()

    # Query all images in the window
    start_dt = target_dt - pd.Timedelta(hours=WINDOW_HOURS)
    end_dt = target_dt + pd.Timedelta(hours=WINDOW_HOURS)

    point = ee.Geometry.Point([lon, lat])
    region = point.buffer(2000)

    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(point)
        .filterDate(ee.Date(start_dt.to_pydatetime()), ee.Date(end_dt.to_pydatetime()))
        #.filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 80))
        .select(BANDS_TO_EXTRACT)
        .sort("system:time_start")
    )

    count = collection.size().getInfo()
    print(f"Found {count} images in the window")
    print()

    if count == 0:
        print("No images found.")
        return

    image_list = collection.toList(count)

    # Download all images
    os.makedirs(TEST_PNG_DIR, exist_ok=True)

    print(f"{'Image #':<8} {'Image ID':<40} {'Date/Time':<30} {'Time Diff':<12} {'Status'}")
    print("-" * 100)

    for i in range(count):
        img = ee.Image(image_list.get(i))

        try:
            image_id = img.get("system:index").getInfo()
        except Exception:
            image_id = img.id().getInfo()

        try:
            image_timestamp = img.get("system:time_start").getInfo()
            image_dt = pd.Timestamp(image_timestamp, unit="ms")
        except Exception as e:
            print(f"{i:<8} {image_id:<40} {'ERROR':<30} {'N/A':<12} {str(e)[:40]}")
            continue

        # Calculate time difference from fire date
        time_diff = image_dt - target_dt
        diff_hours = time_diff.total_seconds() / 3600
        diff_str = f"{diff_hours:+.2f}h"

        # Download PNG
        output_filename = os.path.join(TEST_PNG_DIR, f"{target_name}_{i:02d}.png")
        success = gee_download_thumb_local(point, img, output_filename)

        status = "✓ Downloaded" if success else "✗ Failed"
        print(f"{i:<8} {image_id:<40} {image_dt.isoformat():<30} {diff_str:<12} {status}")

    print()
    print(f"PNGs saved to: {TEST_PNG_DIR}/")

if __name__ == "__main__":
    main()
