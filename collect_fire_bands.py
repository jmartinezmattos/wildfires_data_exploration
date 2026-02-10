import os
import json
import datetime
import concurrent.futures
import time
import argparse

import ee
import pandas as pd
from tqdm import tqdm


# Config
CONFIG_PATH = "config/collect_images_config.json"
CSV_PATH = "data/fire/firms_features_merged.csv"
NUM_THREADS = 100
MAX_ACTIVE_TASKS = 2500
POLL_SECONDS = 15

# Optional: reuse GEE project from config
GEE_PROJECT = None
if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
    GEE_PROJECT = config.get("GEE_PROJECT")
    NUM_THREADS = config.get("NUM_THREADS", NUM_THREADS)

# Placeholder: set the band(s) you want to extract
BANDS_TO_EXTRACT = [
    "B1"
    "B2",
    "B3",
    "B4",
    "B5",
    "B6",
    "B7",
    "B8",
    "B8A",
    "B9",
    "B10"
    "B11",
    "B12",
] 

# Placeholder: implement how to export or download the band(s)

def download_band_placeholder(image, out_path, region):
    """
    Placeholder for band extraction. Replace this with your actual
    export/download logic (e.g., getDownloadURL or Export.image).
    """
    wait_for_task_slot(MAX_ACTIVE_TASKS, POLL_SECONDS)
    task = ee.batch.Export.image.toCloudStorage(
    image=image,
    bucket="fire_model_dataset",
    fileNamePrefix=out_path,
    region=region,
    crs="EPSG:4326",
    fileFormat="GeoTIFF",
    formatOptions={'cloudOptimized': True},
    maxPixels=1e13
    )

    print(f"Starting export to {out_path}", flush=True)
    task.start()


def count_active_tasks():
    active_states = {"READY", "RUNNING"}
    active = 0
    for task in ee.batch.Task.list():
        try:
            if task.status().get("state") in active_states:
                active += 1
        except Exception:
            continue
    return active


def wait_for_task_slot(max_active, poll_seconds):
    while True:
        active = count_active_tasks()
        if active < max_active:
            return
        print(f"Active tasks {active} >= limit {max_active}. Waiting {poll_seconds}s...", flush=True)
        time.sleep(poll_seconds)


def get_s2_image(point, image_date):
    """
    Find the S2_SR_HARMONIZED image for the given point and image_date.
    Uses a 1-day window around the image_date.
    """
    # image_date in ISO format: 2025-11-08T16:17:37
    dt = datetime.datetime.fromisoformat(image_date.replace("Z", ""))
    start = dt - datetime.timedelta(hours=12)
    end = dt + datetime.timedelta(hours=12)

    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(point)
        .filterDate(start, end)
        .select(BANDS_TO_EXTRACT)
        .sort("system:time_start")
    )

    if collection.size().eq(0).getInfo():
        return None

    return ee.Image(collection.first())


def process_row(idx, row, split_sets):
    lat = row["latitude"]
    lon = row["longitude"]
    image_date = row["image_date"]
    point_num = row["thumbnail_file"][:-4]
    country = row["country"]
    if country.lower() in ["uruguay", "new_zealand", "ireland", "france", "finland", "cuba"]:
        country = f"modis_{country}"

    if pd.isna(image_date):
        return

    point = ee.Geometry.Point(lon, lat)
    region = point.buffer(2000)
    image = get_s2_image(point, image_date)

    if image is None:
        print(f"No image found for index {idx}")
        return

    base_name = f"fire_{country}_{point_num}.png"
    split_prefix = None
    for split_name, name_set in split_sets.items():
        if base_name in name_set:
            split_prefix = split_name
            break

    if split_prefix is None:
        print(f"File name not found in split lists: {base_name}")
        return

    out_path = f"{split_prefix}/Fire/{country}_{point_num}"

    try:
        download_band_placeholder(image, out_path, region)
    except NotImplementedError:
        # Keep the placeholder without failing the whole run
        pass


def main(start_line: int = 0):
    ee.Initialize(project=GEE_PROJECT)

    if start_line < 0:
        raise ValueError("start_line must be >= 0")

    if start_line >= 1:
        df = pd.read_csv(CSV_PATH)
    else:
        df = pd.read_csv(CSV_PATH, skiprows=range(1, start_line))

    split_sets = {
        "train": set(pd.read_csv("file_names/train_Fire.csv")["filename"]),
        "val": set(pd.read_csv("file_names/val_Fire.csv")["filename"]),
        "test": set(pd.read_csv("file_names/test_Fire.csv")["filename"]),
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        list(
            tqdm(
                executor.map(
                    lambda args: process_row(*args),
                    [(idx, row, split_sets) for idx, row in df.iterrows()],
                ),
                total=len(df),
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect fire bands from CSV.")
    parser.add_argument(
        "--start-line",
        type=int,
        default=0,
        help="Data row number to start reading from (1-based). Use 0 or 1 for the first data row.",
    )
    args = parser.parse_args()
    main(start_line=args.start_line)
