# =======================
# CHANGES SUMMARY
# =======================
# 1. Disable EE concurrency: NUM_THREADS = 1
# 2. Remove ALL task polling / wait_for_task_slot / count_active_tasks
# 3. Add a global EE semaphore to protect EE calls
# 4. Add small jittered sleep before task.start()
# =======================

import os
import json
import datetime
import concurrent.futures
import time
import argparse
import random
import threading
from google.cloud import storage

import ee
import pandas as pd
from tqdm import tqdm


PROGRESS_PATH = "progress.json"
progress_lock = threading.Lock()

storage_client = storage.Client()
bucket = storage_client.bucket("fire_model_dataset")

def file_exists(path):
    blob = bucket.blob(path)
    print(f"File {path} exists: {blob.exists()}")
    return blob.exists()


def load_last_row():
    if not os.path.exists(PROGRESS_PATH):
        return 0
    with open(PROGRESS_PATH, "r") as f:
        data = json.load(f)
    return data.get("last_row", 0)


def save_last_row(idx):
    with progress_lock:
        with open(PROGRESS_PATH, "w") as f:
            json.dump({"last_row": idx}, f)



# -----------------------
# Config
# -----------------------
CONFIG_PATH = "config/collect_images_config.json"
CSV_PATH = "data/fire/firms_features_merged.csv"

NUM_THREADS = 1  # 🔥 IMPORTANT: EE-safe
MAX_PENDING_TASKS = 500
POLL_SECONDS = 20

if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
    GEE_PROJECT = config.get("GEE_PROJECT")

BANDS_TO_EXTRACT = [
    "B1", "B2", "B3", "B4", "B5", "B6",
    "B7", "B8", "B8A", "B9", "B11", "B12",
]

# Global EE rate limiter
ee_semaphore = threading.Semaphore(1)
pending_tasks = []
pending_lock = threading.Lock()


# -----------------------
# Export
# -----------------------
def _prune_finished_tasks():
    active_states = {"READY", "RUNNING"}
    still_pending = []
    for task in pending_tasks:
        try:
            state = task.status().get("state")
            if state in active_states:
                still_pending.append(task)
        except Exception:
            # If status fails, keep task to be safe
            still_pending.append(task)
    return still_pending


def wait_for_pending_slot(max_pending: int, poll_seconds: int):
    while True:
        with pending_lock:
            current_pending = len(pending_tasks)
            if current_pending < max_pending:
                return
        print(f"Pending tasks: {current_pending}. Waiting for a slot...", flush=True)
        #time.sleep(poll_seconds)
        with pending_lock:
            updated = _prune_finished_tasks()# already takes long, no sleep needed
            pending_tasks.clear()
            pending_tasks.extend(updated)


def download_band_placeholder(image, out_path, region):
    with ee_semaphore:
        proj = image.select("B2").projection()
        region_utm = region.transform(proj, 1)

        task = ee.batch.Export.image.toCloudStorage(
            image=image.clip(region_utm),
            bucket="fire_model_dataset",
            fileNamePrefix=out_path,
            region=region_utm,
            crs=proj.crs(),
            scale=10,
            fileFormat="GeoTIFF",
            formatOptions={"cloudOptimized": True},
            maxPixels=1e13,
        )

        task.start()
        time.sleep(1.2 + random.random() * 0.6)

        #with pending_lock:
            #pending_tasks.append(task)


# -----------------------
# Sentinel-2 fetch
# -----------------------
def get_s2_image(point, image_date):
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

    # Just take first image without checking size
    image = ee.Image(collection.first())

    return image


# -----------------------
# Row processing
# -----------------------
def process_row(idx, row, split_sets):
    lat = row["latitude"]
    lon = row["longitude"]
    image_date = row["image_date"]
    point_num = row["thumbnail_file"][:-4]
    country = row["country"]
    if country.lower() in ["uruguay", "new_zealand", "ireland", "france", "finland", "cuba"]:
        country = f"modis_{country}"

    # Check where it should go
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
    gcs_path = f"{out_path}.tif"

    # Check if is already in the bucket
    if file_exists(gcs_path):
        save_last_row(idx)  # Save progress even if skipping
        print(f"File exists, skipping index {idx}.")
        return

    print(f"Processing index {idx}: {base_name}")
    if pd.isna(image_date):
        return

    point = ee.Geometry.Point(lon, lat)
    region = point.buffer(2000)

    image = get_s2_image(point, image_date)
    if image is None:
        return

    download_band_placeholder(image, out_path, region)

      # Save progress AFTER successful submission

# -----------------------
# Main
# -----------------------
def main(start_line: int = 0):
    ee.Initialize(project=GEE_PROJECT)

    df = pd.read_csv(CSV_PATH)

    if start_line == 0:
        start_line = load_last_row()
        print(f"Resuming from row {start_line}")

    if start_line > 0:
        df = df.iloc[start_line:]

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-line", type=int, default=0)
    args = parser.parse_args()
    main(args.start_line)
