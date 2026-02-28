# =======================
# CHANGES SUMMARY
# =======================
# 1. Disable EE concurrency: NUM_THREADS = 1
# 2. Remove ALL task polling / wait_for_task_slot / count_active_tasks
# 3. Add a global EE semaphore to protect EE calls
# 4. Add small jittered sleep before task.start()
# =======================

import os
import sys
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

MISSING_SPLIT_PATH = "missing_split_prefix.txt"
PROGRESS_PATH = "progress.json"
TASKS_PATH = "tasks.txt"
progress_lock = threading.Lock()
counting_lock = threading.Lock()
missing_split_lock = threading.Lock()
BUCKET = "fire_dataset_2"

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET)
IMAGES_SUBMITTED = 0

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


def random_past_date_from_row(row_date_str):
    base_date = datetime.datetime.fromisoformat(row_date_str)
    months_back = random.randint(1, 13)
    days_back = random.randint(0, 30)
    return base_date - datetime.timedelta(days=30*months_back +5*days_back)#para que sea todo multiplo de 5

# -----------------------
# Config
# -----------------------
CONFIG_PATH = "collect_images_config.json"
CSV_PATH = "firms_features_merged_last.csv"
BUCKET_MISSING_PATH = "missing_in_bucket_no_fire.txt"
BASE_PATH = "all_no_fire_in_bucket.txt"

NUM_THREADS = 8  # 🔥 IMPORTANT: EE-safe
MAX_PENDING_TASKS = 500
POLL_SECONDS = 20

if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
    GEE_PROJECT = config.get("GEE_PROJECT")

BANDS_TO_EXTRACT = [
    "B2", "B3", "B4","B8", "B11", "B12",
]

# Global EE rate limiter
ee_semaphore = threading.Semaphore(1)
pending_tasks = []
pending_lock = threading.Lock()
bucket_lock = threading.Lock()
names_lock = threading.Lock()
task_lock = threading.Lock()

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
            bucket=BUCKET,
            fileNamePrefix=out_path,
            region=region_utm,
            crs=proj.crs(),
            scale=10,
            fileFormat="GeoTIFF",
            formatOptions={"cloudOptimized": True},
            maxPixels=1e13,
        )

        task.start()#
        #time.sleep(1.2 + random.random() * 0.6)#



def log_missing_split(base_name: str):
    with missing_split_lock:
        with open(MISSING_SPLIT_PATH, "a") as f:
            f.write(f"{base_name}\n")
            
def log_not_in_bucket(base_name: str):
    with bucket_lock:
        with open(BUCKET_MISSING_PATH, "a") as f:
            f.write(f"{base_name}\n")

def already_in_bucket(base_name: str):
    with names_lock:
        with open(BASE_PATH, "a") as f:
            f.write(f"{base_name}\n")



# -----------------------
# Sentinel-2 fetch
# -----------------------
def get_s2_image(point, image_date):
    if isinstance(image_date, str):
        dt = datetime.datetime.fromisoformat(image_date.replace("Z", ""))
    else:
        dt = image_date
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
    image_date = random_past_date_from_row(row["image_date"])
    point_num = row["thumbnail_file"][:-4]
    country = row["country"]
    #if country.lower() in ["new zealand"]:
     #   country = "Zealand"
    #    country = f"modis_{country}"

    # Check where it should go
    base_name = f"no_fire_{country}_{point_num}.png"
    #log_missing_split(base_name)  # Log every file name for later analysis of missing splits    
    split_prefix = None
    for split_name, name_set in split_sets.items():
        if base_name in name_set:
            split_prefix = split_name
            break
    
    if split_prefix is None:
        log_missing_split(base_name)
        print(f"File name not found in split lists: {base_name}")
        return

    out_path = f"{split_prefix}/No_Fire/{country}_{point_num}"
    gcs_path = f"{out_path}.tif"

    # Check if is already in the bucket
    if file_exists(gcs_path):
       already_in_bucket(base_name)
       save_last_row(idx)  # Save progress even if skipping
       return

    print(f"Processing {idx}: {base_name}")
    log_not_in_bucket(base_name)
    if pd.isna(random):
        return
    
    point = ee.Geometry.Point(lon, lat)
    region = point.buffer(2000)

    image = get_s2_image(point, image_date)
    if image is None:
        return

    download_band_placeholder(image, out_path, region)
    with counting_lock:
        global IMAGES_SUBMITTED
        IMAGES_SUBMITTED += 1
    #save_last_row(idx)  # Save progress even if skipping

      # Save progress AFTER successful submission

# -----------------------
# Main
# -----------------------
def main(start_line: int = 0):
    #ee.Initialize(project="wildfires-479718")
    ee.Initialize(project="fire-detection-uruguay")

    df = pd.read_csv(CSV_PATH)

    if start_line == 0:
        start_line = load_last_row()
        print(f"Resuming from row {start_line}")

    if start_line > 0:
        df = df.iloc[start_line:]

    split_sets = {
        "train": set(pd.read_csv("train_No_Fire.csv")["filename"]),
        "val": set(pd.read_csv("val_No_Fire.csv")["filename"]),
        "test": set(pd.read_csv("test_No_Fire.csv")["filename"]),
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        args_list = [(idx, row, split_sets) for idx, row in df.iterrows()]
        list(
            tqdm(
                executor.map(
                    lambda args: process_row(*args),
                    args_list
                ),
                total=len(df),
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-line", type=int, default=0)
    args = parser.parse_args()
    main(args.start_line)
    if IMAGES_SUBMITTED == 0:
        print("No new images were found to download. Signal Bash to stop.")
        sys.exit(10) # Tell Bash to break the loop
    else:
        print("Tasks submitted. Bash will wait and restart.")
        sys.exit(0) # Standard exit, Bash will loop again