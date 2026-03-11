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
import csv

import ee
import pandas as pd
from tqdm import tqdm


MISSING_SPLIT_PATH = "missing_split_prefix_fire.txt"
BUCKET = "fire_dataset_3"
TIME_DIFF_PATH = "time_differences_b10.csv"

PROGRESS_PATH = "progress.json"
BASE_NAMES_PATH = "already_in_bucket_fire.txt"
BUCKET_MISSING_PATH = "bucket_missing_files.txt"
missing_split_lock = threading.Lock()
time_diff_lock = threading.Lock()
bucket_lock = threading.Lock()
base_lock = threading.Lock()
counting_lock = threading.Lock()
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET)

IMAGES_SUBMITTED = 0

def file_exists(path):# fixme CAMBIAR DESPUÉS A QUE NO USE BUCKET
    blob = bucket.blob(path)
    print(f"File {path} exists: {blob.exists()}")
    return blob.exists()


# ---------------------
CSV_PATH = "firms_features_merged_last.csv"

NUM_THREADS = 6  # 🔥 IMPORTANT: EE-safe
MAX_PENDING_TASKS = 500
POLL_SECONDS = 20

BANDS_TO_EXTRACT = [
    "B2", "B3", "B4", "B8", "B11", "B12",
]

# Global EE rate limiter
ee_semaphore = threading.Semaphore(1)


# -----------------------
# Export
# -----------------------

def download_band_placeholder(image, out_path, region):
        with ee_semaphore:
            #proj = image.select("B2").projection()
            #region_utm = region.transform(proj, 1)

            task = ee.batch.Export.image.toCloudStorage(
                image=image.clip(region),
                bucket=BUCKET,
                fileNamePrefix=out_path,
                region=region,
                scale=10,
                fileFormat="GeoTIFF",
                formatOptions={"cloudOptimized": True},
                maxPixels=1e13,
            )

            task.start()
        #time.sleep(1.2 + random.random() * 0.6)

def log_missing_split(base_name: str):
    with missing_split_lock:
        with open(MISSING_SPLIT_PATH, "a") as f:
            f.write(f"{base_name}\n")

def log_base_name(base_name: str):
    with base_lock:
        with open(BASE_NAMES_PATH, "a") as f:
            f.write(f"{base_name}\n")
            f.flush()

def log_not_in_bucket(base_name: str):
    with bucket_lock:
        with open(BUCKET_MISSING_PATH, "a") as f:
            f.write(f"{base_name}\n")

def log_times(sentinel_img, landsat_img, diff_seconds, skip=False):

    with time_diff_lock:

        if skip:
            row = [0, 0, 0]
        else:
            sentinel_time = sentinel_img.get("system:time_start").getInfo()
            landsat_time = landsat_img.get("system:time_start").getInfo()

            sentinel_time = datetime.datetime.fromtimestamp(sentinel_time / 1000)
            landsat_time = datetime.datetime.fromtimestamp(landsat_time / 1000)

            row = [
                sentinel_time.isoformat(),
                landsat_time.isoformat(),
                diff_seconds
            ]

        with open(TIME_DIFF_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row)
# Sentinel-2 fetch
# -----------------------
def get_s2_image(point, image_date):
    
    #point = ee.Geometry.Point(lon, lat)
    #region = point.buffer(2000).bounds()
    
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

    sentinel_time = ee.Date(image.get("system:time_start"))

    landsat = (
    ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
    .merge(ee.ImageCollection("LANDSAT/LC09/C02/T1_L2"))
    .filterBounds(point)
    .filterDate(start, end)
    .select("ST_B10")
    )

    if landsat.size().getInfo() == 0:
        log_times(image, ee.Image(),None, True)
        return
    
    def add_time_diff(img):
        diff = ee.Number(img.date().difference(sentinel_time, "second")).abs()
        return img.set("time_diff", diff)

    landsat = landsat.map(add_time_diff)

    landsat_closest = ee.Image(
    landsat.sort("time_diff").first()
    )

    diff_seconds = landsat_closest.get("time_diff").getInfo()
    log_times(image, landsat_closest, diff_seconds)

    thermal = (
    landsat_closest
    .select("ST_B10")
    .multiply(0.00341802)
    .add(149.0)
    .rename("thermal")
    )
    
    thermal = thermal.reproject(
    crs=image.projection(),
    scale=30
)


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
    if country.lower() in ["uruguay", "new_zealand","ireland", "france", "finland", "cuba"]:
        country_aux = f"modis_{country}"
    else:
        country_aux = country

    # Check where it should go
    base_name = f"fire_{country_aux}_{point_num}.png"
    print(f"Processing row {idx}: {base_name}")
    split_prefix = None
    for split_name, name_set in split_sets.items():
        if base_name in name_set:
            split_prefix = split_name
            break
    
    """     if split_prefix is None:
        #print(f"File name not found in split lists: {base_name}")
        log_missing_split(base_name)
        print(f"No split for file {base_name}")
        return """

    out_path = f"{split_prefix}/Fire/{country}_{point_num}"
    gcs_path = f"{out_path}.tif"


    # Check if is already in the bucket
    #if file_exists(gcs_path):
          #Log missing split for files that exist but aren't in the split lists
        #log_base_name(base_name)
        #print(f"File exists, skipping index {idx}.")
        #return
    #log_not_in_bucket
    print(f"Processing index {idx}: {base_name}")
    #log_not_in_bucket(base_name)
    if pd.isna(image_date):
        return
    
    point = ee.Geometry.Point(lon, lat)
    region = point.buffer(2000).bounds()

    image = get_s2_image(point, image_date)
    if image is None:
        return
    return ## SACAR ESTO DESPUÉS
    download_band_placeholder(image, out_path, region)
    with counting_lock:
        global IMAGES_SUBMITTED
        IMAGES_SUBMITTED += 1

      # Save progress AFTER successful submission

# -----------------------
# Main
# -----------------------
def main():
    ee.Initialize(project="fire-detection-uruguay")

    df = pd.read_csv(CSV_PATH)

    split_sets = {
        "train": set(pd.read_csv("train_Fire.csv")["filename"]),
        "val": set(pd.read_csv("val_Fire.csv")["filename"]),
        "test": set(pd.read_csv("test_Fire.csv")["filename"]),
    }
    args_list = [(idx, row, split_sets) for idx, row in df.iterrows()]

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        list(
            tqdm(
                executor.map(
                    lambda args: process_row(*args),
                    args_list,
                ),
                total=len(df),
            )
        )


if __name__ == "__main__":
    main()
    if IMAGES_SUBMITTED == 0:
        print("No new images were found to download. Signal Bash to stop.")
        sys.exit(10) # Tell Bash to break the loop
    else:
        print("Tasks submitted. Bash will wait and restart.")
        sys.exit(0) # Standard exit, Bash will loop again