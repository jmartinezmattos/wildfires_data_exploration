import os
import csv
import ee
import json
import datetime
import pandas as pd
from tqdm import tqdm
import threading
import random
import concurrent.futures

from util_download import download_thumbnail
from missing_filename_indexes import MISSING_FILENAME_INDEXES

CONFIG_FILE = "config/collect_no_fire_images_config.json"
NO_FIRE_DATETIME_PATH = "no_fire_datetimes.csv"
SORTED_NO_FIRE_DATETIME_PATH = "sorted_no_fire_datetetimes.csv"
time_lock = threading.Lock()
output_lock = threading.Lock()
reserved_filenames = set()
existing_sorted_filenames = set()
NUM_THREADS = 5

with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

def log_datetime_no_fire(idx: int, filename: str, latitude: float, longitude: float, datetime_str: str):
    if not filename:
        return
    print(f"Logging no-fire row: idx={idx}, filename={filename}, datetime={datetime_str}")
    with time_lock:
        file_exists = os.path.exists(NO_FIRE_DATETIME_PATH)
        with open(NO_FIRE_DATETIME_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["index", "filename", "latitude", "longitude", "datetime_str"])
            writer.writerow([idx, filename, latitude, longitude, datetime_str])

def load_existing_sorted_filenames(csv_path: str):
    if not os.path.exists(csv_path):
        return set()

    loaded = set()
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            filename = (row.get("filename") or "").strip()
            if filename:
                loaded.add(filename)
    print(f"Loaded {len(loaded)} filenames from {csv_path}")
    return loaded

GEE_PROJECT = config.get("GEE_PROJECT")
IMAGES_SATELLITE = config.get("IMAGES_SATELLITE", 'sentinel-2')
THUMB_SIZE = config.get("THUMB_SIZE", 256)
BUFFER_METERS = config.get("BUFFER_METERS")
OUTPUT_IMG_DIR = f"data/no_fire_images_new"
OUTPUT_CSV = os.path.join(OUTPUT_IMG_DIR, "no_fire_images.csv")
#NUM_THREADS = config.get("NUM_THREADS", 4)

os.makedirs(OUTPUT_IMG_DIR, exist_ok=True)

COLUMNS = ['latitude', 'longitude', 'image_date', 'thumbnail_file', 
           'satellite_image_source', 'country', 'firms_sensor']

def random_past_date_from_row(row_date_str, rng):
    base_date = datetime.datetime.fromisoformat(row_date_str)
    months_back = rng.randint(1, 48)
    days_back = rng.randint(0, 30)
    delta = datetime.timedelta(days=30 * months_back + 5 * days_back)  # para que sea todo multiplo de 5
    cutoff_date = datetime.datetime(2017, 7, 1)

    if base_date < cutoff_date:
        return base_date + delta

    return base_date - delta

def get_ee_image(idx, point, target_date):
    target_date = datetime.datetime.fromisoformat(target_date)
    start_date = target_date - datetime.timedelta(days=15)
    end_date = target_date + datetime.timedelta(days=15)
    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(point)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 60))  # optional cloud filter
        .sort('CLOUDY_PIXEL_PERCENTAGE')  # least cloudy first
        .select(["B4", "B3", "B2"])
    )

    if collection.size().getInfo() == 0:
        print(f"No images found for point {idx} at date range [{start_date}, {end_date}] for point {point}")
        return None

    least_cloudy_image = ee.Image(collection.first())

    return least_cloudy_image

def process_row(idx, row):
    print(f"Processing point {idx}")
    lat, lon = row['latitude'], row['longitude']
    point = ee.Geometry.Point(lon, lat)
    img_datetime_str = row['datetime_str']
    # Hay 380 que no se consiguió con rng = random.Random(idx), se cambia a idx+42
    #rng = random.Random(idx)
    # Attempt to get image in random dates until you get one
    #image = None
    #while image is None:
        #random_date = random_past_date_from_row(row['FIRMS_date'], rng)
    image = get_ee_image(idx, point, img_datetime_str)
    filename = ""
    result = None

    
    if image is None:
        print(f"No image found for point {idx} at random date {img_datetime_str}")
    else:
        original_name = row.get('thumbnail_file', f"point_{idx}.png")
        base_name, ext = os.path.splitext(original_name)
        country = row.get('country', 'unknown_country').replace(" ", "_")
        filename = os.path.join(OUTPUT_IMG_DIR, f"no_fire_{country}_{base_name}{ext}")
        output_basename = os.path.basename(filename)

        # Prevent duplicated downloads/writes when multiple rows map to the same file.
        with output_lock:
            if output_basename in existing_sorted_filenames:
                print(f"Skipping existing filename from sorted CSV for idx {idx}: {output_basename}")
                return None

            if output_basename in reserved_filenames:
                print(f"Skipping duplicated output filename for idx {idx}: {output_basename}")
                return None

            reserved_filenames.add(output_basename)

        max_retries = 3
        success = False
        for attempt in range(1, max_retries + 1):
            success = download_thumbnail(image, filename, point, IMAGES_SATELLITE, size=THUMB_SIZE)
            if success:
                break
            print(f"Attempt {attempt} failed for point {idx}, retrying...")

        if not success:
            print(f"Failed to download image for point {idx} after {max_retries} attempts.")
            with output_lock:
                reserved_filenames.discard(output_basename)
        else:
            result = {
                'latitude': lat,
                'longitude': lon,
                'image_date': img_datetime_str,
                'thumbnail_file': output_basename,
                'satellite_image_source': IMAGES_SATELLITE,
                'country': row.get('country', None),
                'firms_sensor': row.get('firms_sensor', None)
            }
            with output_lock:
                pd.DataFrame([result]).to_csv(
                    OUTPUT_CSV,
                    mode='a',
                    header=not os.path.exists(OUTPUT_CSV),
                    index=False
                )

    log_datetime_no_fire(
        idx=idx,
        filename=os.path.basename(filename) if filename else "",
        latitude=lat,
        longitude=lon,
        datetime_str=img_datetime_str
    )
    return result


if __name__ == "__main__":

    ee.Initialize(project=GEE_PROJECT)

    existing_sorted_filenames = load_existing_sorted_filenames(SORTED_NO_FIRE_DATETIME_PATH)

    df = pd.read_csv("sorted_no_fire_datetetimes.csv")
    start_index = 6720
    df = df[df.index >= start_index]
    print(f"Resuming no-fire image collection from row iwndex {start_index}. Rows to process: {len(df)}")
    #df_all = pd.read_csv("firms_features_merged_last.csv")
    #df = df_all[df_all.index.isin(MISSING_FILENAME_INDEXES)]
    print(f"USING {NUM_THREADS} threads for downloading images.")
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        list(
            tqdm(
                executor.map(lambda args: process_row(*args), [(idx, row) for idx, row in df.iterrows()]),
                total=len(df)
            )
        )

    print(f"Download completed, images saved to {OUTPUT_CSV}")
