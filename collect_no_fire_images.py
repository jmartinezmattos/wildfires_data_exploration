import os
import ee
import json
import datetime
import pandas as pd
from tqdm import tqdm
import random
import concurrent.futures

from collect_images import download_thumbnail

CONFIG_FILE = "config/collect_no_fire_images_config.json"

with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

GEE_PROJECT = config.get("GEE_PROJECT")
IMAGES_SATELLITE = config.get("IMAGES_SATELLITE", 'sentinel-2')
THUMB_SIZE = config.get("THUMB_SIZE", 256)
BUFFER_METERS = config.get("BUFFER_METERS")
OUTPUT_IMG_DIR = f"data/no_fire_images_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
OUTPUT_CSV = os.path.join(OUTPUT_IMG_DIR, "no_fire_images.csv")
NUM_THREADS = config.get("NUM_THREADS", 4)

os.makedirs(OUTPUT_IMG_DIR, exist_ok=True)

COLUMNS = ['latitude', 'longitude', 'image_date', 'thumbnail_file', 
           'satellite_image_source', 'country', 'firms_sensor']

def random_past_date_from_row(row_date_str):
    base_date = datetime.datetime.fromisoformat(row_date_str)
    months_back = random.randint(1, 13)
    days_back = random.randint(0, 30)
    return base_date - datetime.timedelta(days=30*months_back + days_back)

def get_ee_image(point, target_date):
    start_date = target_date - datetime.timedelta(days=7)
    end_date = target_date + datetime.timedelta(days=7)
    collection = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")\
        .filterBounds(point)\
        .filterDate(start_date, end_date)
    if collection.size().getInfo() == 0:
        print(f"No images found for point at date range [{start_date}, {end_date}] for point {point}")
        return None
    images_list = collection.toList(collection.size())
    closest_image = None
    min_diff = None
    for i in range(images_list.size().getInfo()):
        img = ee.Image(images_list.get(i))
        info = img.getInfo()
        if 'bands' not in info:
            continue
        bands = [b['id'] for b in info['bands']]
        required_bands = ['B4','B3','B2']
        if not all(b in bands for b in required_bands):
            continue
        img_date = datetime.datetime.utcfromtimestamp(info['properties']['system:time_start'] / 1000)
        diff = abs((img_date - target_date).total_seconds())
        if min_diff is None or diff < min_diff:
            min_diff = diff
            closest_image = img
    return closest_image

def process_row(idx, row):
    lat, lon = row['latitude'], row['longitude']
    point = ee.Geometry.Point(lon, lat)
    random_date = random_past_date_from_row(row['FIRMS_date'])
    image = get_ee_image(point, random_date)
    if image is None:
        print(f"No image found for point {idx} at random date {random_date}")
        return None
    
    original_name = row.get('thumbnail_file', f"point_{idx}.png")
    base_name, ext = os.path.splitext(original_name)
    country = row.get('country', 'unknown_country').replace(" ", "_")
    filename = os.path.join(OUTPUT_IMG_DIR, f"no_fire_{country}_{base_name}{ext}")

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        success = download_thumbnail(image, filename, point, 'sentinel-2', size=THUMB_SIZE)
        if success:
            break
        else:
            print(f"Attempt {attempt} failed for point {idx}, retrying...")

    if not success:
        print(f"Failed to download image for point {idx} after {max_retries} attempts.")
        return None

    result = {
        'latitude': lat,
        'longitude': lon,
        'image_date': random_date.isoformat(),
        'thumbnail_file': os.path.basename(filename),
        'satellite_image_source': IMAGES_SATELLITE,
        'country': row.get('country', None),
        'firms_sensor': row.get('firms_sensor', None)
    }
    pd.DataFrame([result]).to_csv(
        OUTPUT_CSV,
        mode='a',
        header=not os.path.exists(OUTPUT_CSV),
        index=False
    )
    return result


if __name__ == "__main__":

    ee.Initialize(project=GEE_PROJECT)

    df = pd.read_csv("data/fire/firms_features_merged.csv")

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        list(
            tqdm(
                executor.map(lambda args: process_row(*args), [(idx, row) for idx, row in df.iterrows()]),
                total=len(df)
            )
        )

    print(f"Download completed, images saved to {OUTPUT_CSV}")
