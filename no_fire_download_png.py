from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import os
import pandas as pd
import random
from pathlib import Path
import ee
import requests

PNG_DOWNLOAD_DIR = "no_fire_downloads_png_v2"
NUM_WORKERS = 8
OUTPUT_CSV = os.path.join("final_data", "downloaded_no_fire_images_v2.csv")
DAYS_AHEAD = 100
BANDS_TO_EXTRACT = ["B4", "B3", "B2", "B8", "B11", "B12"]

### FUNCTIONS ###
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
            print(f"[ERROR] HTTP {response.status_code}")
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
        print(f"[ERROR] {e}")
        return False

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

# Example workflow
def gee_initialization(project: str | None = None) -> None:
    env_values = load_env_file(Path("config/.env"))
    os.environ.update(env_values)

    gee_project = project or os.environ.get("GEE_PROJECT")
    try:
        ee.Initialize(project=gee_project)
    except Exception:
        ee.Initialize()

def read_row_no_fire_alt(df: pd.DataFrame, index: int) -> dict | None:
    if index < 0 or index >= len(df):
        return None

    row = df.iloc[index]
    target_dt = pd.to_datetime(row["image_date"])

    return {
        "lat": float(row["latitude"]),
        "lon": float(row["longitude"]),
        "image_date": target_dt,
        "name": str(row["name"]).strip()
    }


def gee_get_image(lat: float, lon: float, target_dt: pd.Timestamp, no_fire=False, window_days: int = 7):

    start_dt = target_dt - pd.Timedelta(hours=10)
    end_dt = target_dt + pd.Timedelta(hours=10)

    if no_fire:
        start_dt = target_dt - pd.Timedelta(days=window_days)
        end_dt = target_dt + pd.Timedelta(days=window_days)

    point = ee.Geometry.Point([lon, lat])
    region = point.buffer(2000)

    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(point)
        .filterDate(ee.Date(start_dt.to_pydatetime()), ee.Date(end_dt.to_pydatetime()))
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 80))
        .select(BANDS_TO_EXTRACT)
        .sort("system:time_start")
    )

    count = collection.size().getInfo()
    if count == 0:
        return None, None

    image_list = collection.toList(count)
    target_ms = int(target_dt.timestamp() * 1000)
    best_image = None
    best_delta = None

    for i in range(count):
        img = ee.Image(image_list.get(i))
        img_time = img.get("system:time_start").getInfo()
        delta = abs(img_time - target_ms)
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_image = img

    return best_image, region

### END FUNCTIONS ###

# Initialize Earth Engine
gee_initialization()

os.makedirs(PNG_DOWNLOAD_DIR, exist_ok=True)

csv_path = "final_data/fires_minimalist_updated.csv"
df = pd.read_csv(csv_path)
df = df[df["name"].notna()].reset_index(drop=True)

csv_columns = [
    "latitude",
    "longitude",
    "image_date",
    "image_id",
    "name",
]

if not os.path.exists(OUTPUT_CSV):
    pd.DataFrame(columns=csv_columns).to_csv(OUTPUT_CSV, index=False)

csv_lock = Lock()

def get_offset_target_dt(target_dt: pd.Timestamp, tried_offsets: set, rng: random.Random, max_n: int = 60):
    available = [n for n in range(1, max_n + 1) if n not in tried_offsets]
    if not available:
        return None, None

    n = rng.choice(available)
    tried_offsets.add(n)

    offset_days = 5 * n
    new_target_dt = target_dt - pd.Timedelta(days=offset_days)
    if new_target_dt < pd.Timestamp("2016-01-01"):
        #print(f"New target date {new_target_dt} Close to Sentinel beginning - chosing date ahead.")
        initial_forward_offset = target_dt + pd.Timedelta(days=DAYS_AHEAD) # add days if too close
        new_target_dt = initial_forward_offset + pd.Timedelta(days=offset_days)
    return new_target_dt, offset_days

def gee_get_image_with_backoff(lat: float, lon: float, target_dt: pd.Timestamp, rng: random.Random, max_retries: int = 60):
    tried_offsets = set()
    window_days = 7

    for attempt in range(1, max_retries + 1):
        new_target_dt, offset_days = get_offset_target_dt(target_dt, tried_offsets, rng, max_n=max_retries)

        if new_target_dt is None:
            # All offsets tried, expand the window and retry
            window_days += 7
            if window_days > 60:
                return None, None
            tried_offsets.clear()
            new_target_dt, offset_days = get_offset_target_dt(target_dt, tried_offsets, rng, max_n=max_retries)

        try:
            best_image, region = gee_get_image(lat, lon, new_target_dt, no_fire=True, window_days=window_days)

            if best_image is not None:
                return best_image, region
        except Exception as e:
            print(f"[Attempt {attempt}] Error querying GEE: {e}, retrying...")
            continue

    return None, None

def process_no_fire_row(idx: int):
    alert = read_row_no_fire_alt(df, idx)
    if alert is None:
        return None

    # Seeded by row index so the PNG and TIFF scripts draw the exact same
    # sequence of random offsets for a given row, regardless of thread
    # scheduling or which script runs first.
    rng = random.Random(idx)

    max_attempts = 3
    best_image = None
    attempt = 0

    while best_image is None and attempt < max_attempts:
        attempt += 1
        try:
            best_image, _ = gee_get_image_with_backoff(alert["lat"], alert["lon"], alert["image_date"], rng)
        except Exception as e:
            print(f"[Row {idx}] Attempt {attempt} failed with error: {e}")
            if attempt < max_attempts:
                continue
            else:
                print(f"[Row {idx}] All retry attempts exhausted, no image found")
                return None

    if best_image is None:
        print(f"[Row {idx}] ({alert['name']}) No suitable image found after {max_attempts} attempts")
        return None

    try:
        image_id = best_image.get("system:index").getInfo()
    except Exception:
        image_id = best_image.id().getInfo()

    try:
        image_timestamp = best_image.get("system:time_start").getInfo()
        actual_image_date = pd.Timestamp(image_timestamp, unit="ms").isoformat()
    except Exception as e:
        print(f"[Row {idx}] Could not read actual image date, falling back: {e}")
        actual_image_date = alert["image_date"].isoformat()

    name = alert["name"]
    thumb_filename = os.path.join(PNG_DOWNLOAD_DIR, f"{name}.png")

    point = ee.Geometry.Point([alert["lon"], alert["lat"]])

    # Retry PNG download up to 3 times
    png_attempt = 0
    success_png = False
    while not success_png and png_attempt < 3:
        png_attempt += 1
        try:
            success_png = gee_download_thumb_local(point, best_image, thumb_filename)
        except Exception as e:
            print(f"[Row {idx}] PNG download attempt {png_attempt} failed: {e}")

    if success_png:
        print(f"✓ Downloaded {thumb_filename}")
    else:
        print(f"✗ Failed to download {thumb_filename} after {png_attempt} attempts")

    print(f"✓ Queued {name} from {image_id} (fire_date={alert['image_date'].isoformat()}, actual_image_date={actual_image_date})")
    record = {
        "latitude": alert["lat"],
        "longitude": alert["lon"],
        "image_date": actual_image_date,
        "image_id": image_id,
        "name": name,
    }

    with csv_lock:
        pd.DataFrame([record]).to_csv(OUTPUT_CSV, mode="a", header=False, index=False)
    return record


with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
    list(executor.map(process_no_fire_row, range(len(df))))

print(f"Finished writing records to {OUTPUT_CSV}")
