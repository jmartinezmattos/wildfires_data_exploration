from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import os
import pandas as pd
from pathlib import Path
import ee
import requests

PNG_DOWNLOAD_DIR = "fire_downloads_png_v2"
NUM_WORKERS = 8
OUTPUT_CSV = os.path.join("final_data", "downloaded_fire_images_v2.csv")
BANDS_TO_EXTRACT = ["B4", "B3", "B2", "B8", "B11", "B12"]
csv_path = "final_data/fires_minimalist_updated.csv"
WINDOW_HOURS = 10


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

def gee_initialization(project: str | None = None) -> None:
    env_values = load_env_file(Path("config/.env"))
    os.environ.update(env_values)

    gee_project = project or os.environ.get("GEE_PROJECT")
    try:
        ee.Initialize(project=gee_project)
    except Exception:
        ee.Initialize()

def read_row_fire(df: pd.DataFrame, index: int) -> dict | None:
    if index < 0 or index >= len(df):
        return None

    row = df.iloc[index]
    target_dt = pd.to_datetime(row["image_date"])

    return {
        "lat": float(row["latitude"]),
        "lon": float(row["longitude"]),
        "image_date": target_dt,
        "name": str(row["name"]).strip(),
    }

def gee_get_image_in_window(lat: float, lon: float, target_dt: pd.Timestamp, window_hours: int):
    start_dt = target_dt - pd.Timedelta(hours=window_hours)
    end_dt = target_dt + pd.Timedelta(hours=window_hours)

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

def gee_get_closest_fire_image(lat: float, lon: float, target_dt: pd.Timestamp):
    return gee_get_image_in_window(lat, lon, target_dt, WINDOW_HOURS)

### END FUNCTIONS ###

# Initialize Earth Engine
gee_initialization()

os.makedirs(PNG_DOWNLOAD_DIR, exist_ok=True)


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

def process_fire_row(idx: int):
    alert = read_row_fire(df, idx)
    if alert is None:
        return None

    try:
        best_image, _ = gee_get_closest_fire_image(alert["lat"], alert["lon"], alert["image_date"])
    except Exception as e:
        print(f"[Row {idx}] Error querying GEE: {e}")
        return None

    if best_image is None:
        print(f"[Row {idx}] ({alert['name']}) No image found within ±{WINDOW_HOURS}h of fire date {alert['image_date'].isoformat()}")
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
    list(executor.map(process_fire_row, range(len(df))))

print(f"Finished writing records to {OUTPUT_CSV}")
