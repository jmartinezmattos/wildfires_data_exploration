#!/usr/bin/env python3
import argparse
import csv
import datetime
import json
import os
import random
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import ee
import pandas as pd
from tqdm import tqdm
import requests

DEFAULT_CONFIG_PATH = Path("config/collect_images_config.json")
DEFAULT_COLLECTION = "COPERNICUS/S2_SR_HARMONIZED"
DEFAULT_SATELLITE = "sentinel-2"
DEFAULT_BUFFER_METERS = 256
DEFAULT_THUMB_SIZE = 256
DEFAULT_NUM_THREADS = 4
DEFAULT_MAX_CLOUD_PERCENTAGE = 80


def download_thumbnail(image, filename: str, point, satellite: str, size: int, buffer_meters: int) -> bool:
    region = point.buffer(buffer_meters).bounds().getInfo()["coordinates"][0]
    if satellite == "sentinel-2":
        bands = ["B4", "B3", "B2"]
        vmin, vmax = 0, 6000
    else:
        raise ValueError(f"Satellite not supported: {satellite}")

    try:
        print(f"Downloading thumbnail for {filename} with size {size} and region {region}", flush=True)
        thumb_url = image.getThumbURL({
            "dimensions": size,
            "region": region,
            "bands": bands,
            "min": vmin,
            "max": vmax,
        })
        response = requests.get(thumb_url)
        response.raise_for_status()
        with open(filename, "wb") as out_file:
            out_file.write(response.content)
        return True
    except Exception as exc:
        print(f"Error downloading {filename}: {exc}")
        return False

OUTPUT_COLUMNS = [
    "latitude",
    "longitude",
    "image_date",
    "downloaded_image_date",
    "date_diff_hours",
    "thumbnail_file",
    "satellite_image_source",
    "country",
    "firms_sensor",
]


def load_config(path: Path) -> dict:
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download matched fire images from GEE using COPERNICUS/S2_SR_HARMONIZED and save results to a CSV."
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=Path("firms_features_merged_last_matched.csv"),
        help="Input matched CSV file with image_date and thumbnail_file columns.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("matched_fire_images"),
        help="Directory where downloaded images and the output CSV are saved.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Output CSV file for results. If omitted, uses output-dir/matched_fire_images.csv.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="JSON config file path for GEE project and thumbnail settings.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=None,
        help="Number of concurrent download threads.",
    )
    return parser.parse_args()


def parse_image_date(date_str: str) -> datetime.datetime | None:
    if not isinstance(date_str, str) or not date_str.strip():
        return None
    try:
        return datetime.datetime.fromisoformat(date_str)
    except ValueError:
        try:
            return datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return None


def get_random_date_window(target_date: datetime.datetime) -> tuple[datetime.datetime, datetime.datetime]:
    month_offset = random.randint(1, 12)
    day_offset = random.randint(1, 10) * 5
    offset_days = month_offset * 30 + day_offset
    start_date = target_date - datetime.timedelta(days=offset_days)
    end_date = target_date
    return start_date, end_date


def get_gee_image(point, target_date: datetime.datetime) -> tuple[object, datetime.datetime] | tuple[None, None]:
    start_date, end_date = get_random_date_window(target_date)
    collection = (
        ee.ImageCollection(DEFAULT_COLLECTION)
        .filterBounds(point)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', DEFAULT_MAX_CLOUD_PERCENTAGE))
    )

    size = collection.size().getInfo()
    if size == 0:
        return None, None

    images = collection.toList(size)
    best_image = None
    best_dt = None
    best_diff = None

    for i in range(size):
        image = ee.Image(images.get(i))
        info = image.getInfo()
        if not info or "properties" not in info:
            continue
        bands = [b["id"] for b in info.get("bands", [])]
        if not all(b in bands for b in ["B4", "B3", "B2"]):
            continue

        millis = info["properties"].get("system:time_start")
        if millis is None:
            continue

        image_dt = datetime.datetime.utcfromtimestamp(millis / 1000)
        diff = abs((image_dt - target_date).total_seconds())
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best_image = image
            best_dt = image_dt

    return (best_image, best_dt) if best_image is not None else (None, None)


def process_row(idx: int, row: dict, output_dir: Path, thumb_size: int) -> dict | None:
    latitude = row.get("latitude")
    longitude = row.get("longitude")
    if pd.isna(latitude) or pd.isna(longitude):
        print(f"Skipping row {idx} because latitude/longitude are missing")
        return None

    target_date = parse_image_date(str(row.get("image_date", "")))
    if target_date is None:
        print(f"Skipping row {idx} because image_date is invalid: {row.get('image_date')}")
        return None

    point = ee.Geometry.Point(float(longitude), float(latitude))
    image, downloaded_date = get_gee_image(point, target_date)
    if image is None:
        print(f"No image found for row {idx} hours of {target_date}")
        return None

    original_file = str(row.get("thumbnail_file", f"point_{idx}.png"))
    base_name, ext = os.path.splitext(os.path.basename(original_file))
    country = str(row.get("country", "unknown_country")).replace(" ", "_")
    filename = output_dir / f"matched_{country}_{base_name}{ext or '.png'}"

    success = download_thumbnail(image, str(filename), point, DEFAULT_SATELLITE, size=thumb_size)
    if not success:
        print(f"Failed to download thumbnail for row {idx}: {filename}")
        return None

    date_diff_hours = round((downloaded_date - target_date).total_seconds() / 3600, 2)

    return {
        "latitude": float(latitude),
        "longitude": float(longitude),
        "image_date": row.get("image_date"),
        "downloaded_image_date": downloaded_date.isoformat(),
        "date_diff_hours": date_diff_hours,
        "thumbnail_file": filename.name,
        "satellite_image_source": DEFAULT_SATELLITE,
        "country": row.get("country"),
        "firms_sensor": row.get("firms_sensor"),
    }


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    project = config.get("GEE_PROJECT")
    thumb_size = int(config.get("THUMB_SIZE", DEFAULT_THUMB_SIZE))
    num_threads = args.threads or int(config.get("NUM_THREADS", DEFAULT_NUM_THREADS))

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = args.output_csv or output_dir / "firms_features_merged_last_matched_downloads.csv"

    if not args.input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {args.input_csv}")

    ee.Initialize(project=project) if project else ee.Initialize()

    df = pd.read_csv(args.input_csv)
    rows = list(df.to_dict(orient="records"))
    results = []

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(process_row, idx, row, output_dir, thumb_size) for idx, row in enumerate(rows)]
        for future in tqdm(futures, total=len(futures), desc="Downloading images"):
            result = future.result()
            if result is not None:
                results.append(result)

    if results:
        output_df = pd.DataFrame(results, columns=OUTPUT_COLUMNS)
        output_df.to_csv(output_csv, index=False)
        print(f"Saved {len(output_df)} downloaded image records to {output_csv}")
    else:
        print("No images were downloaded.")


if __name__ == "__main__":
    main()
