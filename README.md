# Wildfires Data Exploration

This project automates the retrieval and visualization of satellite imagery corresponding to fire detections reported by [FIRMS](https://firms.modaps.eosdis.nasa.gov/map/#d:24hrs;@0.0,0.0,3.0z). It aims to support wildfire analysis, remote sensing validation, and visual inspection of fire-related thermal anomalies.

## Usage

1. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate   # On Linux/Mac
   
   venv\Scripts\Activate.ps1  # On Windows powershell
   venv\Scripts\activate      # On Windows
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Authenticate with Google Earth Engine:**
   ```bash
   earthengine authenticate
   ```

4. **Set up configuration:**
   - Create a file named `collect_images_config.json`.
   - Use `collect_images_config_example.json` as a reference.
   - Set the required variables as described below.

5. **Run the image collection script:**
   ```bash
   python collect_images.py
   ```

## Configuration File (`collect_images_config.json`)

Example:

```json
{
    "GEE_PROJECT": "cellular-retina-276416",
    "IMAGES_SATELLITE": "sentinel-2",
    "COUNTRY": "Uruguay",
    "FIRMS_INSTRUMENT": "VIIRS S-NPP",
    "CSV_PATH": null,
    "BUFFER_METERS": {
        "default": 2000,
        "aqua": 30000,
        "fengyun": 30000
    },
    "THUMB_SIZE": 1024,
    "MAX_IMAGES_PER_POINT": 1,
    "MAX_TIME_DIFF_HOURS": 10,
    "CLOUD_FILTER_PERCENTAGE": 85,
    "OLD_RUN_DIR": null
}
```

## Variable Descriptions

| Variable | Type | Description |
|-----------|------|-------------|
| **`GEE_PROJECT`** | `string` | Google Earth Engine project name. |
| **`IMAGES_SATELLITE`** | `string` | Satellite used for image download. Supported options: `"sentinel-2"`, `"landsat-8"`, `"aqua"`, `"fengyun"`. |
| **`COUNTRY`** | `string` | Country for which to download FIRMS data. |
| **`FIRMS_INSTRUMENT`** | `string` | FIRMS instrument providing fire detections. Options: `"MODIS"`, `"VIIRS S-NPP"`, `"VIIRS NOAA-20"`. |
| **`CSV_PATH`** | `string or null` | Optional. If set, uses a local CSV file with coordinates and timestamps. If `null`, data is downloaded automatically from FIRMS for the specified country and instrument. |
| **`BUFFER_METERS`** | `object` | Defines buffer radius (in meters) around detections for image download. Default values may vary by satellite. |
| **`THUMB_SIZE`** | `integer` | Output image resolution (in pixels per side). |
| **`MAX_IMAGES_PER_POINT`** | `integer` | Maximum number of images downloaded per detection. |
| **`MAX_TIME_DIFF_HOURS`** | `integer` | Maximum time window (in hours) between FIRMS detection and satellite image. |
| **`CLOUD_FILTER_PERCENTAGE`** | `integer` | Maximum allowed cloud coverage percentage (only applies to Sentinel-2 and Landsat-8). |
| **`OLD_RUN_DIR`** | `string or null` | Optional. If set, resumes a previous job using its saved configuration, ignoring the current JSON file. |

## Output files description

| File                 | Description                                       |
| -------------------- | ------------------------------------------------- |
| `firms_features.csv` | Log of detections and associated satellite images |
| `point_XX.png`       | RGB thumbnails of the detected locations          |
| `config.json`        | Saved configuration for reproducibility           |

## Resume run feature

If the script stops unexpectedly, set "OLD_RUN_DIR" in the configuration to the previous run’s directory.
The script will automatically resume from the last processed detection.
