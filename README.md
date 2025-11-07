# Wildfires Data Exploration

This repository automates the download of satellite imagery showing temperature anomalies from **Google Earth Engine (GEE)**, using coordinates and timestamps provided by the [FIRMS](https://firms.modaps.eosdis.nasa.gov/map/#d:24hrs;@0.0,0.0,3.0z) datasets.

## Usage

1. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate   # On Linux/Mac
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
   - Set the required variables such as project name, input/output paths, and satellite type.

5. **Run the image collection script:**
   ```bash
   python collect_images.py
   ```
