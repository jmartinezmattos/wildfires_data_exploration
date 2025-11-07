import ee
import json
import pandas as pd
from tqdm import tqdm
import os
import requests
import datetime
from datetime import timezone
from geopy.distance import geodesic
import shutil
from download_firms_data import download_firms_data

CONFG_FILE_NAME = "config/collect_images_config.json"

with open(CONFG_FILE_NAME, "r") as f:
    config = json.load(f)

GEE_PROJECT = config.get("GEE_PROJECT")

print(f"Configuración cargada: {config}")

old_run_dir = config.get("OLD_RUN_DIR", False)

old_run = old_run_dir if old_run_dir and old_run_dir != "False" else False

if old_run_dir and not os.path.exists(old_run_dir):
    print(f"La carpeta de ejecución anterior {old_run_dir} no existe. Iniciando una nueva ejecución.")
    old_run = False

if old_run:
    print(f"Cargando configuración de ejecución anterior {old_run}")
    with open(f"{old_run}/config.json", "r") as f:
        config = json.load(f)
    OUTPUT_IMG_DIR = old_run

FIRMS_INSTRUMENT = config.get("FIRMS_INSTRUMENT")
COUNTRY = config.get("COUNTRY")

with open("config/instrument_map.json", "r") as f:
    insrtument_map = json.load(f)

CSV_PATH = config.get("CSV_PATH", None)

if CSV_PATH is None or CSV_PATH == "" or CSV_PATH.lower() == "null":
    
    CSV_PATH = f"data/firms_data/{FIRMS_INSTRUMENT.replace(' ', '_')}/{COUNTRY}/{insrtument_map[FIRMS_INSTRUMENT]}_{COUNTRY.replace(' ', '_')}_merged.csv"

    if not os.path.exists(CSV_PATH):
        print(f"Archivo CSV de FIRMS no encontrado en {CSV_PATH}. Iniciando descarga...")
        download_firms_data(COUNTRY, FIRMS_INSTRUMENT)


IMAGES_SATELLITE = config["IMAGES_SATELLITE"]

if not old_run:
    OUTPUT_IMG_DIR = f"data/{CSV_PATH.split('/')[-1].replace('.csv','')}_{IMAGES_SATELLITE}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

THUMB_SIZE = config["THUMB_SIZE"]
MAX_IMAGES_PER_POINT = config["MAX_IMAGES_PER_POINT"]
MAX_TIME_DIFF_HOURS = config["MAX_TIME_DIFF_HOURS"]
CLOUD_FILTER_PERCENTAGE = config["CLOUD_FILTER_PERCENTAGE"]
OUTPUT_CSV = f"{OUTPUT_IMG_DIR}/firms_features.csv"

# Set buffer depending on satellite type
BUFFER_METERS = config["BUFFER_METERS"].get(IMAGES_SATELLITE, config["BUFFER_METERS"]["default"])

COLUMNS = ['latitude', 'longitude', 'FIRMS_date', 'image_date', 'date_diff_hours', 'cloud_pct', 'thumbnail_file', 'satellite_image_source', 'detecion_source']


def filter_by_satellite_start_date(df: pd.DataFrame, satellite: str) -> pd.DataFrame:

    SATELLITE_START_DATES = {
        "sentinel-2": datetime.datetime(2015, 7, 1),
        "landsat-8": datetime.datetime(2013, 2, 11),
        "aqua": datetime.datetime(2002, 7, 4),
        "fengyun": datetime.datetime(2016, 12, 7)
    }

    if satellite not in SATELLITE_START_DATES:
        print(f"No se conoce la fecha mínima operativa para {satellite}. No se filtrará el DataFrame.")
        return df.copy()

    # Convertir columna de fechas a datetime
    df_copy = df.copy()
    df_copy['acq_date_dt'] = pd.to_datetime(df_copy['acq_date'], format="%Y-%m-%d", errors='coerce')

    min_date = SATELLITE_START_DATES[satellite]
    before_filter = len(df_copy)
    df_filtered = df_copy[df_copy['acq_date_dt'] >= min_date].reset_index(drop=True)
    after_filter = len(df_filtered)
    print(f"Se eliminaron {before_filter - after_filter} puntos anteriores a {min_date.date()} para {satellite}")

    df_filtered = df_filtered.drop(columns=['acq_date_dt'])
    df_filtered = df_filtered.sort_values(by=['acq_date', 'acq_time'], ascending=[False, False]).reset_index(drop=True)
    return df_filtered

def clean_firms_df(df: pd.DataFrame, exclude_points: list, radius_km: float=3) -> pd.DataFrame:
    """
    Filtra los puntos del DataFrame eliminando aquellos que estén
    a menos de `radius_km` de cualquiera de las coordenadas en exclude_points.

    Parámetros:
    - df: pandas.DataFrame con columnas 'latitude' y 'longitude'
    - exclude_points: lista de tuplas [(lat, lon), ...] a excluir
    - radius_km: radio de exclusión en kilómetros (default 5)

    Retorna:
    - pandas.DataFrame filtrado
    """
    keep_mask = []
    for _, row in df.iterrows():
        lat, lon = row['latitude'], row['longitude']
        keep = True
        for ex_lat, ex_lon in exclude_points:
            if geodesic((lat, lon), (ex_lat, ex_lon)).km < radius_km:
                keep = False
                print(f"Excluyendo punto ({lat}, {lon}) cercano a ({ex_lat}, {ex_lon})")
                break
        keep_mask.append(keep)
    
    clean_df = df[keep_mask].reset_index(drop=True)

    return clean_df

# Función para descargar thumbnail
def download_thumbnail(image, filename, point, satellite, bands=['B4','B3','B2'], size=THUMB_SIZE):

    region = point.buffer(BUFFER_METERS).bounds().getInfo()['coordinates'][0]

    if satellite == "landsat-8":
        bands = ['SR_B4', 'SR_B3', 'SR_B2']
        image = image.select(bands).multiply(0.0000275).add(-0.2)
        vmin, vmax = 0, 0.3  # reflectancia ya escalada
    elif satellite == "sentinel-2":
        bands = ['B4', 'B3', 'B2']
        vmin, vmax = 0, 6000
    elif satellite == "aqua":
        bands = ['sur_refl_b01','sur_refl_b04','sur_refl_b03']
        vmin, vmax = 0, 5000
    elif satellite == "fengyun":
        bands = ['Channel0001','Channel0002','Channel0003']
        vmin, vmax = 0, 4000
    else:
        raise ValueError(f"Satélite no soportado para miniatura: {satellite}")

    try:
        thumb_url = image.getThumbURL({
            'dimensions': size, # pixels
            'region': region, # region geografica
            'bands': bands,
            'min': vmin,
            'max': vmax,
        })
        r = requests.get(thumb_url)
        if r.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(r.content)
            return True
        else:
            print(f"Error HTTP {r.status_code} al descargar {filename}")
    except Exception as e:
        print(f"Error al generar miniatura: {e}")
    return False

def process_and_download(image, point, idx, datetime_str, satellite):

    coords = point.coordinates().getInfo()
    lon = coords[0]
    lat = coords[1]

    print(f"Procesando punto {idx} en ({lat}, {lon}) con imagen del {datetime_str} de {satellite}")

    millis = image.get('system:time_start').getInfo()
    img_date = datetime.datetime.utcfromtimestamp(millis / 1000).isoformat()

    try:
        cloud_pct = image.get('CLOUDY_PIXEL_PERCENTAGE').getInfo()
    except Exception:
        cloud_pct = None

    # Descargar miniatura
    try:
        img_filename = os.path.join(OUTPUT_IMG_DIR, f"point_{idx}.png")
        download_thumbnail(image, img_filename, point, satellite)
    except Exception as e:
        print(f"No se pudo descargar miniatura para punto {idx}: {e}")
        img_filename = None

    alert_dt = datetime.datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)

    if isinstance(img_date, str):
        img_date_dt = datetime.datetime.fromisoformat(img_date).replace(tzinfo=timezone.utc)
        date_diff_hours = round((img_date_dt - alert_dt).total_seconds() / 3600, 2)
    else:
        print(f"Advertencia: img_date es None para punto {idx}")
        img_date_dt = None
        date_diff_hours = None


    result = {
        'latitude': lat,
        'longitude': lon,
        'FIRMS_date': datetime_str,
        'image_date': img_date,
        'date_diff_hours': date_diff_hours,
        'cloud_pct': cloud_pct,
        'thumbnail_file': os.path.basename(img_filename) if img_filename else None,
        'satellite_image_source': satellite,
        'detecion_source': CSV_PATH.split('/')[-1].replace('.csv', '')
    }

    pd.DataFrame([result]).to_csv(
        OUTPUT_CSV,
        mode='a',
        header=False,
        index=False
    )

def get_collection(alert_dt, max_dt, point, satellite="sentinel-2"):

    if satellite == "sentinel-2":
        collection_string = "COPERNICUS/S2_SR_HARMONIZED"
        cloud_property = "CLOUDY_PIXEL_PERCENTAGE"
        cloud_filter = ee.Filter.lt(cloud_property, CLOUD_FILTER_PERCENTAGE)
    elif satellite == "landsat-8":
        collection_string = "LANDSAT/LC08/C02/T1_L2"
        cloud_filter = ee.Filter.lt('CLOUD_COVER', CLOUD_FILTER_PERCENTAGE)
    elif satellite == "aqua":
        collection_string = "MODIS/061/MYD09GA"  # MODIS Aqua Surface Reflectance
        cloud_filter = None  # No hay atributo CLOUDY_PIXEL_PERCENTAGE en este dataset
    elif satellite == "fengyun":
        collection_string = "CMA/FY4A/AGRI/L1"
        cloud_filter = None  # No hay metadato de nubes disponible directamente
    else:
        raise ValueError(f"Satélite no soportado: {satellite}")

    collection = ee.ImageCollection(collection_string).filterBounds(point).filterDate(alert_dt, max_dt)

    collection_size = collection.size().getInfo()
    if collection_size == 0:
        return None

    if cloud_filter:
        collection = collection.filter(cloud_filter)

    collection = collection.sort('system:time_start')

    return collection

def check_valid_image(image, satellite):

    if satellite == "sentinel-2":
        required_bands = ['B8','B4','B3','B12']
    elif satellite == "landsat-8":
        required_bands = ['SR_B5', 'SR_B4', 'SR_B3', 'SR_B7']
    elif satellite == "aqua":
        required_bands = ['sur_refl_b01', 'sur_refl_b02', 'sur_refl_b07']  # bandas visibles/NIR de Aqua
    elif satellite == "fengyun":
        required_bands = ['Channel0001', 'Channel0002', 'Channel0003']
    else:
        raise ValueError(f"Satélite no soportado: {satellite}")

    if image is not None and image.getInfo() is not None:
            bands = image.bandNames().getInfo()

            if not all(b in bands for b in required_bands):
                return False
    else:
        return False
    
    return True
        
def process_data(detected_coordinates_df, images_satellite, max_images_per_point):
    
    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            pd.DataFrame(columns=COLUMNS).to_csv(f, index=False)

    for idx, row in tqdm(detected_coordinates_df.iterrows(), total=len(detected_coordinates_df)):
        lat, lon = row['latitude'], row['longitude']

        date_str = row['acq_date']
        time_str = str(row['acq_time']).zfill(4) 
        
        point = ee.Geometry.Point(lon, lat)
        
        time_formatted = f"{time_str[:2]}:{time_str[2:]}:00"
        datetime_str = f"{date_str}T{time_formatted}"
        
        alert_dt = datetime.datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        min_dt = alert_dt - datetime.timedelta(hours=1)
        max_dt = alert_dt + datetime.timedelta(hours=MAX_TIME_DIFF_HOURS)

        collection = get_collection(min_dt, max_dt, point, images_satellite)

        if collection is not None:
            images_list = collection.toList(max_images_per_point)
            n_images = min(images_list.size().getInfo(), max_images_per_point)

            for i in range(n_images):
                try:
                    image = ee.Image(images_list.get(i))
                    if check_valid_image(image, images_satellite):
                        im_idx = f"{idx}_{i+1}" if n_images > 1 else str(idx)
                        process_and_download(image, point, im_idx, datetime_str, images_satellite)
                except Exception as e:
                    print(f"Error procesando imagen {i+1} para punto {idx}: {e}")


if __name__ == "__main__":

    ee.Initialize(project=GEE_PROJECT)

    os.makedirs(OUTPUT_IMG_DIR, exist_ok=True)

    print("Iniciando procesamiento de datos...")
    print(f"Archivo de detecciones de entrada: {CSV_PATH}")

    firms_data = pd.read_csv(CSV_PATH)
    firms_data = filter_by_satellite_start_date(firms_data, IMAGES_SATELLITE)

    if old_run:
        last_image = pd.read_csv(f'{old_run}/firms_features.csv').iloc[-1]['thumbnail_file'].split('_')[1].replace('.png','')
        print(f"Siguiendo desde imagen {last_image}...")
        firms_data = firms_data.iloc[int(last_image)+1:]
    else:
        config_dest_path = os.path.join(OUTPUT_IMG_DIR, "config.json")
        shutil.copy(CONFG_FILE_NAME, config_dest_path)

    print(f"{len(firms_data)} puntos cargados desde {CSV_PATH}")

    process_data(firms_data, IMAGES_SATELLITE, MAX_IMAGES_PER_POINT)

    print(f"Archivo CSV guardado en: {OUTPUT_CSV}")
    print(f"Miniaturas guardadas en: {OUTPUT_IMG_DIR}")
