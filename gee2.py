import ee
import pandas as pd
from tqdm import tqdm
import os
import requests
import datetime
from datetime import timezone
from geopy.distance import geodesic
from scipy.spatial import cKDTree
import numpy as np

# Inicializar Earth Engine con tu proyecto
ee.Initialize(project='cellular-retina-276416')

# Parámetros
#CSV_PATH = "data/viirs-jpss1_2024_Uruguay.csv"
CSV_PATH = "data/merged_viirs_noa_Uruguay.csv"
OUTPUT_IMG_DIR = "data/viirs_thumbnails"
OUTPUT_CSV = f"{OUTPUT_IMG_DIR}/viirs_features.csv"
BUFFER_METERS = 1000
THUMB_SIZE = 512  # px

MAX_TIME_DIFF_HOURS = 20  # horas

CLOUD_FILTER_PERCENTAGE = 90  # porcentaje máximo de nubes permitido

os.makedirs(OUTPUT_IMG_DIR, exist_ok=True)

# Cargar CSV
data = pd.read_csv(CSV_PATH)
print(f"{len(data)} puntos cargados desde {CSV_PATH}")

# Revisar esta funcion, elimina por cercania en distancia pero ignora fechas
def deduplicate_close_points(df: pd.DataFrame, threshold_km: float = 1) -> pd.DataFrame:
    """
    Elimina puntos duplicados que estén a menos de `threshold_km` usando KD-Tree para mayor velocidad.
    
    Parámetros:
    - df: pandas.DataFrame con columnas 'latitude' y 'longitude'
    - threshold_km: distancia mínima para considerar puntos distintos (default 1 km)
    
    Retorna:
    - pandas.DataFrame filtrado
    """
    print(f"Eliminando puntos duplicados a menos de {threshold_km} km...")
    
    # Convertir lat/lon a coordenadas aproximadas en km usando proyección simple
    lat = df['latitude'].to_numpy()
    lon = df['longitude'].to_numpy()
    # Aproximación: 1° lat ≈ 111 km, 1° lon ≈ 111 km * cos(lat)
    x = lon * 111 * np.cos(np.radians(lat))
    y = lat * 111
    points = np.column_stack([x, y])
    
    tree = cKDTree(points)
    to_keep = np.ones(len(df), dtype=bool)
    
    for i in range(len(df)):
        if not to_keep[i]:
            continue
        # Encontrar vecinos dentro del umbral (excluye el mismo punto)
        idx = tree.query_ball_point(points[i], r=threshold_km)
        idx.remove(i)
        to_keep[idx] = False  # eliminar vecinos cercanos
        if len(idx) > 0:
            print(f"Punto {i} mantiene, eliminando {len(idx)} puntos cercanos.")
    
    df_filtered = df[to_keep].reset_index(drop=True)
    print(f"Puntos después de eliminar duplicados: {len(df_filtered)}")
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
    # clean_df = deduplicate_close_points(clean_df, threshold_km=1)

    return clean_df

data = clean_firms_df(data, exclude_points=[(-32.86024,-56.54006)])

# Función para descargar thumbnail
def download_thumbnail(image, filename, point, bands=['B4','B3','B2'], size=THUMB_SIZE):

    region = point.buffer(BUFFER_METERS).bounds().getInfo()['coordinates'][0]

    try:
        thumb_url = image.getThumbURL({
            'dimensions': size, # pixels
            'region': region, # region geografica
            'bands': bands,
            'min': 0,
            'max': 6000,
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

    # Calcular índices
    try:
        ndvi = image.normalizedDifference(['B8','B4']).rename('NDVI')
        ndwi = image.normalizedDifference(['B3','B8']).rename('NDWI')
        nbr = image.normalizedDifference(['B8','B12']).rename('NBR')
        image = image.addBands([ndvi, ndwi, nbr])

        # Calcular fire_score sin eliminar bandas originales
        fire_score = image.expression(
            '(NDVI * 0.5) + (NDWI * 0.3) + (1 - NBR) * 0.2',
            {'NDVI': image.select('NDVI'),
            'NDWI': image.select('NDWI'),
            'NBR': image.select('NBR')}
        ).rename('fire_score')
        image = image.addBands(fire_score)
    except Exception:
        print(f"Error al calcular índices para punto {idx}")

    # Reducir a valor medio alrededor del punto
    try:
        stats = image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=point.buffer(BUFFER_METERS),
            scale=30
        ).getInfo()
        mean_ndvi = stats.get('NDVI')
        mean_ndwi = stats.get('NDWI')
        mean_nbr = stats.get('NBR')
        mean_score = stats.get('fire_score')
    except Exception:
        print(f"Error al reducir región para punto {idx}")
        mean_ndvi = mean_ndwi = mean_nbr = mean_score = None

    # Fecha de la imagen
    try:
        img_date = datetime.datetime.utcfromtimestamp(image.get('system:time_start').getInfo() / 1000).isoformat()
    except Exception:
        print(f"No se pudo obtener la fecha de la imagen para punto {idx}")
        img_date = None

    try:
        cloud_pct = image.get('CLOUDY_PIXEL_PERCENTAGE').getInfo()
    except Exception:
        cloud_pct = None

    # Descargar miniatura
    try:
        img_filename = os.path.join(OUTPUT_IMG_DIR, f"point_{idx}.png")
        download_thumbnail(image, img_filename, point)
    except Exception as e:
        print(f"No se pudo descargar miniatura para punto {idx}: {e}")
        img_filename = None

    
    img_date_dt = datetime.datetime.fromisoformat(img_date).replace(tzinfo=timezone.utc)
    date_diff_hours = round((img_date_dt - alert_dt).total_seconds() / 3600, 2)

    result = {
        'latitude': lat,
        'longitude': lon,
        'FIRMS_date': datetime_str,
        'image_date': img_date,
        'date_diff_hours': date_diff_hours,
        'NDVI': mean_ndvi,
        'NDWI': mean_ndwi,
        'NBR': mean_nbr,
        'fire_score': mean_score,
        'cloud_pct': cloud_pct,
        'thumbnail_file': os.path.basename(img_filename) if img_filename else None,
        'satellite': satellite
    }

    pd.DataFrame([result]).to_csv(
        OUTPUT_CSV,
        mode='a',
        header=False,
        index=False
    )


# Columnas nuevas
columns = ['latitude', 'longitude', 'FIRMS_date', 'image_date', 'date_diff_hours', 'NDVI', 'NDWI', 'NBR', 'fire_score', 'cloud_pct', 'thumbnail_file', 'satellite']
with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
    pd.DataFrame(columns=columns).to_csv(f, index=False)

# Procesar cada punto
for idx, row in tqdm(data.iterrows(), total=len(data)):
    lat, lon = row['latitude'], row['longitude']

    # FIRMS guarda en UTC
    date_str = row['acq_date']
    time_str = str(row['acq_time']).zfill(4) 
    
    point = ee.Geometry.Point(lon, lat)
    
    time_formatted = f"{time_str[:2]}:{time_str[2:]}:00"
    datetime_str = f"{date_str}T{time_formatted}"
    
    #alert_dt = datetime.datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S")
    alert_dt = datetime.datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    max_dt = alert_dt + datetime.timedelta(hours=MAX_TIME_DIFF_HOURS)

    collection_sentinel = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(point)
        .filterDate(alert_dt, max_dt)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", CLOUD_FILTER_PERCENTAGE))
        .sort('system:time_start')  # Ordenar por fecha
    )

    image = collection_sentinel.first() # Primer imagen después de la alerta

    if image is not None and image.getInfo() is not None:
        bands = image.bandNames().getInfo()
        required_sentinel_bands = ['B8','B4','B3','B12']

        if not all(b in bands for b in required_sentinel_bands):
            image = None
        else:
            satellite = 'Sentinel-2'

    # if image is None or image.getInfo() is None:
    #     collection_landsat = (
    #         ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
    #         .filterBounds(point)
    #         .filterDate(alert_dt, max_dt)
    #         .sort('system:time_start')
    #     )

    #     image = collection_landsat.first()

    #     if image is not None and image.getInfo() is not None:
    #         satellite = 'Landsat-8'
    #         image = image.rename(['B2','B3','B4','B5','B6','B7'])

    #         bands = image.bandNames().getInfo()
    #         required_landsat_bands = ['B2','B3','B4','B5','B6','B7']
    #         if not all(b in bands for b in required_landsat_bands):
    #             image = None

    #     else:
    #         satellite = 'None'

    if image is not None and image.getInfo() is not None:
        process_and_download(image, point, idx, datetime_str, satellite)


print(f"Archivo CSV guardado en: {OUTPUT_CSV}")
print(f"Miniaturas guardadas en: {OUTPUT_IMG_DIR}")
