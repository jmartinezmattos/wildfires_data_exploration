import os
import requests
import pandas as pd
from glob import glob
from time import sleep

# ---------------------------
# Configuración
# ---------------------------
pais = "Brazil"            # país deseado (usar nombre en inglés tal como aparece en URL)
instrument = "VIIRS S-NPP"       # opciones: "MODIS", "VIIRS S‑NPP", "VIIRS NOAA‑20"
end_year = 2024            # año final (inclusivo)
base_output_dir = f"data/firms_data/{instrument.replace(' ', '_')}"
country_output_dir = os.path.join(base_output_dir, pais.replace(' ', '_'))
os.makedirs(country_output_dir, exist_ok=True)

# ---------------------------
# Mapas de instrumentos
# ---------------------------
# Código para URL
instrument_map = {
    "MODIS": "modis",
    "VIIRS S-NPP": "viirs-snpp",
    "VIIRS NOAA-20": "viirs-noaa20"
}

# Año de inicio según instrumento
start_year_map = {
    "MODIS": 2000,
    "VIIRS S-NPP": 2012,
    "VIIRS NOAA-20": 2018
}

if instrument not in instrument_map:
    raise ValueError(f"Instrumento no válido. Opciones: {list(instrument_map.keys())}")

instr_code = instrument_map[instrument]
start_year = start_year_map[instrument]

# ---------------------------
# Función para descargar CSV de un año
# ---------------------------
def download_yearly_csv(country_name, year, instr_code, save_dir):
    fname = f"{instr_code}_{year}_{country_name}.csv"
    url = f"https://firms.modaps.eosdis.nasa.gov/data/country/{instr_code}/{year}/{fname}"
    outpath = os.path.join(save_dir, fname)
    
    print(f"Descargando año {year}: {url}")
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        with open(outpath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Guardado: {outpath}")
        return True
    else:
        print(f"No encontrado o error para {url} (status {r.status_code})")
        return False

# ---------------------------
# Descargar todos los años disponibles
# ---------------------------
for year in range(start_year, end_year + 1):
    download_yearly_csv(pais, year, instr_code, country_output_dir)
    sleep(1)  # opcional, para no saturar servidor

# ---------------------------
# Concatenar los CSVs descargados
# ---------------------------
all_files = glob(os.path.join(country_output_dir, f"{instr_code}_*.csv"))
dfs = []
for fpath in all_files:
    try:
        df = pd.read_csv(fpath)
        dfs.append(df)
    except Exception as e:
        print(f"Error leyendo {fpath}: {e}")

if dfs:
    df_all = pd.concat(dfs, ignore_index=True)
    merged_fname = os.path.join(country_output_dir, f"{instr_code}_{pais.replace(' ', '_')}_merged.csv")
    df_all.to_csv(merged_fname, index=False)
    print(f"CSV final concatenado guardado en: {merged_fname}")
    print(f"Total archivos: {len(dfs)}, total filas: {len(df_all)}")
else:
    print("No se encontraron archivos válidos para concatenar.")
