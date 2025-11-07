import os
import json
import requests
import pandas as pd
from glob import glob
from time import sleep

CONFG_FILE_NAME = "config/download_firms_csv_config.json"

with open(CONFG_FILE_NAME, "r") as f:
    config = json.load(f)

COUNTRY = config.get("COUNTRY", None)
INSTRUMENT = config.get("INSTRUMENT", "ALL") # Instument options: "MODIS", "VIIRS S-NPP", "VIIRS NOAA-20", "ALL"

if COUNTRY is None:
    raise ValueError("COUNTRY must be specified in the config file.")

with open("config/instrument_map.json", "r") as f:
    insrtument_map = json.load(f)

start_year_map = {
    "MODIS": 2000,
    "VIIRS S-NPP": 2012,
    "VIIRS NOAA-20": 2018
}

def download_yearly_csv(country_name, year, instr_code, save_dir):
    fname = f"{instr_code}_{year}_{country_name}.csv"
    url = f"https://firms.modaps.eosdis.nasa.gov/data/country/{instr_code}/{year}/{fname}"
    outpath = os.path.join(save_dir, fname)
    
    print(f"Downloading year {year}: {url}")
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        with open(outpath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Saving: {outpath}")
        return True
    else:
        print(f"No encontrado o error para {url} (status {r.status_code})")
        return False

def download_firms_data(country, instrument="ALL", end_year=2024):

    if INSTRUMENT not in instrument_map and INSTRUMENT != "ALL":
        raise ValueError(f"Invalid instrument. Options are: {list(instrument_map.keys())}")

    if instrument == "ALL":
        instruments = ["MODIS", "VIIRS S-NPP", "VIIRS NOAA-20"]
    else:
        instruments = [instrument]

    for instrument in instruments:
        start_year = start_year_map[instrument]
        instr_code = instrument_map[instrument]

        base_output_dir = f"data/firms_data/{instrument.replace(' ', '_')}"
        country_output_dir = os.path.join(base_output_dir, country.replace(' ', '_'))
        os.makedirs(country_output_dir, exist_ok=True)

        for year in range(start_year, end_year + 1):
            download_yearly_csv(country, year, instr_code, country_output_dir)
            sleep(0.5)  # opcional, para no saturar servidor

    for instrument in instruments:
        # Concatenar todos los CSV descargados para el instrumento
        instr_code = instrument_map[instrument]

        base_output_dir = f"data/firms_data/{instrument.replace(' ', '_')}"
        country_output_dir = os.path.join(base_output_dir, country.replace(' ', '_'))

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
            merged_fname = os.path.join(country_output_dir, f"{instr_code}_{country.replace(' ', '_')}_merged.csv")
            df_all.to_csv(merged_fname, index=False)
            print(f"CSV final concatenado guardado en: {merged_fname}")
            print(f"Total archivos: {len(dfs)}, total filas: {len(df_all)}")
        else:
            print("No se encontraron archivos válidos para concatenar.")

if __name__ == "__main__":

    download_firms_data(COUNTRY, INSTRUMENT)
