import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np

# Archivos de entrada
FILES = {
    "VIIRS NOAA-20": "firms_datasets/merged_viirs_noa_Uruguay.csv",
    "VIIRS SNPP": "firms_datasets/merged_virss_snpp_Uruguay.csv",
    "MODIS": "firms_datasets/merged_modis_Uruguay.csv"
}

OUTPUT_PATH = "firms_datasets/acq_time_comparison_grouped.png"

def load_and_process(filepath):
    """Carga el CSV y devuelve un DataFrame con columna 'hour'."""
    df = pd.read_csv(filepath)
    df['acq_time'] = df['acq_time'].astype(str).str.zfill(4)
    df['hour'] = df['acq_time'].str[:2].astype(int)
    return df

# Crear tabla de frecuencias por hora
all_counts = pd.DataFrame({'hour': range(24)})

for label, path in FILES.items():
    if os.path.exists(path):
        df = load_and_process(path)
        counts = df['hour'].value_counts().sort_index()
        all_counts[label] = all_counts['hour'].map(counts).fillna(0)
    else:
        print(f"⚠️ No se encontró el archivo: {path}")
        all_counts[label] = 0

# Parámetros del gráfico
bar_width = 0.25
x = np.arange(len(all_counts['hour']))

colors = {
    "VIIRS NOAA-20": "tab:blue",
    "VIIRS SNPP": "tab:orange",
    "MODIS": "tab:green"
}

plt.figure(figsize=(12, 6))

# Dibujar barras agrupadas
for i, label in enumerate(FILES.keys()):
    plt.bar(
        x + i * bar_width,
        all_counts[label],
        width=bar_width,
        label=label,
        color=colors[label],
        edgecolor='black',
        alpha=0.85
    )

# Configuración del gráfico
plt.title("Distribución de horas de detección (acq_time) por satélite")
plt.xlabel("Hora del día (UTC)")
plt.ylabel("Cantidad de detecciones")
plt.xticks(x + bar_width, all_counts['hour'])
plt.grid(axis='y', linestyle='--', alpha=0.6)
plt.legend()
plt.tight_layout()

# Guardar gráfico
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
plt.savefig(OUTPUT_PATH, dpi=300)
plt.close()

print(f"Gráfico guardado en: {OUTPUT_PATH}")
