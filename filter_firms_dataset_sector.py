import pandas as pd

# ---------------------------
# Configuración
# ---------------------------
csv_path = "data/firms_data/VIIRS_S-NPP/Brazil/viirs-snpp_Brazil_merged.csv"  # CSV a leer
output_path = "data/firms_data/VIIRS_S-NPP/Brazil/viirs-snpp_Brazil_merged_filtered.csv"  # CSV filtrado de salida

# Coordenadas límite del cuadrante
Norte = -27.5
Sur   = -33.8
Este  = -49.0
Oeste = -57.5

# ---------------------------
# Leer CSV
# ---------------------------
df = pd.read_csv(csv_path)

# FIRMS suele tener columnas 'latitude' y 'longitude' para coordenadas
# Filtrar por cuadrante
df_filtered = df[
    (df['latitude'] <= Norte) &
    (df['latitude'] >= Sur) &
    (df['longitude'] <= Este) &
    (df['longitude'] >= Oeste)
]

# Guardar CSV filtrado
df_filtered.to_csv(output_path, index=False)
print(f"Filtrado completado. Filas originales: {len(df)}, filas después del filtro: {len(df_filtered)}")
print(f"CSV filtrado guardado en: {output_path}")
