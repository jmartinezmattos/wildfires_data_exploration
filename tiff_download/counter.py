import csv
from pathlib import Path
p = Path('/home/fperdomo/Documents/fium/data-exploration/wildfires_data_exploration/tiff_download/time_differences.csv')
count_not_000 = 0
with p.open(newline='', encoding='utf-8') as f:
    reader = csv.reader(f)
    total_count = 0
    zeros_count = 0
    for row in reader:
        if row == ['0', '0', '0']:
            zeros_count += 1
        total_count += 1
print(f"Total rows: {total_count}")
print(f"Rows with '0,0,0': {zeros_count}")
