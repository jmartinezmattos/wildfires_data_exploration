from __future__ import annotations

import csv
from pathlib import Path


def load_filenames(csv_path: Path) -> set[str]:
	with csv_path.open(newline="", encoding="utf-8") as handle:
		reader = csv.DictReader(handle)
		return {row["filename"].strip() for row in reader if row.get("filename")}

def main() -> None:
	base_dir = Path(__file__).resolve().parent
	file_dir = base_dir / "file_names"

	test_no_fire = load_filenames(file_dir / "test_No_Fire.csv")
	train_no_fire = load_filenames(file_dir / "train_No_Fire.csv")
	val_no_fire = load_filenames(file_dir / "val_No_Fire.csv")

	# Simple sanity output to confirm counts.
	print(f"test_No_Fire: {len(test_no_fire)}")
	print(f"train_No_Fire: {len(train_no_fire)}")
	print(f"val_No_Fire: {len(val_no_fire)}")

	print(f"ENTRE TRAIN Y TEST HAY SE COMPARTEN {len(test_no_fire & train_no_fire)} ARCHIVOS")
	print(f"ENTRE TEST Y VAL HAY SE COMPARTEN {len(test_no_fire & val_no_fire)} ARCHIVOS")
	print(f"ENTRE TRAIN Y VAL HAY SE COMPARTEN {len(train_no_fire & val_no_fire)} ARCHIVOS")
if __name__ == "__main__":
	main()
