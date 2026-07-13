#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path

PREFIXES = ("fire_test_", "fire_train_", "fire_validation_")


def normalize_fire_used_line(line: str) -> str:
    value = line.strip()
    if not value:
        return ""
    for prefix in PREFIXES:
        if value.startswith(prefix):
            return value[len(prefix) :]
    return value


def load_fire_used_set(path: Path) -> set[str]:
    with path.open("r", encoding="utf-8") as stream:
        return {
            normalize_fire_used_line(line)
            for line in stream
            if normalize_fire_used_line(line)
        }


def filter_csv_rows(input_path: Path, output_path: Path, fire_used_set: set[str]) -> tuple[int, int]:
    with input_path.open("r", encoding="utf-8", newline="") as input_file:
        reader = csv.DictReader(input_file)
        fieldnames = reader.fieldnames
        if fieldnames is None:
            raise ValueError(f"Input CSV {input_path} has no header row")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8", newline="") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            writer.writeheader()

            matched = 0
            total = 0

            for row in reader:
                total += 1
                country = row.get("country", "").strip()
                thumbnail = row.get("thumbnail_file", "").strip()
                if not country or not thumbnail:
                    continue

                row_key = f"{country}_{thumbnail}"
                if row_key in fire_used_set:
                    writer.writerow(row)
                    matched += 1

    return total, matched


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter firms_features_merged_last.csv rows by matching country_thumbnail_file values against fire_used.txt"
    )
    parser.add_argument(
        "--fire-used",
        type=Path,
        default=Path("fire_used.txt"),
        help="Path to the fire_used.txt file",
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=Path("no_fire_folder/firms_features_merged_last.csv"),
        help="Path to the input firms features CSV",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("no_fire_folder/firms_features_merged_last_matched.csv"),
        help="Path to write the filtered output CSV",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fire_used_set = load_fire_used_set(args.fire_used)
    total, matched = filter_csv_rows(args.input_csv, args.output_csv, fire_used_set)
    print(f"Loaded {len(fire_used_set)} cleaned fire entries from {args.fire_used}")
    print(f"Processed {total} rows from {args.input_csv}")
    print(f"Wrote {matched} matching rows to {args.output_csv}")


if __name__ == "__main__":
    main()
