#!/usr/bin/env python3
import csv
from pathlib import Path


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    input_files = [
        base_dir / "data" / "fire" / "firms_features_merged.csv",
        base_dir / "data" / "fire" / "firms_features_filtered_finland.csv",
        base_dir / "data" / "fire" / "firms_features_filtered_ireland.csv",
        base_dir / "data" / "fire" / "firms_features_filtered_new_zealand.csv",
    ]
    output_file = base_dir / "data" / "fire" / "firms_features_filtered_final.csv"

    for path in input_files:
        if not path.exists():
            raise FileNotFoundError(f"Missing input file: {path}")

    with output_file.open("w", newline="") as out_f:
        writer = None
        expected_header = None

        for path in input_files:
            with path.open("r", newline="") as in_f:
                reader = csv.reader(in_f)
                header = next(reader, None)
                if header is None:
                    continue
                if expected_header is None:
                    expected_header = header
                    writer = csv.writer(out_f)
                    writer.writerow(header)

                extras = [col for col in header if col not in expected_header]
                if extras and extras != ["firms_sensor"]:
                    raise ValueError(
                        "Header mismatch in file: "
                        f"{path.name}.\nExpected: {expected_header}\nGot: {header}"
                    )

                expected_indexes = [header.index(col) for col in expected_header]

                for row in reader:
                    if row:
                        normalized_row = [row[idx] for idx in expected_indexes]
                        writer.writerow(normalized_row)

    print(f"Wrote combined file: {output_file}")


if __name__ == "__main__":
    main()
