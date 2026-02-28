#!/usr/bin/env python3
import csv
from pathlib import Path


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    input_dir = base_dir / "file_names"
    output_file = input_dir / "file_names_combined.csv"

    if not input_dir.exists():
        raise FileNotFoundError(f"Missing input folder: {input_dir}")

    input_files = [
        input_dir / "test_Fire.csv",
        input_dir / "train_Fire.csv",
        input_dir / "val_Fire.csv",
    ]
    if not input_files:
        raise FileNotFoundError(f"No CSV files found in: {input_dir}")

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
                elif header != expected_header:
                    raise ValueError(
                        "Header mismatch in file: "
                        f"{path.name}.\nExpected: {expected_header}\nGot: {header}"
                    )

                for row in reader:
                    if row:
                        writer.writerow(row)

    print(f"Wrote combined file: {output_file}")


if __name__ == "__main__":
    main()
