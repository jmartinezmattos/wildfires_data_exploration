#!/usr/bin/env python3
from pathlib import Path
import concurrent.futures


def read_lines(path: Path) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    all_base_names_path = base_dir / "all_base_names.txt"
    combined_path = base_dir / "file_names" / "file_names_combined.csv"
    missing_in_all_path = base_dir / "missing_en_csvs.txt"
    missing_in_combined_path = base_dir / "missing_en_bucket_viejo.txt"
    ".txt"

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        all_future = executor.submit(read_lines, all_base_names_path)
        combined_future = executor.submit(read_lines, combined_path)
        all_base_names = all_future.result()
        combined_names = combined_future.result()

    missing_in_all = sorted(name for name in combined_names if name not in all_base_names)
    missing_in_combined = sorted(name for name in all_base_names if name not in combined_names)

    with missing_in_all_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(missing_in_all))
        if missing_in_all:
            f.write("\n")

    with missing_in_combined_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(missing_in_combined))
        if missing_in_combined:
            f.write("\n")

    print(
        "Found "
        f"{len(missing_in_all)} missing in all_base_names and "
        f"{len(missing_in_combined)} missing in file_names_combined."
    )
    print(
        "Wrote: "
        f"{missing_in_all_path} and {missing_in_combined_path}"
    )


if __name__ == "__main__":
    main()
