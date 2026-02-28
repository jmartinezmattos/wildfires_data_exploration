from __future__ import annotations

from pathlib import Path


def load_base_names(file_path: Path) -> set[str]:
    """Load lines from a file into a set."""
    final_set = set()
    with file_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                # Convert base name to .tif format
                line = line.replace("New_Zealand", "Zealand")  # Handle country name change
                #line = line.replace("modis_", "")
                final_set.add(line)
        return final_set


def load_csv_files(file_path: Path) -> set[str]:
    """Load lines from a file into a set."""
    final_set = set()
    with file_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                # Convert base name to .tif format
                line = line.replace("no_fire_", "").replace(".png", ".tif")
                line = line.replace("New_Zealand", "Zealand")  # Handle country name change
                final_set.add(line)
        return final_set

def main() -> None:
    base_dir = Path(__file__).resolve().parent
    
    all_base_names_path = base_dir / "all_base_names.txt"
    no_fire_combined_path = base_dir / "no_fire_combined.txt"
    
    # Load both files into sets
    all_base_names = load_base_names(all_base_names_path)
    no_fire_combined = load_csv_files(no_fire_combined_path)
    
    # Calculate intersection
    intersection = all_base_names & no_fire_combined
    
    # Calculate relative complements
    only_in_all_base = all_base_names - no_fire_combined
    only_in_no_fire = no_fire_combined - all_base_names
    
    # Save relative complements to files
    only_all_base_path = base_dir / "only_in_all_base_names.txt"
    only_no_fire_path = base_dir / "only_in_no_fire_combined.txt"
    
    only_all_base_path.write_text("\n".join(sorted(only_in_all_base)) + "\n", encoding="utf-8")
    only_no_fire_path.write_text("\n".join(sorted(only_in_no_fire)) + "\n", encoding="utf-8")
    
    print(f"Saved {len(only_in_all_base)} items to {only_all_base_path.name}")
    print(f"Saved {len(only_in_no_fire)} items to {only_no_fire_path.name}")
    
    # Print results
    print(f"Total in all_base_names.txt: {len(all_base_names)}")
    print(f"Total in no_fire_combined.txt: {len(no_fire_combined)}")
    print(f"\nIntersection (present in both): {len(intersection)}")
    print(f"Only in all_base_names.txt: {len(only_in_all_base)}")
    print(f"Only in no_fire_combined.txt: {len(only_in_no_fire)}")
    
    # Show some examples from each set
    if intersection:
        print(f"\nFirst 10 items in intersection:")
        for item in list(intersection)[:10]:
            print(f"  {item}")
    
    if only_in_all_base:
        print(f"\nFirst 10 items only in all_base_names.txt:")
        for item in list(only_in_all_base)[:10]:
            print(f"  {item}")
    
    if only_in_no_fire:
        print(f"\nFirst 10 items only in no_fire_combined.txt:")
        for item in list(only_in_no_fire)[:10]:
            print(f"  {item}")


if __name__ == "__main__":
    main()
