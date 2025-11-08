import os
import pandas as pd

def generate_actualized_df(input_dir):

    csv_name = "firms_features.csv"
    csv_path = os.path.join(input_dir, csv_name)

    png_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.png')]

    df = pd.read_csv(csv_path)

    filtered_df = df[df['thumbnail_file'].isin(png_files)].copy()

    filtered_df["country"] = filtered_df["detecion_source"].apply(lambda x: x.split("_")[-1])
    filtered_df["firms_sensor"] = filtered_df["detecion_source"].apply(lambda x: x.split("_")[0])

    filtered_csv_path = os.path.join(input_dir, csv_name.replace('.csv', '_filtered.csv'))
    filtered_df.to_csv(filtered_csv_path, index=False, encoding='utf-8')
    print(f"Filtered CSV saved to: {filtered_csv_path}. Rows saved: {len(filtered_df)} from {len(df)}")

    if filtered_df.empty:
        return None
    else:
        return filtered_df


def generate_all_actualized_df(base_dir):

    all_dfs = []

    for subdir in os.listdir(base_dir):
        subdir_path = os.path.join(base_dir, subdir)
        if os.path.isdir(subdir_path):
            df_filtered = generate_actualized_df(subdir_path)
            if df_filtered is not None:
                all_dfs.append(df_filtered)

    if all_dfs:
        merged_df = pd.concat(all_dfs, ignore_index=True)
        merged_csv_path = os.path.join(base_dir, "firms_features_merged.csv")
        merged_df.to_csv(merged_csv_path, index=False, encoding='utf-8')
        print(f"Merged CSV saved to: {merged_csv_path}")
        print(f"Total rows in merged CSV: {len(merged_df)}")
    else:
        print("No filtered CSVs found to merge.")
    

if __name__ == "__main__":

    base_dir = "data/fire"

    generate_all_actualized_df(base_dir)