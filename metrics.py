import pandas as pd
from sklearn.cluster import DBSCAN
import numpy as np


def assign_fire_ids(df, max_km=3):
    df = df.copy()
    df["FIRMS_date"] = pd.to_datetime(df["FIRMS_date"])
    df["day"] = df["FIRMS_date"].dt.date

    kms_per_radian = 6371.0088
    epsilon = max_km / kms_per_radian

    df["fire_id"] = -1
    current_fire_id = 0

    for day, day_df in df.groupby("day"):
        coords = day_df[["latitude", "longitude"]].to_numpy()
        rad = np.radians(coords)

        clustering = DBSCAN(
            eps=epsilon,
            min_samples=1,
            metric="haversine"
        ).fit(rad)

        labels = clustering.labels_

        for cluster_label in set(labels):
            mask = (labels == cluster_label)
            df.loc[day_df.index[mask], "fire_id"] = current_fire_id
            current_fire_id += 1

    return df


if __name__ == "__main__":
    df = pd.read_csv("data/fire/firms_features_merged.csv")

    df_with_ids = assign_fire_ids(df, max_km=3)

    df_with_ids.to_csv("data/fire/firms_with_fire_id.csv", index=False)

    print("Unique wildfires:", df_with_ids["fire_id"].nunique())
