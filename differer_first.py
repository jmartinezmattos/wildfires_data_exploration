dup_image_names = ["South_Africa_point_16592.tif",
"South_Africa_point_18515.tif",
"South_Africa_point_18517.tif",
"South_Africa_point_20469.tif",
"South_Africa_point_20502.tif",
"South_Africa_point_20517.tif",
"South_Africa_point_20555.tif",
"South_Africa_point_20647.tif",
"South_Africa_point_20648.tif",
"South_Africa_point_20819.tif",
"South_Africa_point_22364.tif",
"South_Africa_point_22367.tif",
"South_Africa_point_22370.tif",
"South_Africa_point_22504.tif",
"South_Africa_point_23987.tif",
"South_Africa_point_24111.tif"]

for image_name in dup_image_names:
    print(f"gsutil ls -r gs://fire_model_dataset/**/Fire/{image_name}")