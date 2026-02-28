base_path = "../all_base_names.txt"
bucket_path = "merged_no_fire.txt"
processed_base_path = "processed_base_names.txt"
processed_bucket_path = "processed_bucket_names.txt"

with open(base_path, 'r') as in_file, open(processed_base_path, 'w') as out_file:
    for line in in_file:
        line = line.strip()
        line = line.replace("New_Zealand", "Zealand")
        line = line.replace("modis_", "")
        out_file.write(line + "\n")

with open(bucket_path, 'r') as in_file, open(processed_bucket_path, 'w') as out_file:
    for line in in_file:
        line = line.strip()
        line = line.replace("New_Zealand", "Zealand")
        out_file.write(line + "\n")


