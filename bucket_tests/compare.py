bucket_path = "processed_bucket_names.txt"
processed_path = "processed_base_names.txt"

only_all_base_path = "only_in_all_base_names.txt"
only_no_fire_path = "only_in_no_fire_bucket.txt"

with open(bucket_path, 'r') as file:
    bucket_names = set(line.strip() for line in file if line.strip())

with open(processed_path, 'r') as file:
    base_names = set(line.strip() for line in file if line.strip())

intersection = bucket_names & base_names
only_in_bucket = bucket_names - base_names
only_in_base = base_names - bucket_names

print(f"Total intersection: {len(intersection)}")
print(f"Total only in bucket: {len(only_in_bucket)}")
print(f"Total only in base: {len(only_in_base)}")

with open(only_all_base_path, 'w') as file:
    for name in sorted(only_in_base):
        file.write(name + "\n")

with open(only_no_fire_path, 'w') as file:
    for name in sorted(only_in_bucket):
        file.write(name + "\n")