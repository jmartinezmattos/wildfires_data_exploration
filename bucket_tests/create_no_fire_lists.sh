#!/bin/bash

# ====== CONFIGURE THIS ======
BUCKET_NAME="fire_model_dataset"
CLASS_FOLDER="No Fire"
# ============================

echo "Generating file lists..."
echo "----------------------------------"

# Train
gsutil ls "gs://${BUCKET_NAME}/train/${CLASS_FOLDER}/" \
  | awk -F/ '{print $NF}' \
  > train_no_fire.txt

echo "Created train_no_fire.txt"

# Test
gsutil ls "gs://${BUCKET_NAME}/test/${CLASS_FOLDER}/" \
  | awk -F/ '{print $NF}' \
  > test_no_fire.txt

echo "Created test_no_fire.txt"

# Val
gsutil ls "gs://${BUCKET_NAME}/val/${CLASS_FOLDER}/" \
  | awk -F/ '{print $NF}' \
  > val_no_fire.txt

echo "Created val_no_fire.txt"

# Merge all
cat train_no_fire.txt test_no_fire.txt val_no_fire.txt > merged_no_fire.txt

echo "Created merged_no_fire.txt"
echo "----------------------------------"
echo "Running Python scripts..."
echo "----------------------------------"

# Run Python scripts
python3 process_base_names.py
python3 compare.py

rm train_no_fire.txt test_no_fire.txt val_no_fire.txt
rm merged_no_fire.txt

echo "All done."
