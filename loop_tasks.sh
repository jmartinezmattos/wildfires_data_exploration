#!/bin/bash

# --- CONFIGURATION ---
PYTHON_SCRIPT="tasks_check.py"  # The name of your python file
INTERVAL=600                    # Time in seconds (600s = 10 minutes)
# ---------------------

echo "Starting $PYTHON_SCRIPT loop (Interval: $INTERVAL seconds)"

# Check if the file exists before starting the loop
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "Error: $PYTHON_SCRIPT not found in the current directory."
    exit 1
fi

while true; do
    echo "[$(date)] --- Executing $PYTHON_SCRIPT ---"
    
    # Run the script
    python3 "$PYTHON_SCRIPT"
    
    echo "[$(date)] --- Finished. Sleeping for $INTERVAL seconds ---"
    sleep "$INTERVAL"
done