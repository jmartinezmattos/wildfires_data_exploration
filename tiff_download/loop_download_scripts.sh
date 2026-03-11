#!/bin/bash

SCRIPT_1="collect_no_fire_bands.py"
SCRIPT_2="collect_fire_bands_joaco.py"
CHECK_INTERVAL=3000

echo "---- DEBUG INFO ----"
echo "Python path: $(which python)"
echo "Python3 path: $(which python3)"
echo "Pip path: $(which pip)"
echo "Python version: $(python --version)"
echo "--------------------"


echo "Starting Watchdog for $SCRIPT_1 and $SCRIPT_2..."

while true; do
    # --- RUN SCRIPT 1 ---
    echo "Running $SCRIPT_1..."
    python3 "$SCRIPT_1"
    STATUS_1=$?

    # --- RUN SCRIPT 2 ---
    echo "Running $SCRIPT_2..."
    python3 "$SCRIPT_2"
    STATUS_2=$?

    # --- LOGIC CHECK ---

    # If BOTH scripts report they are finished (Exit Code 10)
    if [ $STATUS_1 -eq 10 ] && [ $STATUS_2 -eq 10 ]; then
        echo "Both scripts reported completion. Exiting."
        break
    
    # If both finished a batch successfully (Exit Code 0)
    elif [ $STATUS_1 -eq 0 ] && [ $STATUS_2 -eq 0 ]; then
        echo "Both batches complete. Waiting $CHECK_INTERVAL seconds..."
        sleep $CHECK_INTERVAL

    # If something crashed (Non-zero, non-10)
    else
        echo "One or more scripts reported an error or is still working."
        echo "Status 1: $STATUS_1 | Status 2: $STATUS_2"
        echo "Retrying in $CHECK_INTERVAL..."
        sleep $CHECK_INTERVAL
    fi
done