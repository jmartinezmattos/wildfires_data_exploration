#!/bin/bash

PYTHON_SCRIPT="collect_fire_bands.py"
QUEUE_LIMIT=2500
CHECK_INTERVAL=600

#echo "Starting, waiting"
#sleep 4000

while true; do

    python3 "$PYTHON_SCRIPT"
        
    # Capture the exit code immediately after the command
    STATUS=$?

    if [ $STATUS -eq 10 ]; then
        echo "Python reported everything is finished. Exiting Watchdog."
        break # This breaks the 'while true' loop
    elif [ $STATUS -eq 0 ]; then
        echo "Batch complete. Waiting for queue to clear..."
        sleep $CHECK_INTERVAL
    else
        echo "Script crashed with error $STATUS. Retrying in $CHECK_INTERVAL..."
        sleep $CHECK_INTERVAL
    fi

done