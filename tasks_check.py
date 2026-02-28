from datetime import datetime

import ee
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

# Initialize
ee.Initialize(project="fire-detection-uruguay")
#ee.Initialize(project="wildfires-479718")

def get_task_state(task):
    """Simple helper to extract state from a task dictionary."""
    return task.get('metadata', {}).get('state', 'UNKNOWN')

def main():
    print("Connecting to Earth Engine...")
    start_time = time.perf_counter()
    print(f"STARTING AT {datetime.now()}\n")

    # 1. Fetch the raw operations list
    # Note: The 'fetch' itself is a single API call, 
    # but we will process the resulting list in parallel.
    tasks = ee.data.listOperations()

    if not tasks:
        print("No tasks found.")
        return

    # 2. Use ThreadPoolExecutor to process the list 
    # (Useful if the list is massive or if doing extra metadata parsing)
    with ThreadPoolExecutor(max_workers=10) as executor:
        states = list(executor.map(get_task_state, tasks))

    # 3. Tally the results
    stats = Counter(states)
    end_time = time.perf_counter()

    # 4. Output results
    print("\n" + "="*25)
    print("   GEE TASK SUMMARY")
    print("="*25)
    
    for state, count in stats.items():
        print(f"{state:.<20} {count}")
    
    print("-" * 25)
    print(f"{'TOTAL TASKS':.<20} {len(states)}")
    print(f"{'TIME TAKEN':.<20} {end_time - start_time:.2f} seconds")
    print("="*25)

if __name__ == "__main__":
    main()