import ee
from concurrent.futures import ThreadPoolExecutor

# Initialize
ee.Initialize(project="fire-detection-uruguay")
#ee.Initialize(project="wildfires-479718")

def cancel_task(task):
    """Function to handle individual task cancellation."""
    task_name = task.get('name')
    # Use .get() to avoid KeyErrors if the structure varies
    state = task.get('metadata', {}).get('state')
    
    if state == 'PENDING':
        try:
            ee.data.cancelOperation(task_name)
            return True
        except Exception as e:
            # Silencing 'already cancelled' errors if they occur
            return False
    return False

print("Fetching active tasks...")
# Removed the 'filter' argument that caused the TypeError
tasks = ee.data.listOperations() 

if not tasks:
    print("No tasks found.")
else:
    # Filter the list locally before passing to the executor to save overhead
    pending_tasks = [t for t in tasks if t.get('metadata', {}).get('state') == 'PENDING']
    
    if not pending_tasks:
        print("No PENDING tasks to cancel.")
    else:
        print(f"Found {len(pending_tasks)} pending tasks. Starting parallel cancellation...")
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(cancel_task, pending_tasks))

        cancelled_count = sum(1 for r in results if r)
        print(f"--- Finished ---")
        print(f"Successfully cancelled {cancelled_count} tasks.")