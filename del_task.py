import ee

# Initialize with your specific project
ee.Initialize(project="fire-detection-uruguay")
delete_count = 0
tasks = ee.data.listOperations()
for task in tasks:
    if task['metadata']['state'] == 'READY':
        print(f"Cancelling task: {task['name']}")
        ee.data.cancelOperation(task['name'])
        delete_count += 1
print(f"Cancelled {delete_count} tasks.")