# Generate a JSON file with a list of tasks to be processed in parallel
# Format of a task:
#   {
#     "task_id": "parallel-task-000001",
#     "data": {
#       "num1": 42,
#       "num2": 58
#     }
#   },
# num1 and num2 are random numbers between -99999 and 99999

import json
import random

tasks = []
for i in range(10000):
    task = {
        "task_id": f"addition-task-{i+1:06d}",
        "data": {"num1": random.randint(-99999, 99999), "num2": random.randint(-99999, 99999)},
    }
    tasks.append(task)

with open("addition_tasks.json", "w") as f:
    json.dump(tasks, f, indent=2)
