"""
Example worker adapted to use the cloud task adapter with multiprocessing.

This demonstrates how to use the cloud_tasks module to adapt any
worker code to run in a cloud environment with true parallel processing.

This version allow variable delays and uses a task factory function to generate
tasks instead of an external task file or queue.
"""

import asyncio
import os
import multiprocessing
import random
import socket
import sys
import time
from typing import Any, Dict, Iterable, Tuple

# Import the cloud task adapter
from cloud_tasks.worker import Worker, WorkerData

from filecache import FCPath


def process_task(
    task_id: str, task_data: Dict[str, Any], worker_data: WorkerData
) -> Tuple[bool, Any]:
    """
    Process a task by adding two numbers together.

    This is the worker-specific logic that processes a task.
    It simply adds two numbers together and writes the result to a file.

    Args:
        task_id: Unique identifier for the task
        task_data: Task data containing the numbers to add
        worker_data: WorkerData object (useful for retrieving information about the
            local environment and polling for shutdown notifications)

    Returns:
        Tuple of (retry, result)
    """
    # Extract the two numbers from the task data
    num1 = task_data.get("num1")
    num2 = task_data.get("num2")

    if num1 is None or num2 is None:
        # We return False because we don't want to retry the task
        return False, "Missing required parameters"

    result = num1 + num2

    task_delay = os.getenv("ADDITION_TASK_DELAY")
    if task_delay is not None:
        delay = float(task_delay)
        if delay < 0:  # Randomize
            delay = random.uniform(0, abs(delay))
        time.sleep(delay)

    output_dir = FCPath(os.getenv("ADDITION_OUTPUT_DIR", "results"))
    output_file = output_dir / f"{task_id}.txt"
    with output_file.open(mode="w") as f:
        process_id = os.getpid()
        hostname = socket.gethostname()
        worker_id = multiprocessing.current_process().name
        f.write(f"Process {process_id} on {hostname} ({worker_id})\n")
        f.write(f"Task {task_id}: {num1} + {num2} = {result}\n")

        if worker_data.received_termination_notice:
            f.write("*** Received spot termination signal ***\n")

    return False, str(output_file)


def task_factory() -> Iterable[Dict[str, Any]]:
    """Generate a series of tasks."""
    max_tasks = int(os.getenv("ADDITION_MAX_TASKS", "10000"))
    for task_num in range(max_tasks):
        yield {
            "task_id": f"factory-task-{task_num:06d}",
            "data": {"num1": random.randint(0, 1000000), "num2": random.randint(0, 1000000)},
        }


async def main():
    worker = Worker(process_task, args=sys.argv[1:], task_source=task_factory)
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
