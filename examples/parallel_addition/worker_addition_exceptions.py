"""
Example worker adapted to use the cloud task adapter with multiprocessing.

This demonstrates how to use the cloud_tasks module to adapt any
worker code to run in a cloud environment with true parallel processing.

This version allows variable delays and probabilistic exceptions, timeouts, and
premature exits.
"""

import asyncio
import os
import multiprocessing
import random
import socket
import sys
import time
from typing import Any, Dict, Tuple

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

    exception_prob = float(os.getenv("ADDITION_EXCEPTION_PROBABILITY", 0))
    if random.random() < exception_prob:
        _ = 1 / 0

    timeout_prob = float(os.getenv("ADDITION_TIMEOUT_PROBABILITY", 0))
    if random.random() < timeout_prob:
        time.sleep(100000)

    exit_prob = float(os.getenv("ADDITION_EXIT_PROBABILITY", 0))
    if random.random() < exit_prob:
        sys.exit(2)

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


async def main():
    worker = Worker(process_task, args=sys.argv[1:])
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
