#!/usr/bin/env python3
"""
Example worker adapted to use the cloud task adapter with multiprocessing.

This demonstrates how to use the cloud_tasks module to adapt any
worker code to run in a cloud environment with true parallel processing.
"""
import asyncio
import logging
import os
import multiprocessing
import socket
import time
from typing import Any, Dict, Tuple

# Import the cloud task adapter
from cloud_tasks.worker import Worker

from filecache import FCPath


def process_task(task_id: str, task_data: Dict[str, Any]) -> Tuple[bool, Any]:
    """
    Process a task by adding two numbers together.

    This is the worker-specific logic that processes a task.
    It simply adds two numbers together and writes the result to a file.

    Args:
        task_id: Unique identifier for the task
        task_data: Task data containing the numbers to add

    Returns:
        Tuple of (success, result)
    """
    try:
        # Extract the two numbers from the task data
        num1 = task_data.get("num1")
        num2 = task_data.get("num2")

        if num1 is None or num2 is None:
            # We still return True because we don't want to retry the task
            return True, "Missing required parameters"

        result = num1 + num2

        output_dir = FCPath(os.getenv("ADDITION_OUTPUT_DIR", "results"))
        output_file = output_dir / f"{task_id}.txt"

        with output_file.open(mode="w") as f:
            process_id = os.getpid()
            hostname = socket.gethostname()
            worker_id = multiprocessing.current_process().name
            f.write(f"Process {process_id} on {hostname} ({worker_id})\n")
            f.write(f"Task {task_id}: {num1} + {num2} = {result}\n")

        task_delay = os.getenv("ADDITION_TASK_DELAY")
        if task_delay is not None:
            delay = float(task_delay)
            time.sleep(delay)

        return True, output_file

    except Exception as e:
        # We still return True because we don't want to retry the task
        return False, str(e)


async def main():
    worker = Worker(process_task)
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
