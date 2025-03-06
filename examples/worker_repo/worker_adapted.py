#!/usr/bin/env python3
"""
Example worker adapted to use the cloud task adapter.

This demonstrates how to use the cloud_tasks module to adapt any
worker code to run in a cloud environment.
"""
import asyncio
import json
import logging
import os
from typing import Any, Dict, Tuple

# Import the cloud task adapter
from cloud_tasks.worker import run_cloud_worker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Output directory for results
OUTPUT_DIR = "results"


async def process_task(task_id: str, task_data: Dict[str, Any]) -> Tuple[bool, Any]:
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
            logger.error(f"Task {task_id}: Missing required parameters num1 or num2")
            return False, "Missing required parameters"

        # Perform the addition
        result = num1 + num2
        logger.info(f"Task {task_id}: {num1} + {num2} = {result}")

        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Write the result to a file
        output_file = os.path.join(OUTPUT_DIR, f"{task_id}.out")
        with open(output_file, 'w') as f:
            f.write(str(result))

        logger.info(f"Task {task_id}: Result written to {output_file}")
        return True, result

    except Exception as e:
        logger.error(f"Task {task_id}: Error processing task: {e}")
        return False, str(e)


if __name__ == "__main__":
    """
    Main entry point that connects the worker with cloud task processing.

    The cloud_tasks.worker.run_cloud_worker function handles:
    1. Reading configuration from the command line or arguments
    2. Setting up cloud provider connections
    3. Monitoring for spot/preemptible instance termination
    4. Receiving tasks from the queue
    5. Processing tasks using the provided function
    6. Handling retries and reporting results
    7. Uploading results to cloud storage (if configured)
    """
    asyncio.run(run_cloud_worker(process_task))