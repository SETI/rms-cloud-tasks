#!/usr/bin/env python3
"""
Example worker adapted to use the cloud task adapter with multiprocessing.

This demonstrates how to use the cloud_tasks module to adapt any
worker code to run in a cloud environment with true parallel processing.
"""
import asyncio
import json
import logging
import os
import multiprocessing
import argparse
import sys
import traceback
from typing import Any, Dict, Tuple

# Import the cloud task adapter
from cloud_tasks.worker import run_cloud_worker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S.%f'  # Explicitly use period for fractions
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
        # Log the process ID to demonstrate parallel execution
        process_id = os.getpid()
        worker_id = multiprocessing.current_process().name
        logger.info(f"Processing task {task_id} in process {process_id} ({worker_id})")

        # Extract the two numbers from the task data
        num1 = task_data.get("num1")
        num2 = task_data.get("num2")

        if num1 is None or num2 is None:
            logger.error(f"Task {task_id}: Missing required parameters num1 or num2")
            return False, "Missing required parameters"

        # Simulate some CPU-intensive work
        # This will benefit from running in parallel
        result = 0
        for i in range(10000000):  # Adjust this number based on your machine's speed
            if i % 1000000 == 0:
                logger.info(f"Task {task_id}: Processing iteration {i//1000000}/10...")
            result = num1 + num2

        logger.info(f"Task {task_id}: {num1} + {num2} = {result}")

        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Write the result to a file
        output_file = os.path.join(OUTPUT_DIR, f"{task_id}.out")
        with open(output_file, 'w') as f:
            f.write(f"{result} (processed by PID {process_id})")

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
    8. Running tasks in parallel across multiple processes
    """
    # Parse arguments
    parser = argparse.ArgumentParser(description="Cloud-adapted worker")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--workers", type=int, help="Number of worker processes (default: auto)")
    args = parser.parse_args()

    # Verify config file exists
    if not os.path.exists(args.config):
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)

    try:
        # Try to load the configuration
        try:
            with open(args.config, 'r') as f:
                try:
                    config = json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse config file {args.config}: {e}")
                    logger.error(f"Error at line {e.lineno}, column {e.colno}: {e.msg}")
                    sys.exit(1)
        except IOError as e:
            logger.error(f"Error reading config file {args.config}: {e}")
            sys.exit(1)

        logger.info(f"Starting worker with config from {args.config}")

        # Run the worker with the cloud adapter
        try:
            workers = args.workers or config.get("worker_options", {}).get("num_workers")
            asyncio.run(run_cloud_worker(process_task, config_file=args.config, num_workers=workers))
        except KeyError as e:
            logger.error(f"Missing required key in configuration from {args.config}: {e}")
            sys.exit(1)
        except ValueError as e:
            logger.error(f"Invalid configuration value in {args.config}: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error initializing worker from config {args.config}: {e}")
            logger.debug(traceback.format_exc())
            sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error in worker: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)