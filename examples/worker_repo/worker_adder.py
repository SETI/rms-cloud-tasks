#!/usr/bin/env python3
"""
Number adder worker - processes tasks from a queue.
Each task will add two numbers together and write the result to a file.
"""
import asyncio
import json
import logging
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Output directory for results
OUTPUT_DIR = "results"


async def process_task(task_id, task_data):
    """
    Process a task by adding two numbers together.

    Args:
        task_id: Unique identifier for the task
        task_data: Task data containing the numbers to add

    Returns:
        True if successful, False otherwise
    """
    try:
        # Extract the two numbers from the task data
        num1 = task_data.get("num1")
        num2 = task_data.get("num2")

        if num1 is None or num2 is None:
            logger.error(f"Task {task_id}: Missing required parameters num1 or num2")
            return False

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
        return True

    except Exception as e:
        logger.error(f"Task {task_id}: Error processing task: {e}")
        return False


async def main():
    """
    Main worker function that runs when the worker starts.
    In a real implementation, this would connect to the queue and process tasks.
    """
    # In a production environment, this would use the cloud_tasks library
    # to poll the queue. For demonstration, we'll just read from config

    if len(sys.argv) < 2:
        logger.error("Please provide a config file path (--config=path/to/config.json)")
        sys.exit(1)

    config_path = None
    for arg in sys.argv:
        if arg.startswith("--config="):
            config_path = arg.split("=")[1]

    if not config_path:
        logger.error("Please provide a config file path (--config=path/to/config.json)")
        sys.exit(1)

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        logger.info(f"Worker starting with config: {config}")

        # In a real environment, this would connect to the queue
        # and continuously process tasks until there are none left

        # Simulate task processing for demonstration purposes
        # (in reality, this would come from the queue)
        if "sample_task" in config:
            task_id = config["sample_task"].get("id", "sample-task")
            task_data = config["sample_task"].get("data", {})

            success = await process_task(task_id, task_data)
            logger.info(f"Task {task_id} processed {'successfully' if success else 'with errors'}")

    except Exception as e:
        logger.error(f"Worker error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())