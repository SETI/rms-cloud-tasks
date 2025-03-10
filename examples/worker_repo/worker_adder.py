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
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S.%f'  # Explicitly use period for fractions
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
        try:
            os.makedirs(OUTPUT_DIR, exist_ok=True)
        except OSError as e:
            logger.error(f"Task {task_id}: Failed to create output directory {OUTPUT_DIR}: {e}")
            return False

        # Write the result to a file
        output_file = os.path.join(OUTPUT_DIR, f"{task_id}.out")
        try:
            with open(output_file, 'w') as f:
                f.write(str(result))
        except IOError as e:
            logger.error(f"Task {task_id}: Failed to write result to {output_file}: {e}")
            return False

        logger.info(f"Task {task_id}: Result written to {output_file}")
        return True

    except Exception as e:
        logger.error(f"Task {task_id}: Error processing task: {e}", exc_info=True)
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
        # Validate the config file exists
        if not os.path.exists(config_path):
            logger.error(f"Config file not found: {config_path}")
            sys.exit(1)

        # Load the configuration
        try:
            with open(config_path, 'r') as f:
                try:
                    config = json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse config file {config_path}: {e}")
                    logger.error(f"Error at line {e.lineno}, column {e.colno}: {e.msg}")
                    sys.exit(1)
        except IOError as e:
            logger.error(f"Error reading config file {config_path}: {e}")
            sys.exit(1)

        logger.info(f"Worker starting with config from: {config_path}")

        # In a real environment, this would connect to the queue
        # and continuously process tasks until there are none left

        # Simulate task processing for demonstration purposes
        # (in reality, this would come from the queue)
        if "sample_task" in config:
            try:
                task_id = config["sample_task"].get("id", "sample-task")
                task_data = config["sample_task"].get("data", {})

                if not task_data:
                    logger.warning(f"Sample task in {config_path} has empty data")

                success = await process_task(task_id, task_data)
                logger.info(f"Task {task_id} processed {'successfully' if success else 'with errors'}")
            except KeyError as e:
                logger.error(f"Missing required key in sample task configuration from {config_path}: {e}")
                sys.exit(1)
            except Exception as e:
                logger.error(f"Error processing sample task from {config_path}: {e}")
                logger.debug(traceback.format_exc())
                sys.exit(1)
        else:
            logger.info(f"No sample task found in config {config_path}")

    except Exception as e:
        logger.error(f"Unexpected worker error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())