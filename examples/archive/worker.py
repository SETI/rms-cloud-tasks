#!/usr/bin/env python3
"""
Main worker entry point for cloud task processing.
This file integrates with the cloud_tasks framework and routes tasks to the adder implementation.
"""
import asyncio
import json
import logging
import os
import sys
import traceback
from worker_adder import process_task

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S.%f'  # Explicitly use period for fractions
)
logger = logging.getLogger(__name__)


async def handle_task(task_id, task_data):
    """
    Process a task from the queue.

    Args:
        task_id: Unique identifier for the task
        task_data: Task data containing parameters

    Returns:
        True if processing succeeded, False otherwise
    """
    logger.info(f"Processing task {task_id}")
    return await process_task(task_id, task_data)


async def main():
    """
    Main worker entrypoint.
    In a production environment, this would:
    1. Connect to the cloud provider's task queue
    2. Poll for tasks
    3. Process tasks as they arrive
    4. Handle graceful shutdown for spot/preemptible instances
    """
    if len(sys.argv) < 2:
        logger.error("Please provide a config file path (--config=/path/to/config.json)")
        sys.exit(1)

    config_path = None
    for arg in sys.argv:
        if arg.startswith("--config="):
            config_path = arg.split("=")[1]

    if not config_path:
        logger.error("Please provide a config file path (--config=/path/to/config.json)")
        sys.exit(1)

    try:
        # Validate config file exists
        if not os.path.exists(config_path):
            logger.error(f"Config file not found: {config_path}")
            sys.exit(1)

        # Load configuration
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

        logger.info(f"Starting worker with config from {config_path}")

        # In a real implementation, this would use the cloud_tasks library
        # to poll the queue for tasks. For demonstration purposes, we'll
        # use a sample task if provided in the config.

        if "sample_task" in config:
            try:
                task_id = config["sample_task"].get("id", "sample-task")
                task_data = config["sample_task"].get("data", {})

                if not task_data:
                    logger.warning(f"Sample task in {config_path} has empty data")

                success = await handle_task(task_id, task_data)
                logger.info(f"Task {task_id} processed {'successfully' if success else 'with errors'}")

                # In a real worker, we would keep polling for more tasks until
                # the queue is empty or the instance is scheduled for termination
            except KeyError as e:
                logger.error(f"Missing required key in sample task configuration: {e}")
                sys.exit(1)
            except Exception as e:
                logger.error(f"Error processing sample task from {config_path}: {e}")
                logger.debug(traceback.format_exc())
                sys.exit(1)
        else:
            logger.info("No sample task in config, would poll queue in production")

    except Exception as e:
        logger.error(f"Unexpected error in worker: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())