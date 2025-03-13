"""
Test queue manager functionality across providers.
"""
import asyncio
import logging
import time
import uuid
import pytest
import yaml

from cloud_tasks.queue_manager import create_queue
from cloud_tasks.common.logging_config import configure_logging

# Skip the entire module as it's been superseded by newer tests
pytestmark = pytest.mark.skip(reason="This test has been superseded by more modern tests in queue_manager/")

@pytest.mark.asyncio
async def test_queue_functionality(config_file, provider, queue_name):
    """
    Test the full queue functionality including:
    - Queue creation
    - Sending tasks
    - Getting queue depth
    - Receiving tasks
    - Completing tasks
    """
    # Configure logging with millisecond precision
    configure_logging(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info(f"Testing queue functionality for provider: {provider}, queue: {queue_name}")

    # Load configuration
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_file}")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return

    # Create the queue
    try:
        logger.info(f"Creating queue: {queue_name}")
        queue = await create_queue(
            provider=provider,
            queue_name=queue_name,
            config=config
        )
        logger.info(f"Queue created successfully: {queue}")
    except Exception as e:
        logger.error(f"Failed to create queue: {e}")
        return

    # Check initial queue depth
    try:
        depth = await queue.get_queue_depth()
        logger.info(f"Initial queue depth: {depth}")
    except Exception as e:
        logger.error(f"Failed to get queue depth: {e}")
        return

    # Send test tasks
    num_tasks = 5
    logger.info(f"Sending {num_tasks} test tasks")
    task_ids = []

    for i in range(num_tasks):
        task_id = f"test-task-{uuid.uuid4()}"
        task_ids.append(task_id)
        task_data = {
            "task_type": "test",
            "value": i,
            "timestamp": time.time()
        }

        try:
            await queue.send_task(task_id, task_data)
            logger.info(f"Sent task {i+1}/{num_tasks}: {task_id}")
        except Exception as e:
            logger.error(f"Failed to send task {i+1}: {e}")

    # Check queue depth after sending
    try:
        # Wait a bit for messages to be delivered
        logger.info("Waiting 5 seconds for messages to be delivered...")
        await asyncio.sleep(5)

        depth = await queue.get_queue_depth()
        logger.info(f"Queue depth after sending tasks: {depth}")

        if depth < num_tasks:
            logger.warning(f"Queue depth ({depth}) is less than the number of tasks sent ({num_tasks})")
    except Exception as e:
        logger.error(f"Failed to get queue depth after sending: {e}")

    # Receive and process tasks
    try:
        logger.info("Receiving tasks")
        tasks = await queue.receive_tasks(max_count=num_tasks)
        logger.info(f"Received {len(tasks)} tasks")

        # Print task details
        for i, task in enumerate(tasks):
            logger.info(f"Task {i+1}: id={task['task_id']}, data={task['data']}")

            # Complete task
            await queue.complete_task(task.get('ack_id') or task.get('receipt_handle') or task.get('lock_token'))
            logger.info(f"Completed task {i+1}")

    except Exception as e:
        logger.error(f"Error receiving or processing tasks: {e}")

    # Check final queue depth
    try:
        # Wait a bit for message deletions to be processed
        logger.info("Waiting 5 seconds for task completions to be processed...")
        await asyncio.sleep(5)

        depth = await queue.get_queue_depth()
        logger.info(f"Final queue depth: {depth}")
    except Exception as e:
        logger.error(f"Failed to get final queue depth: {e}")

    logger.info("Queue test completed")


def main():
    parser = argparse.ArgumentParser(description='Test queue functionality')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--provider', choices=['aws', 'gcp', 'azure'], required=True, help='Cloud provider')
    parser.add_argument('--queue-name', required=True, help='Name of the queue to test')

    args = parser.parse_args()

    # Run the test
    asyncio.run(test_queue_functionality(args.config, args.provider, args.queue_name))


if __name__ == "__main__":
    main()