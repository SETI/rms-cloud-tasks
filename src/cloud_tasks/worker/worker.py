"""
Worker module for processing tasks from queues.

This module runs on worker instances and processes tasks from the queue.
"""
import argparse
import asyncio
import importlib
import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Set, Awaitable, Callable

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Worker:
    """Worker class for processing tasks from queues."""

    def __init__(self, config_file: str):
        """
        Initialize the worker with configuration.

        Args:
            config_file: Path to JSON configuration file
        """
        self.config_file = config_file
        self.config = self._load_config()

        self.provider = self.config['provider']
        self.queue_name = self.config['queue_name']
        self.job_id = self.config['job_id']
        self.tasks_per_worker = self.config.get('tasks_per_worker', 10)
        self.provider_config = self.config['config']

        self.running = False
        self.task_queue: Optional[Any] = None
        self.worker_tasks: Set[asyncio.Task[Any]] = set()
        self.shutdown_event = asyncio.Event()
        self.tasks_processed = 0
        self.tasks_failed = 0

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Flag to check if instance is marked for termination
        # (e.g., when running on spot/preemptible instances)
        self._termination_requested = False
        self._last_termination_check: float = 0.0
        self._termination_check_interval = 5  # seconds

    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from JSON file.

        Returns:
            Configuration dictionary
        """
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)

            # Validate required fields
            required_fields = ['provider', 'queue_name', 'job_id', 'config']
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"Missing required field in config: {field}")

            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            sys.exit(1)

    def _signal_handler(self, signum, frame):
        """Handle termination signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self._termination_requested = True
        # Use asyncio to signal the main event loop
        if not self.shutdown_event.is_set():
            asyncio.get_event_loop().call_soon_threadsafe(self.shutdown_event.set)

    async def initialize(self) -> None:
        """Initialize the worker and connect to the queue."""
        # Import and initialize the task queue
        from cloud_tasks.queue_manager import create_queue

        task_queue = await create_queue(
            provider=self.provider,
            queue_name=self.queue_name,
            config=self.provider_config
        )
        self.task_queue = task_queue

        # Verify queue connection
        try:
            if self.task_queue is not None:
                depth = await self.task_queue.get_queue_depth()
                logger.info(f"Connected to queue {self.queue_name}, current depth: {depth}")
            else:
                raise ValueError("Failed to initialize task queue")
        except Exception as e:
            logger.error(f"Failed to connect to queue: {e}")
            sys.exit(1)

    async def start(self) -> None:
        """Start the worker processing loop."""
        if self.running:
            logger.warning("Worker is already running")
            return

        self.running = True

        # Start background task to check for termination requests
        asyncio.create_task(self._check_termination_loop())

        logger.info(f"Starting worker for job {self.job_id} with {self.tasks_per_worker} worker tasks")

        # Start the main processing loop
        await self._processing_loop()

        # Wait for all worker tasks to complete during shutdown
        if self.worker_tasks:
            logger.info(f"Waiting for {len(self.worker_tasks)} tasks to complete")
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)

        logger.info(f"Worker shutdown complete. Processed: {self.tasks_processed}, Failed: {self.tasks_failed}")

    async def _processing_loop(self) -> None:
        """Main processing loop that manages worker tasks."""
        while self.running:
            # Check if shutdown has been requested
            if self.shutdown_event.is_set():
                logger.info("Shutdown event detected, stopping processing loop")
                self.running = False
                break

            # Clean up completed worker tasks
            self._cleanup_worker_tasks()

            # Check if we can start more worker tasks
            available_slots = self.tasks_per_worker - len(self.worker_tasks)

            if available_slots > 0 and not self._termination_requested:
                # Start new worker tasks
                for _ in range(available_slots):
                    task = asyncio.create_task(self._process_task())
                    self.worker_tasks.add(task)
                    # Add callback to remove the task when it's done
                    task.add_done_callback(self.worker_tasks.discard)

            # Sleep briefly to avoid hammering the CPU
            await asyncio.sleep(0.1)

    def _cleanup_worker_tasks(self) -> None:
        """Clean up completed or cancelled worker tasks."""
        to_remove = set()
        for task in self.worker_tasks:
            if task.done():
                # Handle any exceptions
                if task.exception():
                    logger.error(f"Worker task failed with error: {task.exception()}")
                to_remove.add(task)

        # Remove completed tasks
        self.worker_tasks -= to_remove

    async def _process_task(self) -> None:
        """
        Process a single task from the queue.

        This method gets a task from the queue, processes it, and marks it as complete.
        """
        # Check if termination has been requested
        if self._termination_requested:
            return

        try:
            # Ensure task_queue is available
            if self.task_queue is None:
                logger.error("Task queue not initialized")
                return

            # Receive tasks (just one for simplicity)
            tasks = await self.task_queue.receive_tasks(max_count=1)

            if not tasks:
                # If no tasks available, sleep briefly to avoid hammering the queue
                await asyncio.sleep(1)
                return

            task = tasks[0]
            task_id = task['task_id']
            task_data = task['data']
            receipt = task.get('receipt_handle') or task.get('lock_token') or task.get('ack_id')

            logger.info(f"Processing task {task_id}")

            # Process the task
            success, result = await self._execute_task(task_id, task_data)

            # Mark task as complete or failed
            if success:
                await self.task_queue.complete_task(receipt)
                self.tasks_processed += 1
                logger.info(f"Task {task_id} completed successfully")
            else:
                await self.task_queue.fail_task(receipt)
                self.tasks_failed += 1
                logger.error(f"Task {task_id} failed: {result}")

        except Exception as e:
            logger.error(f"Error processing task: {e}")
            self.tasks_failed += 1

    async def _execute_task(self, task_id: str, task_data: Dict[str, Any]) -> Tuple[bool, Any]:
        """
        Execute a task with the provided data.

        Args:
            task_id: ID of the task
            task_data: Data associated with the task

        Returns:
            Tuple of (success, result)
        """
        try:
            # In a real implementation, this would execute the task based on task_data
            # For simplicity, we'll just simulate processing time proportional to task "size"
            size = task_data.get('size', 1)
            await asyncio.sleep(size)

            # Simulate random failure (10% chance)
            if task_data.get('should_fail', False):
                return False, "Task was configured to fail"

            # Return success
            return True, {"status": "completed", "task_id": task_id}

        except Exception as e:
            return False, str(e)

    async def _check_termination_loop(self) -> None:
        """Periodically check if the instance is marked for termination."""
        while self.running and not self.shutdown_event.is_set():
            try:
                # Only check periodically to reduce API calls
                current_time = time.time()
                if current_time - self._last_termination_check > self._termination_check_interval:
                    self._last_termination_check = current_time

                    # Check if instance is marked for termination
                    if await self._check_termination_notice():
                        logger.warning("Termination notice detected, initiating graceful shutdown")
                        self._termination_requested = True
                        self.shutdown_event.set()

            except Exception as e:
                logger.error(f"Error checking termination notice: {e}")

            # Sleep before checking again
            await asyncio.sleep(1)

    async def _check_termination_notice(self) -> bool:
        """
        Check if the instance is marked for termination.

        This is cloud provider specific. On AWS, you would check the
        spot instance termination notice. On GCP, you would check the
        preemption notice.

        Returns:
            True if the instance is marked for termination, False otherwise
        """
        # This is a simplified implementation
        # In a real implementation, this would check the cloud provider API

        if self.provider == 'aws':
            # AWS spot instance termination check
            # In reality, would make an HTTP request to http://169.254.169.254/latest/meta-data/spot/instance-action
            return False

        elif self.provider == 'gcp':
            # GCP preemption check
            # In reality, would make an HTTP request to http://metadata.google.internal/computeMetadata/v1/instance/preempted
            return False

        elif self.provider == 'azure':
            # Azure doesn't have a direct equivalent, but could check other signals
            return False

        # Default to no termination notice
        return False


async def main():
    """Main entry point for the worker."""
    parser = argparse.ArgumentParser(description='Cloud Tasks Worker')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    args = parser.parse_args()

    worker = Worker(args.config)
    await worker.initialize()
    await worker.start()


if __name__ == '__main__':
    asyncio.run(main())