#!/usr/bin/env python3
"""
Cloud-native number adder worker - processes tasks from cloud provider queues.
Enhanced with:
- Direct cloud queue access
- Spot/preemptible instance termination handling
- Uploading results to cloud storage
- Resilient error handling with retries
"""
import asyncio
import aiohttp
import aiofiles
import json
import logging
import os
import sys
import time
import boto3
import requests
from typing import Dict, Any, Optional, Tuple, List
from urllib.parse import urlparse
from google.cloud import pubsub_v1, storage
from azure.servicebus.aio import ServiceBusClient
from azure.storage.blob.aio import BlobServiceClient
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S.%f'  # Explicitly use period for fractions
)
logger = logging.getLogger(__name__)

# Output directory for results (local)
OUTPUT_DIR = "results"

# Max retries for task processing
MAX_RETRIES = 3

# Termination check interval in seconds
TERMINATION_CHECK_INTERVAL = 5


class CloudWorker:
    """
    Cloud-native worker that connects directly to cloud queues,
    handles spot/preemptible instance termination, and uploads
    results to cloud storage.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize cloud worker with configuration.

        Args:
            config: Configuration dictionary containing provider details,
                   credentials, and runtime settings
        """
        self.config = config
        self.provider = config.get('provider', '').lower()
        self.job_id = config.get('job_id', 'unknown')
        self.queue_name = config.get('queue_name', '')
        self.result_bucket = config.get('result_bucket', '')
        self.result_prefix = config.get('result_prefix', '')

        # Initialize cloud-specific clients
        self.queue_client = None
        self.storage_client = None

        # Termination flags
        self.running = False
        self.termination_requested = False
        self.shutdown_event = asyncio.Event()

        # Task metrics
        self.tasks_processed = 0
        self.tasks_failed = 0

        # Validate config
        if not self.provider:
            raise ValueError("Provider must be specified (aws, gcp, or azure)")
        if not self.queue_name:
            raise ValueError("Queue name must be specified")

        # Configure provider-specific details
        self._configure_provider()

    def _configure_provider(self):
        """Configure provider-specific clients and settings."""
        if self.provider == 'aws':
            self._configure_aws()
        elif self.provider == 'gcp':
            self._configure_gcp()
        elif self.provider == 'azure':
            self._configure_azure()
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")

    def _configure_aws(self):
        """Configure AWS-specific clients and settings."""
        aws_config = self.config.get('config', {})
        region = aws_config.get('region', 'us-east-1')
        access_key = aws_config.get('access_key')
        secret_key = aws_config.get('secret_key')

        # Create SQS client for queue access
        self.queue_client = boto3.client(
            'sqs',
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        # Create S3 client for results storage
        self.storage_client = boto3.client(
            's3',
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        # Get queue URL
        response = self.queue_client.get_queue_url(QueueName=self.queue_name)
        self.queue_url = response['QueueUrl']

        logger.info(f"Configured AWS worker for queue {self.queue_name}")

    def _configure_gcp(self):
        """Configure GCP-specific clients and settings."""
        gcp_config = self.config.get('config', {})
        self.project_id = gcp_config.get('project_id')

        if not self.project_id:
            raise ValueError("GCP project_id is required")

        # Create Pub/Sub client for queue access
        self.queue_client = pubsub_v1.SubscriberClient()

        # Create Storage client for results
        self.storage_client = storage.Client(project=self.project_id)

        # Set subscription path
        self.subscription_path = self.queue_client.subscription_path(
            self.project_id, f"{self.queue_name}-subscription"
        )

        logger.info(f"Configured GCP worker for subscription {self.subscription_path}")

    def _configure_azure(self):
        """Configure Azure-specific clients and settings."""
        azure_config = self.config.get('config', {})
        connection_string = azure_config.get('connection_string')

        if not connection_string:
            # Build connection string from parts if not provided directly
            account_name = azure_config.get('storage_account_name')
            account_key = azure_config.get('storage_account_key')

            if not (account_name and account_key):
                raise ValueError("Azure requires either connection_string or storage account credentials")

            connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

        # Create Service Bus client for queue access
        self.queue_client = ServiceBusClient.from_connection_string(connection_string)

        # Create Blob Storage client for results
        self.storage_client = BlobServiceClient.from_connection_string(connection_string)

        logger.info(f"Configured Azure worker for queue {self.queue_name}")

    async def process_task(self, task_id: str, task_data: Dict[str, Any]) -> Tuple[bool, Any]:
        """
        Process a task by adding two numbers together.

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

            # Write result to local file
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            output_file = os.path.join(OUTPUT_DIR, f"{task_id}.out")

            async with aiofiles.open(output_file, 'w') as f:
                await f.write(str(result))

            # Upload result to cloud storage
            success = await self.upload_result(task_id, result)
            if success:
                logger.info(f"Task {task_id}: Result uploaded to cloud storage")
            else:
                logger.warning(f"Task {task_id}: Failed to upload result to cloud storage")

            return True, result

        except Exception as e:
            logger.error(f"Task {task_id}: Error processing task: {e}")
            return False, str(e)

    async def upload_result(self, task_id: str, result: Any) -> bool:
        """
        Upload the task result to cloud storage.

        Args:
            task_id: Unique identifier for the task
            result: Result data to upload

        Returns:
            True if upload succeeded, False otherwise
        """
        try:
            # Skip upload if no bucket specified
            if not self.result_bucket:
                logger.info("No result bucket specified, skipping upload")
                return True

            # Create result key/path
            result_key = f"{self.result_prefix}/{self.job_id}/{task_id}.out"

            if self.provider == 'aws':
                # Upload to S3
                self.storage_client.put_object(
                    Bucket=self.result_bucket,
                    Key=result_key,
                    Body=str(result)
                )

            elif self.provider == 'gcp':
                # Upload to GCS
                bucket = self.storage_client.bucket(self.result_bucket)
                blob = bucket.blob(result_key)
                blob.upload_from_string(str(result))

            elif self.provider == 'azure':
                # Upload to Azure Blob Storage
                blob_client = self.storage_client.get_blob_client(
                    container=self.result_bucket,
                    blob=result_key
                )
                await blob_client.upload_blob(str(result), overwrite=True)

            return True

        except Exception as e:
            logger.error(f"Error uploading result for task {task_id}: {e}")
            return False

    async def receive_aws_tasks(self, max_count: int = 10) -> List[Dict[str, Any]]:
        """Receive tasks from AWS SQS queue."""
        response = self.queue_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_count,
            VisibilityTimeout=120,  # 2 minutes to process task
            WaitTimeSeconds=20,  # Long polling
            AttributeNames=['All'],
            MessageAttributeNames=['All']
        )

        tasks = []
        if 'Messages' in response:
            for message in response['Messages']:
                body = json.loads(message['Body'])
                tasks.append({
                    'task_id': body.get('task_id'),
                    'data': body.get('data', {}),
                    'receipt_handle': message['ReceiptHandle']
                })

        return tasks

    async def receive_gcp_tasks(self, max_count: int = 10) -> List[Dict[str, Any]]:
        """Receive tasks from GCP Pub/Sub subscription."""
        response = self.queue_client.pull(
            request={
                "subscription": self.subscription_path,
                "max_messages": max_count,
            }
        )

        tasks = []
        for received_message in response.received_messages:
            message_data = json.loads(received_message.message.data.decode('utf-8'))
            tasks.append({
                'task_id': message_data.get('task_id'),
                'data': message_data.get('data', {}),
                'ack_id': received_message.ack_id
            })

        return tasks

    async def receive_azure_tasks(self, max_count: int = 10) -> List[Dict[str, Any]]:
        """Receive tasks from Azure Service Bus queue."""
        tasks = []

        async with self.queue_client:
            receiver = self.queue_client.get_queue_receiver(
                queue_name=self.queue_name
            )
            async with receiver:
                messages = await receiver.receive_messages(
                    max_message_count=max_count,
                    max_wait_time=20  # Max wait time in seconds
                )

                for message in messages:
                    message_body = json.loads(str(message))
                    tasks.append({
                        'task_id': message_body.get('task_id'),
                        'data': message_body.get('data', {}),
                        'message': message
                    })

        return tasks

    async def complete_task(self, task: Dict[str, Any], success: bool = True) -> None:
        """
        Mark a task as completed or failed in the queue.

        Args:
            task: Task details including provider-specific receipt/ack
            success: Whether the task was successful
        """
        try:
            if self.provider == 'aws':
                if success:
                    # Delete message from SQS
                    self.queue_client.delete_message(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=task['receipt_handle']
                    )
                else:
                    # Make message visible again after a delay
                    self.queue_client.change_message_visibility(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=task['receipt_handle'],
                        VisibilityTimeout=30  # Short delay before retry
                    )

            elif self.provider == 'gcp':
                if success:
                    # Acknowledge the message
                    self.queue_client.acknowledge(
                        request={
                            "subscription": self.subscription_path,
                            "ack_ids": [task['ack_id']]
                        }
                    )
                else:
                    # Negative acknowledgment
                    self.queue_client.modify_ack_deadline(
                        request={
                            "subscription": self.subscription_path,
                            "ack_ids": [task['ack_id']],
                            "ack_deadline_seconds": 0  # Immediate retry
                        }
                    )

            elif self.provider == 'azure':
                if success:
                    # Complete the message
                    await task['message'].complete()
                else:
                    # Abandon the message
                    await task['message'].abandon()

        except Exception as e:
            logger.error(f"Error completing task: {e}")

    async def check_termination(self) -> bool:
        """
        Check if the instance is scheduled for termination.

        Returns:
            True if termination is imminent, False otherwise
        """
        try:
            if self.provider == 'aws':
                # Check AWS spot termination notice
                response = requests.get(
                    "http://169.254.169.254/latest/meta-data/spot/instance-action",
                    timeout=2
                )
                return response.status_code == 200

            elif self.provider == 'gcp':
                # Check GCP preemption notice
                response = requests.get(
                    "http://metadata.google.internal/computeMetadata/v1/instance/preempted",
                    headers={"Metadata-Flavor": "Google"},
                    timeout=2
                )
                return response.text.strip().lower() == "true"

            elif self.provider == 'azure':
                # Azure doesn't have a direct API for this
                # We could check for scheduled events, but that's more complex
                return False

        except Exception:
            # Request failed, likely not a spot/preemptible instance
            # or metadata endpoint not available
            return False

    async def termination_check_loop(self):
        """Background task to check for instance termination."""
        while self.running and not self.shutdown_event.is_set():
            try:
                # Check if instance is being terminated
                is_terminating = await self.check_termination()

                if is_terminating:
                    logger.warning("Instance termination detected, initiating graceful shutdown")
                    self.termination_requested = True
                    self.shutdown_event.set()

            except Exception as e:
                logger.error(f"Error checking for termination: {e}")

            # Check every few seconds
            await asyncio.sleep(TERMINATION_CHECK_INTERVAL)

    async def process_queue(self):
        """Main processing loop for tasks."""
        while self.running and not self.shutdown_event.is_set():
            try:
                # Receive tasks from queue
                tasks = []

                if self.provider == 'aws':
                    tasks = await self.receive_aws_tasks()
                elif self.provider == 'gcp':
                    tasks = await self.receive_gcp_tasks()
                elif self.provider == 'azure':
                    tasks = await self.receive_azure_tasks()

                if not tasks:
                    # No tasks available, wait before trying again
                    logger.debug("No tasks available, waiting...")
                    await asyncio.sleep(5)
                    continue

                logger.info(f"Received {len(tasks)} tasks from queue")

                # Process each task
                for task in tasks:
                    # Skip if termination has been requested
                    if self.termination_requested:
                        logger.info("Termination requested, skipping remaining tasks")
                        break

                    task_id = task.get('task_id', 'unknown')
                    task_data = task.get('data', {})

                    logger.info(f"Processing task {task_id}")

                    # Process with retry
                    success = False
                    result = None

                    for attempt in range(MAX_RETRIES):
                        success, result = await self.process_task(task_id, task_data)
                        if success:
                            break
                        logger.warning(f"Task {task_id} failed, attempt {attempt+1}/{MAX_RETRIES}")

                    # Mark task as complete or failed
                    await self.complete_task(task, success)

                    if success:
                        self.tasks_processed += 1
                        logger.info(f"Task {task_id} completed successfully")
                    else:
                        self.tasks_failed += 1
                        logger.error(f"Task {task_id} failed after {MAX_RETRIES} attempts")

            except Exception as e:
                logger.error(f"Error processing queue: {e}")
                # Wait before retrying
                await asyncio.sleep(5)

    async def start(self):
        """Start the worker."""
        self.running = True

        # Start termination check loop
        termination_task = asyncio.create_task(self.termination_check_loop())

        try:
            # Process queue
            logger.info(f"Starting worker for {self.provider} queue {self.queue_name}")
            await self.process_queue()

        except asyncio.CancelledError:
            logger.info("Worker task cancelled")

        except Exception as e:
            logger.error(f"Unexpected error in worker: {e}")

        finally:
            # Cleanup
            self.running = False
            termination_task.cancel()

            try:
                await termination_task
            except asyncio.CancelledError:
                pass

            logger.info(f"Worker shutdown. Tasks processed: {self.tasks_processed}, "
                        f"failed: {self.tasks_failed}")


async def main():
    """
    Main entry point for the cloud worker.
    Parses arguments, loads config, and starts the worker.
    """
    if len(sys.argv) < 2 or not any(arg.startswith("--config=") for arg in sys.argv):
        logger.error("Please provide a config file path (--config=/path/to/config.json)")
        sys.exit(1)

    config_path = None
    for arg in sys.argv:
        if arg.startswith("--config="):
            config_path = arg.split("=", 1)[1]
            break

    if not config_path:
        logger.error("Please provide a config file path (--config=/path/to/config.json)")
        sys.exit(1)

    try:
        # Validate the config file exists
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

        logger.info(f"Starting cloud worker with config from {config_path}")

        try:
            # Create worker instance
            worker = CloudWorker(config)

            # Start the worker
            await worker.start()
        except KeyError as e:
            logger.error(f"Missing required key in configuration from {config_path}: {e}")
            sys.exit(1)
        except ValueError as e:
            logger.error(f"Invalid configuration value in {config_path}: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error initializing worker from config {config_path}: {e}")
            logger.debug(traceback.format_exc())
            sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error in cloud worker: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())