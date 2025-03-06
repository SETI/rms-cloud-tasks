"""
Cloud adapter module for worker integration.

This module allows any worker code to be easily integrated with cloud
task processing by abstracting away cloud provider-specific implementation
details.
"""
import asyncio
import base64
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Callable, Awaitable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default values
DEFAULT_MAX_RETRIES = 3
DEFAULT_TERMINATION_CHECK_INTERVAL = 5  # in seconds


class CloudTaskAdapter:
    """
    Adapter class to connect any worker code to cloud task processing.

    This provides an abstract interface to different cloud providers
    with standard methods for receiving tasks, reporting results,
    and handling instance lifecycle events.
    """

    def __init__(
        self,
        task_processor: Callable[[str, Dict[str, Any]], Awaitable[Tuple[bool, Any]]],
        config_file: str = None,
        config: Dict[str, Any] = None
    ):
        """
        Initialize the cloud task adapter.

        Args:
            task_processor: Async function that processes tasks, taking (task_id, task_data)
                            and returning (success, result)
            config_file: Path to configuration file (alternative to config param)
            config: Configuration dictionary (alternative to config_file param)
        """
        if not config and not config_file:
            raise ValueError("Either config or config_file must be provided")

        self.task_processor = task_processor
        self.config = self._load_config(config_file) if config_file else config

        # Initialize configuration
        self.provider = self.config.get('provider', '').lower()
        self.job_id = self.config.get('job_id', 'unknown')
        self.queue_name = self.config.get('queue_name', '')
        self.result_bucket = self.config.get('result_bucket', '')
        self.result_prefix = self.config.get('result_prefix', '')

        # Worker options
        worker_options = self.config.get('worker_options', {})
        self.max_retries = worker_options.get('max_retries', DEFAULT_MAX_RETRIES)
        self.termination_check_interval = worker_options.get(
            'termination_check_interval', DEFAULT_TERMINATION_CHECK_INTERVAL
        )

        # Validate configuration
        if not self.provider:
            raise ValueError("Provider must be specified (aws, gcp, or azure)")
        if not self.queue_name:
            raise ValueError("Queue name must be specified")

        # Initialize cloud provider clients
        self.queue_client = None
        self.storage_client = None

        # State variables
        self.running = False
        self.termination_requested = False
        self.shutdown_event = asyncio.Event()

        # Task metrics
        self.tasks_processed = 0
        self.tasks_failed = 0

        # Import necessary cloud provider modules
        if self.provider == 'aws':
            self._setup_aws_imports()
        elif self.provider == 'gcp':
            self._setup_gcp_imports()
        elif self.provider == 'azure':
            self._setup_azure_imports()
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")

        # Configure provider-specific details
        self._configure_provider()

    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from a file."""
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise ValueError(f"Failed to load configuration: {e}")

    def _setup_aws_imports(self):
        """Import AWS-specific modules."""
        try:
            global boto3
            import boto3
        except ImportError:
            raise ImportError(
                "AWS dependencies not installed. "
                "Install boto3 with: pip install boto3"
            )

    def _setup_gcp_imports(self):
        """Import GCP-specific modules."""
        try:
            global pubsub_v1, storage
            from google.cloud import pubsub_v1, storage
        except ImportError:
            raise ImportError(
                "GCP dependencies not installed. "
                "Install google-cloud packages with: "
                "pip install google-cloud-pubsub google-cloud-storage"
            )

    def _setup_azure_imports(self):
        """Import Azure-specific modules."""
        try:
            global ServiceBusClient, BlobServiceClient
            from azure.servicebus.aio import ServiceBusClient
            from azure.storage.blob.aio import BlobServiceClient
        except ImportError:
            raise ImportError(
                "Azure dependencies not installed. "
                "Install azure packages with: "
                "pip install azure-servicebus azure-storage-blob"
            )

    def _configure_provider(self):
        """Configure provider-specific clients and settings."""
        if self.provider == 'aws':
            self._configure_aws()
        elif self.provider == 'gcp':
            self._configure_gcp()
        elif self.provider == 'azure':
            self._configure_azure()

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
            result_str = str(result)

            if self.provider == 'aws':
                # Upload to S3
                self.storage_client.put_object(
                    Bucket=self.result_bucket,
                    Key=result_key,
                    Body=result_str
                )

            elif self.provider == 'gcp':
                # Upload to GCS
                bucket = self.storage_client.bucket(self.result_bucket)
                blob = bucket.blob(result_key)
                blob.upload_from_string(result_str)

            elif self.provider == 'azure':
                # Upload to Azure Blob Storage
                blob_client = self.storage_client.get_blob_client(
                    container=self.result_bucket,
                    blob=result_key
                )
                await blob_client.upload_blob(result_str, overwrite=True)

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
                import requests  # Import here to avoid dependency if not using AWS
                response = requests.get(
                    "http://169.254.169.254/latest/meta-data/spot/instance-action",
                    timeout=2
                )
                return response.status_code == 200

            elif self.provider == 'gcp':
                # Check GCP preemption notice
                import requests  # Import here to avoid dependency if not using GCP
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
            await asyncio.sleep(self.termination_check_interval)

    async def process_task_with_retries(self, task_id: str, task_data: Dict[str, Any]) -> Tuple[bool, Any]:
        """
        Process a task with retries.

        Args:
            task_id: ID of the task
            task_data: Task data to process

        Returns:
            Tuple of (success, result)
        """
        for attempt in range(self.max_retries):
            try:
                success, result = await self.task_processor(task_id, task_data)
                if success:
                    # If result bucket is specified, upload the result
                    if self.result_bucket:
                        upload_success = await self.upload_result(task_id, result)
                        if upload_success:
                            logger.info(f"Result for task {task_id} uploaded to storage")
                        else:
                            logger.warning(f"Failed to upload result for task {task_id}")
                    return True, result

                # If task failed but we have retries left
                if attempt < self.max_retries - 1:
                    logger.warning(f"Task {task_id} failed, attempt {attempt+1}/{self.max_retries}")
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

            except Exception as e:
                logger.error(f"Error processing task {task_id}: {e}")
                if attempt < self.max_retries - 1:
                    logger.warning(f"Retrying task {task_id}, attempt {attempt+1}/{self.max_retries}")
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

        # If we get here, all retries have failed
        return False, "Failed after all retry attempts"

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
                    success, result = await self.process_task_with_retries(task_id, task_data)

                    # Mark task as complete or failed
                    await self.complete_task(task, success)

                    if success:
                        self.tasks_processed += 1
                        logger.info(f"Task {task_id} completed successfully")
                    else:
                        self.tasks_failed += 1
                        logger.error(f"Task {task_id} failed after {self.max_retries} attempts")

            except Exception as e:
                logger.error(f"Error processing queue: {e}")
                # Wait before retrying
                await asyncio.sleep(5)

    async def process_sample_task(self):
        """Process a sample task if one is defined in the configuration."""
        sample_task = self.config.get('sample_task')
        if sample_task:
            task_id = sample_task.get('id', 'sample-task')
            task_data = sample_task.get('data', {})

            logger.info(f"Processing sample task {task_id}")
            success, result = await self.process_task_with_retries(task_id, task_data)

            if success:
                logger.info(f"Sample task {task_id} completed successfully with result: {result}")
                return True
            else:
                logger.error(f"Sample task {task_id} failed with result: {result}")
                return False
        return None

    async def start(self):
        """Start the worker."""
        self.running = True
        logger.info(f"Starting cloud task worker for {self.provider}")

        # Start termination check loop
        termination_task = asyncio.create_task(self.termination_check_loop())

        try:
            # Check for a sample task
            sample_result = await self.process_sample_task()

            # If no sample task or in continuous mode, process the queue
            if sample_result is None or self.config.get('continuous_mode', True):
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


async def run_cloud_worker(
    task_processor: Callable[[str, Dict[str, Any]], Awaitable[Tuple[bool, Any]]],
    config_file: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None
) -> None:
    """
    Run a worker in the cloud.

    This is a convenience function to create and run a CloudTaskAdapter.

    Args:
        task_processor: Async function to process tasks
        config_file: Path to configuration file (alternative to config)
        config: Configuration dictionary (alternative to config_file)
    """
    # Get config file from command line if not provided
    if not config_file and not config:
        for arg in sys.argv:
            if arg.startswith("--config="):
                config_file = arg.split("=", 1)[1]
                break

    if not config_file and not config:
        logger.error("No configuration provided. Use --config=path/to/config.json or provide config dict")
        sys.exit(1)

    # Create and start the adapter
    adapter = CloudTaskAdapter(task_processor, config_file, config)
    await adapter.start()