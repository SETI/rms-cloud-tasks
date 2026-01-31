"""
AWS SQS implementation of the TaskQueue interface.
"""

import asyncio
import json
import logging
from typing import Any

import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from ..common.config import AWSConfig
from .queue_manager import QueueManager


class AWSSQSQueue(QueueManager):
    """AWS SQS implementation of the TaskQueue interface."""

    # A message not acknowledged within this time will be made available again for processing
    _DEFAULT_VISIBILITY_TIMEOUT = 60

    # Maximum visibility timeout allowed by AWS SQS
    _MAXIMUM_VISIBILITY_TIMEOUT = 43200

    def __init__(
        self,
        aws_config: AWSConfig | None = None,
        queue_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the SQS queue with configuration.

        Args:
            aws_config: AWS configuration
            queue_name: Name of the SQS queue (if not using aws_config)
            **kwargs: Additional configuration parameters
        """
        if queue_name is not None:
            self._queue_name = queue_name
        else:
            if aws_config is None:
                raise ValueError("Either aws_config or queue_name is required")
            self._queue_name = aws_config.queue_name or ""

        if not self._queue_name:
            raise ValueError("Queue name is required")

        self._logger = logging.getLogger(__name__)

        self._sqs: Any = None
        self._queue_url: str | None = None

        # Check if queue exists
        self._queue_exists = False
        config = aws_config if queue_name is None else None
        try:
            self._logger.info(f"Initializing AWS SQS queue with queue name: {self._queue_name}")

            # Create SQS client
            self._sqs = boto3.client(
                "sqs",
                aws_access_key_id=config.access_key if config else kwargs.get("access_key"),
                aws_secret_access_key=(config.secret_key if config else kwargs.get("secret_key")),
                region_name=config.region if config else kwargs.get("region"),
            )

            # Check if queue exists
            try:
                response = self._sqs.get_queue_url(QueueName=self._queue_name)
                self._queue_url = response["QueueUrl"]
                self._logger.info(f"Found existing queue: {self._queue_name}")
                self._queue_exists = True
            except ClientError as e:
                if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
                    self._logger.info(
                        f"Queue {self._queue_name} does not exist...deferring creation"
                    )
                else:
                    raise

        except Exception as e:
            self._logger.error(f"Failed to initialize AWS SQS queue: {str(e)}")
            raise

    def _get_sqs(self) -> Any:
        """Return the SQS client; raises if not initialized."""
        assert self._sqs is not None, "SQS client not initialized"
        return self._sqs

    def _get_queue_url(self) -> str:
        """Return the queue URL; raises if not set."""
        assert self._queue_url is not None, "Queue URL not set"
        return self._queue_url

    def _create_queue(self) -> None:
        """Create the SQS queue if it doesn't exist."""
        if self._queue_exists:
            return

        try:
            sqs = self._get_sqs()
            self._logger.debug(f"Creating queue: {self._queue_name}")
            response = sqs.create_queue(
                QueueName=self._queue_name,
                Attributes={
                    "VisibilityTimeout": str(self._DEFAULT_VISIBILITY_TIMEOUT),
                    "MessageRetentionPeriod": "1209600",  # 14 days (maximum)
                },
            )
            self._queue_url = response["QueueUrl"]
            self._queue_exists = True
            self._logger.info(f"Successfully created queue: {self._queue_name}")
        except ClientError as e:
            # If queue was created by another process while we were trying
            if e.response["Error"]["Code"] == "QueueAlreadyExists":
                response = sqs.get_queue_url(QueueName=self._queue_name)
                self._queue_url = response["QueueUrl"]
                self._queue_exists = True
                self._logger.info(f"Queue was created by another process: {self._queue_name}")
            else:
                self._logger.error(
                    f"Failed to create queue '{self._queue_name}': {str(e)}",
                    exc_info=True,
                )
                raise

    async def send_message(self, message: dict[str, Any], _quiet: bool = False) -> None:
        """
        Send a message to the SQS queue.

        Args:
            message: Message to be sent
            _quiet: If True, suppress logging (for base class compatibility)
        """
        self._logger.debug(f'Sending message to queue "{self._queue_name}"')

        self._create_queue()
        sqs = self._get_sqs()
        queue_url = self._get_queue_url()

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking SQS operation in a thread pool
            await loop.run_in_executor(
                None,
                lambda: sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(message),
                ),
            )

            self._logger.debug(f"Published message to queue {self._queue_name}")
        except Exception as e:
            self._logger.error(f"Failed to send message to AWS SQS queue: {str(e)}")
            raise

    async def send_task(self, task_id: str, task_data: dict[str, Any]) -> None:
        """
        Send a task to the SQS queue.

        Args:
            task_id: Unique identifier for the task
            task_data: Task data to be sent
        """
        self._logger.debug(f"Sending task '{task_id}' to queue '{self._queue_name}'")

        self._create_queue()

        message = {"task_id": task_id, "data": task_data}
        sqs = self._get_sqs()
        queue_url = self._get_queue_url()

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking SQS operation in a thread pool
            await loop.run_in_executor(
                None,
                lambda: sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(message),
                    MessageAttributes={"TaskId": {"DataType": "String", "StringValue": task_id}},
                ),
            )

            self._logger.debug(f"Published message for task {task_id}")
        except Exception as e:
            self._logger.error(f"Failed to send task to AWS SQS queue: {str(e)}")
            raise

    async def receive_messages(
        self,
        max_count: int = 1,
        acknowledge: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Receive messages from the SQS queue.

        Args:
            max_count: Maximum number of messages to receive
            acknowledge: Unused for SQS (for base class compatibility)

        Returns:
            List of message dictionaries, each containing:
                - 'message_id' (str): SQS message ID
                - 'data' (Dict[str, Any]): Message payload
                - 'receipt_handle' (str): SQS receipt handle used for completing or failing the message
        """
        # SQS limits max_count to 10
        max_count = min(max_count, 10)

        self._logger.debug(f"Receiving up to {max_count} messages from queue '{self._queue_name}'")

        self._create_queue()
        sqs = self._get_sqs()
        queue_url = self._get_queue_url()

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking receive operation in a thread pool
            response = await loop.run_in_executor(
                None,
                lambda: sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=max_count,
                    MessageAttributeNames=["All"],
                    WaitTimeSeconds=10,  # Using long polling
                ),
            )

            messages = []
            if "Messages" in response:
                for message in response["Messages"]:
                    body = json.loads(message["Body"])
                    messages.append(
                        {
                            "message_id": message["MessageId"],
                            "data": body,
                            "receipt_handle": message["ReceiptHandle"],
                        }
                    )

                    # Delete the message immediately (default arg captures receipt per iteration)
                    receipt = message["ReceiptHandle"]
                    await loop.run_in_executor(
                        None,
                        lambda r=receipt: sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=r),
                    )

            self._logger.debug(f"Received and deleted {len(messages)} messages from SQS queue")
            return messages
        except Exception as e:
            self._logger.error(f"Error receiving messages: {str(e)}")
            raise

    async def receive_tasks(
        self,
        max_count: int = 1,
        visibility_timeout: int = 60,
    ) -> list[dict[str, Any]]:
        """
        Receive tasks from the SQS queue.

        Args:
            max_count: Maximum number of messages to receive
            visibility_timeout: Duration in seconds that messages are hidden

        Returns:
            List of task dictionaries, each containing:
                - 'task_id' (str): Unique identifier for the task
                - 'data' (Dict[str, Any]): Task payload/parameters
                - 'receipt_handle' (str): SQS receipt handle used for completing or failing the task
        """
        # SQS limits max_count to 10
        max_count = min(max_count, 10)

        self._logger.debug(f"Receiving up to {max_count} tasks from queue '{self._queue_name}'")

        self._create_queue()
        sqs = self._get_sqs()
        queue_url = self._get_queue_url()

        try:
            # SQS limits max_count to 10
            max_count = min(max_count, 10)

            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking receive operation in a thread pool
            response = await loop.run_in_executor(
                None,
                lambda: sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=max_count,
                    VisibilityTimeout=visibility_timeout,
                    MessageAttributeNames=["All"],
                    WaitTimeSeconds=10,  # Using long polling
                ),
            )

            tasks = []
            if "Messages" in response:
                for message in response["Messages"]:
                    body = json.loads(message["Body"])
                    tasks.append(
                        {
                            "task_id": body["task_id"],
                            "data": body["data"],
                            "receipt_handle": message["ReceiptHandle"],
                        }
                    )

            self._logger.debug(f"Received {len(tasks)} tasks from SQS queue")
            return tasks
        except Exception as e:
            self._logger.error(f"Error receiving tasks: {str(e)}")
            raise

    async def acknowledge_task(self, task_handle: Any) -> None:
        """
        Mark a task as completed and remove from the queue.

        Args:
            task_handle: Receipt handle from receive_tasks
        """
        self._logger.debug(
            f"Completing task with ack_id '{task_handle}' on queue '{self._queue_name}'"
        )

        self._create_queue()
        sqs = self._get_sqs()
        queue_url = self._get_queue_url()

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking delete operation in a thread pool
            await loop.run_in_executor(
                None,
                lambda: sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=task_handle),
            )
            self._logger.debug(f"Completed task with ack_id: {task_handle}")
        except Exception as e:
            self._logger.error(f"Error completing task: {str(e)}")
            raise

    async def retry_task(self, task_handle: Any) -> None:
        """
        Mark a task as failed, allowing it to be retried.

        Args:
            task_handle: Receipt handle from receive_tasks
        """
        self._logger.debug(
            f"Failing task with ack_id '{task_handle}' on queue '{self._queue_name}'"
        )

        self._create_queue()
        sqs = self._get_sqs()
        queue_url = self._get_queue_url()

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking visibility change operation in a thread pool
            await loop.run_in_executor(
                None,
                lambda: sqs.change_message_visibility(
                    QueueUrl=queue_url, ReceiptHandle=task_handle, VisibilityTimeout=0
                ),
            )
            self._logger.debug(f"Failed task with ack_id: {task_handle}")
        except Exception as e:
            self._logger.error(f"Error failing task: {str(e)}")
            raise

    async def get_queue_depth(self) -> int:
        """
        Get the current depth (number of messages) in the queue.

        Returns:
            Approximate number of messages in the queue
        """
        self._logger.debug(f"Getting queue depth for queue '{self._queue_name}'")

        self._create_queue()
        sqs = self._get_sqs()
        queue_url = self._get_queue_url()

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking get attributes operation in a thread pool
            response = await loop.run_in_executor(
                None,
                lambda: sqs.get_queue_attributes(
                    QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
                ),
            )

            message_count = int(response["Attributes"]["ApproximateNumberOfMessages"])
            self._logger.debug(f"Queue depth estimated at {message_count}+ messages")
            return message_count
        except Exception as e:
            self._logger.error(f"Error getting queue depth: {str(e)}")
            raise

    async def purge_queue(self) -> None:
        """Remove all messages from the queue."""
        self._logger.debug(f"Purging queue '{self._queue_name}'")

        self._create_queue()
        sqs = self._get_sqs()
        queue_url = self._get_queue_url()

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking purge operation in a thread pool
            await loop.run_in_executor(None, lambda: sqs.purge_queue(QueueUrl=queue_url))
            self._logger.debug(f"Purged queue {self._queue_name}")
        except Exception as e:
            self._logger.error(f"Error purging queue: {str(e)}")
            raise

    async def delete_queue(self) -> None:
        """Delete the queue."""
        sqs = self._get_sqs()
        try:
            queue_url = sqs.get_queue_url(QueueName=self._queue_name)["QueueUrl"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
                self._logger.info(f"Queue '{self._queue_name}' does not exist")
                self._queue_exists = False
                self._queue_url = None
                return
            else:
                raise

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking delete operation in a thread pool
            await loop.run_in_executor(None, lambda: sqs.delete_queue(QueueUrl=queue_url))
            self._queue_exists = False
            self._queue_url = None
            self._logger.info(f"Successfully deleted queue {self._queue_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
                self._logger.info(f"Queue '{self._queue_name}' was deleted concurrently")
                self._queue_exists = False
                self._queue_url = None
                return
            self._logger.error(f"Error deleting queue: {str(e)}")
            raise
        except Exception as e:
            self._logger.error(f"Error deleting queue: {str(e)}")
            raise

    def get_max_visibility_timeout(self) -> int:
        """Get the maximum visibility timeout allowed by AWS SQS.

        Returns:
            Maximum visibility timeout in seconds (43200 - 12 hours)
        """
        return 43200  # AWS SQS maximum visibility timeout

    async def extend_message_visibility(
        self, message_handle: Any, timeout: int | None = None
    ) -> None:
        """Extend the visibility timeout for a message.

        Args:
            message_handle: Receipt handle from receive_tasks
            timeout: New visibility timeout in seconds. If None, extends by the original timeout.
        """
        if timeout is None:
            # Use the default visibility timeout
            timeout = self._DEFAULT_VISIBILITY_TIMEOUT

        self._logger.debug(
            f"Extending visibility timeout for message with ack_id '{message_handle}' "
            f"to {timeout} seconds on queue '{self._queue_name}'"
        )

        self._create_queue()
        sqs = self._get_sqs()
        queue_url = self._get_queue_url()

        # Get the event loop
        loop = asyncio.get_event_loop()

        # Run the blocking visibility change operation in a thread pool
        await loop.run_in_executor(
            None,
            lambda: sqs.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=message_handle,
                VisibilityTimeout=timeout,
            ),
        )
