"""
AWS SQS implementation of the TaskQueue interface.
"""

import json
import logging
import asyncio
from typing import Any, Dict, List, Optional

import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from .queue_manager import QueueManager
from ..common.config import AWSConfig


class AWSSQSQueue(QueueManager):
    """AWS SQS implementation of the TaskQueue interface."""

    def __init__(
        self,
        aws_config: Optional[AWSConfig] = None,
        queue_name: Optional[str] = None,
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
            self._queue_name = aws_config.queue_name

        if self._queue_name is None:
            raise ValueError("Queue name is required")

        self._logger = logging.getLogger(__name__)

        self._sqs = None
        self._queue_url = None

        # Check if queue exists
        self._queue_exists = False
        try:
            self._logger.info(f"Initializing AWS SQS queue with queue name: {self._queue_name}")

            # Create SQS client
            self._sqs = boto3.client(
                "sqs",
                aws_access_key_id=aws_config.access_key if aws_config else kwargs.get("access_key"),
                aws_secret_access_key=(
                    aws_config.secret_key if aws_config else kwargs.get("secret_key")
                ),
                region_name=aws_config.region if aws_config else kwargs.get("region"),
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

    def _create_queue(self) -> None:
        """Create the SQS queue if it doesn't exist."""
        if self._queue_exists:
            return

        try:
            self._logger.debug(f"Creating queue: {self._queue_name}")
            response = self._sqs.create_queue(
                QueueName=self._queue_name,
                Attributes={
                    "VisibilityTimeout": "30",  # TODO Default visibility timeout in seconds
                    "MessageRetentionPeriod": "1209600",  # 14 days (maximum)
                },
            )
            self._queue_url = response["QueueUrl"]
            self._queue_exists = True
            self._logger.info(f"Successfully created queue: {self._queue_name}")
        except ClientError as e:
            # If queue was created by another process while we were trying
            if e.response["Error"]["Code"] == "QueueAlreadyExists":
                response = self._sqs.get_queue_url(QueueName=self._queue_name)
                self._queue_url = response["QueueUrl"]
                self._queue_exists = True
                self._logger.info(f"Queue was created by another process: {self._queue_name}")
            else:
                self._logger.error(
                    f"Failed to create queue '{self._queue_name}': {str(e)}",
                    exc_info=True,
                )
                raise

    async def send_message(self, message: Dict[str, Any]) -> None:
        """
        Send a message to the SQS queue.

        Args:
            message: Message to be sent
        """
        self._logger.debug(f'Sending message to queue "{self._queue_name}"')

        self._create_queue()

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking SQS operation in a thread pool
            await loop.run_in_executor(
                None,
                lambda: self._sqs.send_message(
                    QueueUrl=self._queue_url,
                    MessageBody=json.dumps(message),
                ),
            )

            self._logger.debug(f"Published message to queue {self._queue_name}")
        except Exception as e:
            self._logger.error(f"Failed to send message to AWS SQS queue: {str(e)}")
            raise

    async def send_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """
        Send a task to the SQS queue.

        Args:
            task_id: Unique identifier for the task
            task_data: Task data to be sent
        """
        self._logger.debug(f"Sending task '{task_id}' to queue '{self._queue_name}'")

        self._create_queue()

        message = {"task_id": task_id, "data": task_data}

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking SQS operation in a thread pool
            await loop.run_in_executor(
                None,
                lambda: self._sqs.send_message(
                    QueueUrl=self._queue_url,
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
    ) -> List[Dict[str, Any]]:
        """
        Receive messages from the SQS queue.

        Args:
            max_count: Maximum number of messages to receive

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

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking receive operation in a thread pool
            response = await loop.run_in_executor(
                None,
                lambda: self._sqs.receive_message(
                    QueueUrl=self._queue_url,
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

                    # Delete the message immediately
                    await loop.run_in_executor(
                        None,
                        lambda: self._sqs.delete_message(
                            QueueUrl=self._queue_url, ReceiptHandle=message["ReceiptHandle"]
                        ),
                    )

            self._logger.debug(f"Received and deleted {len(messages)} messages from SQS queue")
            return messages
        except Exception as e:
            self._logger.error(f"Error receiving messages: {str(e)}")
            raise

    async def receive_tasks(
        self,
        max_count: int = 1,
        visibility_timeout: int = 30,
    ) -> List[Dict[str, Any]]:
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

        try:
            # SQS limits max_count to 10
            max_count = min(max_count, 10)

            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking receive operation in a thread pool
            response = await loop.run_in_executor(
                None,
                lambda: self._sqs.receive_message(
                    QueueUrl=self._queue_url,
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

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking delete operation in a thread pool
            await loop.run_in_executor(
                None,
                lambda: self._sqs.delete_message(
                    QueueUrl=self._queue_url, ReceiptHandle=task_handle
                ),
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

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking visibility change operation in a thread pool
            await loop.run_in_executor(
                None,
                lambda: self._sqs.change_message_visibility(
                    QueueUrl=self._queue_url, ReceiptHandle=task_handle, VisibilityTimeout=0
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

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking get attributes operation in a thread pool
            response = await loop.run_in_executor(
                None,
                lambda: self._sqs.get_queue_attributes(
                    QueueUrl=self._queue_url, AttributeNames=["ApproximateNumberOfMessages"]
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

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Run the blocking purge operation in a thread pool
            await loop.run_in_executor(
                None, lambda: self._sqs.purge_queue(QueueUrl=self._queue_url)
            )
            self._logger.debug(f"Purged queue {self._queue_name}")
        except Exception as e:
            self._logger.error(f"Error purging queue: {str(e)}")
            raise

    async def delete_queue(self) -> None:
        """Delete the queue."""
        try:
            queue_url = self._sqs.get_queue_url(QueueName=self._queue_name)["QueueUrl"]
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
            await loop.run_in_executor(None, lambda: self._sqs.delete_queue(QueueUrl=queue_url))
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
