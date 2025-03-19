"""
AWS SQS implementation of the TaskQueue interface.
"""

import json
import logging
from typing import Any, Dict, List

import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from .taskqueue import TaskQueue
from cloud_tasks.common.config import ProviderConfig


class AWSSQSQueue(TaskQueue):
    """AWS SQS implementation of the TaskQueue interface."""

    def __init__(self, queue_name: str, config: ProviderConfig) -> None:
        """
        Initialize the SQS queue with configuration.

        Args:
            queue_name: Name of the SQS queue
            config: AWS configuration with access_key, secret_key, and region
        """
        self._sqs = None
        self._queue_url = None
        self._queue_name = None
        self._logger = logging.getLogger(__name__)

        try:
            self._queue_name = queue_name
            self._logger.info(f"Initializing AWS SQS queue with queue name: {self._queue_name}")
            # Create SQS client
            self._sqs = boto3.client(
                "sqs",
                aws_access_key_id=config.access_key,
                aws_secret_access_key=config.secret_key,
                region_name=config.region,
            )

            # Create queue if it doesn't exist
            try:
                response = self._sqs.create_queue(
                    QueueName=queue_name,
                    Attributes={
                        "VisibilityTimeout": "30",  # TODO Default visibility timeout in seconds
                        "MessageRetentionPeriod": "1209600",  # 14 days (maximum)
                    },
                )
                self._queue_url = response["QueueUrl"]
            except ClientError as e:
                # If queue already exists, get its URL
                if e.response["Error"]["Code"] == "QueueAlreadyExists":
                    response = self._sqs.get_queue_url(QueueName=queue_name)
                    self._queue_url = response["QueueUrl"]
                else:
                    raise

        except Exception as e:
            self._logger.error(f"Failed to initialize AWS SQS queue: {str(e)}")
            raise

    async def send_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """
        Send a task to the SQS queue.

        Args:
            task_id: Unique identifier for the task
            task_data: Task data to be processed
        """
        message = {"task_id": task_id, "data": task_data}

        try:
            self._sqs.send_message(
                QueueUrl=self._queue_url,
                MessageBody=json.dumps(message),
                MessageAttributes={"TaskId": {"DataType": "String", "StringValue": task_id}},
            )
            self._logger.debug(f"Published message for task {task_id}")
        except Exception as e:
            self._logger.error(f"Failed to send task to AWS SQS queue: {str(e)}")
            raise RuntimeError(f"Failed to publish task to SQS: {str(e)}")

    async def receive_tasks(
        self,
        max_count: int = 1,
        visibility_timeout_seconds: int = 30,  # TODO Default visibility timeout in seconds
    ) -> List[Dict[str, Any]]:
        """
        Receive tasks from the SQS queue with a visibility timeout.

        Args:
            max_count: Maximum number of messages to receive (1-10)
            visibility_timeout_seconds: Duration in seconds that messages are hidden

        Returns:
            List of task dictionaries with task_id, data, and receipt_handle
        """
        try:
            # SQS limits max_count to 10
            max_count = min(max_count, 10)

            response = self._sqs.receive_message(
                QueueUrl=self._queue_url,
                MaxNumberOfMessages=max_count,
                VisibilityTimeout=visibility_timeout_seconds,
                MessageAttributeNames=["All"],
                WaitTimeSeconds=10,  # Using long polling
            )

            tasks = []
            if "Messages" in response:
                for message in response["Messages"]:
                    body = json.loads(message["Body"])
                    tasks.append(
                        {
                            "task_id": body["task_id"],
                            "data": body["data"],
                            "receipt_handle": message[
                                "ReceiptHandle"
                            ],  # Used to complete/fail the task
                        }
                    )

            self._logger.debug(f"Received {len(tasks)} tasks from SQS queue")
            return tasks
        except Exception as e:
            self._logger.error(f"Error receiving tasks: {str(e)}")
            return []

    async def complete_task(self, task_handle: Any) -> None:
        """
        Mark a task as completed and remove from the queue.

        Args:
            task_handle: Receipt handle from receive_tasks
        """
        try:
            self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=task_handle)
            self._logger.debug(f"Completed task with ack_id: {task_handle}")
        except Exception as e:
            self._logger.error(f"Error completing task: {str(e)}")
            raise

    async def fail_task(self, task_handle: Any) -> None:
        """
        Mark a task as failed, allowing it to be retried.

        Args:
            task_handle: Receipt handle from receive_tasks
        """
        try:
            # Change visibility timeout to 0, making the message immediately available
            self._sqs.change_message_visibility(
                QueueUrl=self._queue_url, ReceiptHandle=task_handle, VisibilityTimeout=0
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
        try:
            response = self._sqs.get_queue_attributes(
                QueueUrl=self._queue_url, AttributeNames=["ApproximateNumberOfMessages"]
            )

            message_count = int(response["Attributes"]["ApproximateNumberOfMessages"])
            self._logger.debug(f"Queue depth estimated at {message_count}+ messages")
            return message_count
        except Exception as e:
            self._logger.error(f"Error getting queue depth: {str(e)}")
            return 0

    async def purge_queue(self) -> None:
        """Remove all messages from the queue."""
        try:
            self._sqs.purge_queue(QueueUrl=self._queue_url)
            self._logger.debug(f"Purged queue {self._queue_name}")
        except Exception as e:
            self._logger.error(f"Error purging queue: {str(e)}")
            raise
