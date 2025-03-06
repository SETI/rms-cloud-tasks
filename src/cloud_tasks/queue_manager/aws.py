"""
AWS SQS implementation of the TaskQueue interface.
"""
import json
from typing import Any, Dict, List, Optional

import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from cloud_tasks.common.base import TaskQueue


class AWSSQSQueue(TaskQueue):
    """AWS SQS implementation of the TaskQueue interface."""

    def __init__(self):
        self.sqs = None
        self.queue_url = None
        self.queue_name = None

    async def initialize(self, queue_name: str, config: Dict[str, Any]) -> None:
        """
        Initialize the SQS queue with configuration.

        Args:
            queue_name: Name of the SQS queue
            config: AWS configuration with access_key, secret_key, and region
        """
        self.queue_name = queue_name

        # Create SQS client
        self.sqs = boto3.client(
            'sqs',
            aws_access_key_id=config['access_key'],
            aws_secret_access_key=config['secret_key'],
            region_name=config['region']
        )

        # Create queue if it doesn't exist
        try:
            response = self.sqs.create_queue(
                QueueName=queue_name,
                Attributes={
                    'VisibilityTimeout': '30',  # Default visibility timeout in seconds
                    'MessageRetentionPeriod': '1209600',  # 14 days (maximum)
                }
            )
            self.queue_url = response['QueueUrl']
        except ClientError as e:
            # If queue already exists, get its URL
            if e.response['Error']['Code'] == 'QueueAlreadyExists':
                response = self.sqs.get_queue_url(QueueName=queue_name)
                self.queue_url = response['QueueUrl']
            else:
                raise

    async def send_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """
        Send a task to the SQS queue.

        Args:
            task_id: Unique identifier for the task
            task_data: Task data to be processed
        """
        message = {
            'task_id': task_id,
            'data': task_data
        }

        self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(message),
            MessageAttributes={
                'TaskId': {
                    'DataType': 'String',
                    'StringValue': task_id
                }
            }
        )

    async def receive_tasks(self, max_count: int = 1, visibility_timeout_seconds: int = 30) -> List[Dict[str, Any]]:
        """
        Receive tasks from the SQS queue with a visibility timeout.

        Args:
            max_count: Maximum number of messages to receive (1-10)
            visibility_timeout_seconds: Duration in seconds that messages are hidden

        Returns:
            List of task dictionaries with task_id, data, and receipt_handle
        """
        # SQS limits max_count to 10
        max_count = min(max_count, 10)

        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_count,
            VisibilityTimeout=visibility_timeout_seconds,
            MessageAttributeNames=['All'],
            WaitTimeSeconds=10  # Using long polling
        )

        tasks = []
        if 'Messages' in response:
            for message in response['Messages']:
                body = json.loads(message['Body'])
                tasks.append({
                    'task_id': body['task_id'],
                    'data': body['data'],
                    'receipt_handle': message['ReceiptHandle']  # Used to complete/fail the task
                })

        return tasks

    async def complete_task(self, task_handle: Any) -> None:
        """
        Mark a task as completed and remove from the queue.

        Args:
            task_handle: Receipt handle from receive_tasks
        """
        self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=task_handle
        )

    async def fail_task(self, task_handle: Any) -> None:
        """
        Mark a task as failed, allowing it to be retried.

        Args:
            task_handle: Receipt handle from receive_tasks
        """
        # Change visibility timeout to 0, making the message immediately available
        self.sqs.change_message_visibility(
            QueueUrl=self.queue_url,
            ReceiptHandle=task_handle,
            VisibilityTimeout=0
        )

    async def get_queue_depth(self) -> int:
        """
        Get the current depth (number of messages) in the queue.

        Returns:
            Approximate number of messages in the queue
        """
        response = self.sqs.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )

        return int(response['Attributes']['ApproximateNumberOfMessages'])

    async def purge_queue(self) -> None:
        """Remove all messages from the queue."""
        self.sqs.purge_queue(QueueUrl=self.queue_url)