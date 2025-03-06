"""
Google Cloud Pub/Sub implementation of the TaskQueue interface.
"""
import json
import time
from typing import Any, Dict, List, Optional

from google.cloud import pubsub_v1  # type: ignore
from google.cloud.pubsub_v1.types import PullResponse, ReceivedMessage  # type: ignore

from cloud_tasks.common.base import TaskQueue


class GCPPubSubQueue(TaskQueue):
    """Google Cloud Pub/Sub implementation of the TaskQueue interface."""

    def __init__(self):
        self.publisher = None
        self.subscriber = None
        self.project_id = None
        self.topic_name = None
        self.subscription_name = None
        self.topic_path = None
        self.subscription_path = None

    async def initialize(self, queue_name: str, config: Dict[str, Any]) -> None:
        """
        Initialize the Pub/Sub queue with configuration.

        Args:
            queue_name: Base name for topic and subscription
            config: GCP configuration with project_id and optionally credentials_file
        """
        self.project_id = config['project_id']
        credentials_file = config.get('credentials_file')

        # If credentials file provided, use it
        if credentials_file:
            self.publisher = pubsub_v1.PublisherClient.from_service_account_file(credentials_file)
            self.subscriber = pubsub_v1.SubscriberClient.from_service_account_file(credentials_file)
        else:
            # Use default credentials
            self.publisher = pubsub_v1.PublisherClient()
            self.subscriber = pubsub_v1.SubscriberClient()

        # Derive topic and subscription names
        self.topic_name = f"{queue_name}-topic"
        self.subscription_name = f"{queue_name}-subscription"

        # Create fully qualified paths
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        self.subscription_path = self.subscriber.subscription_path(
            self.project_id, self.subscription_name
        )

        # Create topic if it doesn't exist
        try:
            self.publisher.get_topic(request={"topic": self.topic_path})
        except Exception:
            self.publisher.create_topic(request={"name": self.topic_path})

        # Create subscription if it doesn't exist
        try:
            self.subscriber.get_subscription(request={"subscription": self.subscription_path})
        except Exception:
            self.subscriber.create_subscription(
                request={
                    "name": self.subscription_path,
                    "topic": self.topic_path,
                    # Set message retention to maximum (7 days)
                    "message_retention_duration": {"seconds": 7 * 24 * 60 * 60},
                    # Default ack deadline (30 seconds)
                    "ack_deadline_seconds": 30,
                }
            )

    async def send_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """
        Send a task to the Pub/Sub topic.

        Args:
            task_id: Unique identifier for the task
            task_data: Task data to be processed
        """
        message = {
            'task_id': task_id,
            'data': task_data
        }

        # Convert message to JSON string and encode as bytes
        data = json.dumps(message).encode('utf-8')

        # Add task_id as an attribute
        future = self.publisher.publish(
            self.topic_path,
            data=data,
            task_id=task_id
        )

        # Wait for message to be published
        future.result()

    async def receive_tasks(self, max_count: int = 1, visibility_timeout_seconds: int = 30) -> List[Dict[str, Any]]:
        """
        Receive tasks from the Pub/Sub subscription.

        Args:
            max_count: Maximum number of messages to receive
            visibility_timeout_seconds: Duration in seconds for ack deadline

        Returns:
            List of task dictionaries with task_id, data, and ack_id
        """
        # Pull messages from the subscription
        response = self.subscriber.pull(
            request={
                "subscription": self.subscription_path,
                "max_messages": max_count,
            }
        )

        tasks = []
        for received_message in response.received_messages:
            # Modify the ack deadline for this message
            self.subscriber.modify_ack_deadline(
                request={
                    "subscription": self.subscription_path,
                    "ack_ids": [received_message.ack_id],
                    "ack_deadline_seconds": visibility_timeout_seconds,
                }
            )

            # Parse message data
            message_data = json.loads(received_message.message.data.decode('utf-8'))

            tasks.append({
                'task_id': message_data['task_id'],
                'data': message_data['data'],
                'ack_id': received_message.ack_id  # Used to complete/fail the task
            })

        return tasks

    async def complete_task(self, task_handle: Any) -> None:
        """
        Mark a task as completed and remove from the queue.

        Args:
            task_handle: ack_id from receive_tasks
        """
        # Acknowledge the message
        self.subscriber.acknowledge(
            request={
                "subscription": self.subscription_path,
                "ack_ids": [task_handle],
            }
        )

    async def fail_task(self, task_handle: Any) -> None:
        """
        Mark a task as failed, allowing it to be retried.

        Args:
            task_handle: ack_id from receive_tasks
        """
        # Set ack deadline to 0, making the message immediately available
        self.subscriber.modify_ack_deadline(
            request={
                "subscription": self.subscription_path,
                "ack_ids": [task_handle],
                "ack_deadline_seconds": 0,
            }
        )

    async def get_queue_depth(self) -> int:
        """
        Get the current depth (number of messages) in the queue.

        Returns:
            Approximate number of messages in the queue
        """
        # Get subscription details
        subscription = self.subscriber.get_subscription(
            request={"subscription": self.subscription_path}
        )

        # Get unacked message count from subscription
        return subscription.message_retention_duration.seconds

    async def purge_queue(self) -> None:
        """Remove all messages from the queue by recreating the subscription."""
        # Delete and recreate the subscription
        try:
            self.subscriber.delete_subscription(
                request={"subscription": self.subscription_path}
            )
        except Exception:
            pass  # Subscription might not exist

        # Wait a moment for deletion to complete
        time.sleep(2)

        # Recreate subscription
        self.subscriber.create_subscription(
            request={
                "name": self.subscription_path,
                "topic": self.topic_path,
                "message_retention_duration": {"seconds": 7 * 24 * 60 * 60},
                "ack_deadline_seconds": 30,
            }
        )