"""
Google Cloud Pub/Sub implementation of the TaskQueue interface.
"""
import json
import time
import logging
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
        self.logger = logging.getLogger(__name__)

    async def initialize(self, queue_name: str, config: Dict[str, Any]) -> None:
        """
        Initialize the Pub/Sub queue with configuration.

        Args:
            queue_name: Base name for topic and subscription
            config: GCP configuration with project_id and optionally credentials_file
        """
        try:
            self.project_id = config['project_id']
            self.logger.info(f"Initializing GCP Pub/Sub queue with project ID: {self.project_id}")

            credentials_file = config.get('credentials_file')

            # If credentials file provided, use it
            if credentials_file:
                self.logger.info(f"Using credentials from file: {credentials_file}")
                self.publisher = pubsub_v1.PublisherClient.from_service_account_file(credentials_file)
                self.subscriber = pubsub_v1.SubscriberClient.from_service_account_file(credentials_file)
            else:
                # Use default credentials
                self.logger.info("Using default application credentials")
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

            self.logger.info(f"Topic path: {self.topic_path}")
            self.logger.info(f"Subscription path: {self.subscription_path}")

            # Create topic if it doesn't exist
            try:
                self.publisher.get_topic(request={"topic": self.topic_path})
                self.logger.info(f"Topic {self.topic_name} already exists")
            except Exception as e:
                self.logger.info(f"Topic {self.topic_name} doesn't exist, creating it: {str(e)}")
                try:
                    self.publisher.create_topic(request={"name": self.topic_path})
                    self.logger.info(f"Topic {self.topic_name} created successfully")
                except Exception as e:
                    self.logger.error(f"Failed to create topic {self.topic_name}: {str(e)}")
                    raise RuntimeError(f"Failed to create Pub/Sub topic: {str(e)}")

            # Create subscription if it doesn't exist
            try:
                self.subscriber.get_subscription(request={"subscription": self.subscription_path})
                self.logger.info(f"Subscription {self.subscription_name} already exists")
            except Exception as e:
                self.logger.info(f"Subscription {self.subscription_name} doesn't exist, creating it: {str(e)}")
                try:
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
                    self.logger.info(f"Subscription {self.subscription_name} created successfully")
                except Exception as e:
                    self.logger.error(f"Failed to create subscription {self.subscription_name}: {str(e)}")
                    raise RuntimeError(f"Failed to create Pub/Sub subscription: {str(e)}")

        except Exception as e:
            self.logger.error(f"Failed to initialize GCP Pub/Sub queue: {str(e)}")
            raise

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
        try:
            future = self.publisher.publish(
                self.topic_path,
                data=data,
                task_id=task_id
            )

            # Wait for message to be published with timeout
            message_id = future.result(timeout=30)
            self.logger.debug(f"Published message {message_id} for task {task_id}")
        except Exception as e:
            self.logger.error(f"Failed to publish message for task {task_id}: {str(e)}")
            raise RuntimeError(f"Failed to publish task to Pub/Sub: {str(e)}")

    async def receive_tasks(self, max_count: int = 1, visibility_timeout_seconds: int = 30) -> List[Dict[str, Any]]:
        """
        Receive tasks from the Pub/Sub subscription.

        Args:
            max_count: Maximum number of messages to receive
            visibility_timeout_seconds: Duration in seconds for ack deadline

        Returns:
            List of task dictionaries with task_id, data, and ack_id
        """
        try:
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

            self.logger.debug(f"Received {len(tasks)} tasks from subscription")
            return tasks
        except Exception as e:
            self.logger.error(f"Error receiving tasks: {str(e)}")
            return []

    async def complete_task(self, task_handle: Any) -> None:
        """
        Mark a task as completed and remove from the queue.

        Args:
            task_handle: ack_id from receive_tasks
        """
        try:
            # Acknowledge the message
            self.subscriber.acknowledge(
                request={
                    "subscription": self.subscription_path,
                    "ack_ids": [task_handle],
                }
            )
            self.logger.debug(f"Completed task with ack_id: {task_handle}")
        except Exception as e:
            self.logger.error(f"Error completing task: {str(e)}")
            raise

    async def fail_task(self, task_handle: Any) -> None:
        """
        Mark a task as failed, allowing it to be retried.

        Args:
            task_handle: ack_id from receive_tasks
        """
        try:
            # Set ack deadline to 0, making the message immediately available
            self.subscriber.modify_ack_deadline(
                request={
                    "subscription": self.subscription_path,
                    "ack_ids": [task_handle],
                    "ack_deadline_seconds": 0,
                }
            )
            self.logger.debug(f"Failed task with ack_id: {task_handle}")
        except Exception as e:
            self.logger.error(f"Error failing task: {str(e)}")
            raise

    async def get_queue_depth(self) -> int:
        """
        Get the current depth (number of messages) in the queue.

        Returns:
            Approximate number of messages in the queue
        """
        try:
            # Attempt to pull a few messages to check if there are any in the queue
            # without actually processing them
            pull_response = self.subscriber.pull(
                request={
                    "subscription": self.subscription_path,
                    "max_messages": 10,  # Request up to 10 messages to get a sample
                    "return_immediately": True  # Don't block waiting for messages
                }
            )

            # If we received any messages, we need to modify their ack deadline
            # to make them immediately available for regular processing
            if pull_response.received_messages:
                ack_ids = [msg.ack_id for msg in pull_response.received_messages]
                self.subscriber.modify_ack_deadline(
                    request={
                        "subscription": self.subscription_path,
                        "ack_ids": ack_ids,
                        "ack_deadline_seconds": 0  # Make immediately available again
                    }
                )

                # Count how many messages we found
                message_count = len(pull_response.received_messages)
                self.logger.debug(f"Queue depth estimated at {message_count}+ messages")
                return message_count

            # If we didn't receive any messages, the queue might be empty
            self.logger.debug("Queue appears to be empty")
            return 0

        except Exception as e:
            # Log error and return 0 as fallback
            self.logger.error(f"Error getting queue depth: {str(e)}")
            return 0

    async def purge_queue(self) -> None:
        """Remove all messages from the queue by recreating the subscription."""
        try:
            self.logger.info(f"Purging queue {self.subscription_name} by recreating subscription")
            # Delete and recreate the subscription
            try:
                self.subscriber.delete_subscription(
                    request={"subscription": self.subscription_path}
                )
                self.logger.info(f"Deleted subscription {self.subscription_name}")
            except Exception as e:
                self.logger.warning(f"Failed to delete subscription, it might not exist: {str(e)}")
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
            self.logger.info(f"Recreated subscription {self.subscription_name}, queue is now empty")
        except Exception as e:
            self.logger.error(f"Error purging queue: {str(e)}")
            raise