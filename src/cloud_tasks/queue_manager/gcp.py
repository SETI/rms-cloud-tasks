"""
Google Cloud Pub/Sub implementation of the TaskQueue interface.
"""

import json
import logging
import time
import asyncio
from typing import Any, Dict, List, Optional

from google.cloud import pubsub_v1  # type: ignore
from google.api_core import exceptions as gcp_exceptions

from cloud_tasks.common.config import GCPConfig

from .taskqueue import TaskQueue


class GCPPubSubQueue(TaskQueue):
    """Google Cloud Pub/Sub implementation of the TaskQueue interface."""

    def __init__(
        self,
        gcp_config: Optional[GCPConfig] = None,
        queue_name: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the Pub/Sub queue with configuration.

        Args:
            config: GCP configuration
        """
        if queue_name is not None:
            self._queue_name = queue_name
        else:
            self._queue_name = gcp_config.queue_name

        if self._queue_name is None:
            raise ValueError("Queue name is required")

        self._logger = logging.getLogger(__name__)

        if "project_id" in kwargs and kwargs["project_id"] is not None:
            self._project_id = kwargs["project_id"]
        else:
            self._project_id = gcp_config.project_id

        self._logger.info(
            f"Initializing GCP Pub/Sub queue '{self._queue_name}' with project ID "
            f"'{self._project_id}'"
        )

        credentials_file = gcp_config.credentials_file if gcp_config is not None else None

        # If credentials file provided, use it
        if credentials_file:
            self._logger.info(f"Using credentials from '{credentials_file}'")
            self._publisher = pubsub_v1.PublisherClient.from_service_account_file(credentials_file)
            self._subscriber = pubsub_v1.SubscriberClient.from_service_account_file(
                credentials_file
            )
        else:
            # Use default credentials
            self._logger.info("Using default application credentials")
            self._publisher = pubsub_v1.PublisherClient()
            self._subscriber = pubsub_v1.SubscriberClient()

        # Derive topic and subscription names
        self._topic_name = f"{self._queue_name}-topic"
        self._subscription_name = f"{self._queue_name}-subscription"

        # Create fully qualified paths
        self._topic_path = self._publisher.topic_path(self._project_id, self._topic_name)
        self._subscription_path = self._subscriber.subscription_path(
            self._project_id, self._subscription_name
        )

        self._logger.debug(f"Topic path: {self._topic_path}")
        self._logger.debug(f"Subscription path: {self._subscription_path}")

        # Check if topic exists
        self._topic_exists = False
        try:
            self._publisher.get_topic(request={"topic": self._topic_path})
            self._logger.debug(f"Topic '{self._topic_name}' already exists")
            self._topic_exists = True
        except gcp_exceptions.NotFound as e:
            self._logger.debug(f"Topic '{self._topic_name}' doesn't exist...deferring creation")
        except Exception as e:
            # self._logger.error(
            #     f"Failed to access topic '{self._topic_name}' for Pub/Sub queue "
            #     f"'{self._queue_name}': {str(e)}",
            #     exc_info=True,
            # )
            raise

        # Check if subscription exists
        self._subscription_exists = False
        try:
            self._subscriber.get_subscription(request={"subscription": self._subscription_path})
            self._logger.debug(f"Subscription {self._subscription_name} already exists")
            self._subscription_exists = True
        except gcp_exceptions.NotFound as e:
            self._logger.debug(
                f"Subscription {self._subscription_name} doesn't exist...deferring creation"
            )
        except Exception as e:
            # self._logger.error(
            #     f"Failed to access subscription '{self._subscription_name}' for Pub/Sub "
            #     f"queue '{self._queue_name}': {str(e)}",
            #     exc_info=True,
            # )
            raise

    def _create_topic_subscription(self) -> None:
        """Create the Pub/Sub topic and subscription if they don't exist."""
        if not self._topic_exists:
            try:
                self._logger.debug(f"Creating topic '{self._topic_name}'")
                self._publisher.create_topic(request={"name": self._topic_path})
                self._logger.info(f"Topic '{self._topic_name}' created successfully")
                self._topic_exists = True
            except gcp_exceptions.AlreadyExists:
                self._logger.info(
                    f"Topic '{self._topic_name}' already exists (created by another process)"
                )
                self._topic_exists = True
            except Exception as e:
                # self._logger.error(
                #     f"Failed to create topic '{self._topic_name}' for Pub/Sub queue "
                #     f"'{self._queue_name}': {str(e)}",
                #     exc_info=True,
                # )
                raise

        if not self._subscription_exists:
            try:
                self._logger.debug(f"Creating subscription '{self._subscription_name}'")
                self._subscriber.create_subscription(
                    request={
                        "name": self._subscription_path,
                        "topic": self._topic_path,
                        # Set message retention to maximum (7 days)
                        "message_retention_duration": {"seconds": 7 * 24 * 60 * 60},
                        # Default ack deadline (30 seconds)
                        "ack_deadline_seconds": 30,  # TODO Default ack deadline in seconds
                    }
                )
                self._logger.info(f"Subscription '{self._subscription_name}' created successfully")
                self._subscription_exists = True
            except gcp_exceptions.AlreadyExists:
                self._logger.info(
                    f"Subscription '{self._subscription_name}' already exists (created by another process)"
                )
                self._subscription_exists = True
            except Exception as e:
                # self._logger.error(
                #     f"Failed to create subscription '{self._subscription_name}' for Pub/Sub "
                #     f"queue '{self._queue_name}': {str(e)}",
                #     exc_info=True,
                # )
                raise

    async def send_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """
        Send a task to the Pub/Sub topic.

        Args:
            task_id: Unique identifier for the task
            task_data: Task data to be sent
        """
        self._logger.debug(f"Sending task '{task_id}' to queue '{self._queue_name}'")

        self._create_topic_subscription()

        message = {"task_id": task_id, "data": task_data}

        # Convert message to JSON string and encode as bytes
        data = json.dumps(message).encode("utf-8")

        try:
            # Create the publish future
            future = self._publisher.publish(self._topic_path, data=data, task_id=task_id)

            # Convert the synchronous future to an asyncio future
            loop = asyncio.get_event_loop()
            message_id = await loop.run_in_executor(None, future.result, 30)

            self._logger.debug(
                f"Published message '{message_id}' for task '{task_id}' on queue "
                f"'{self._queue_name}'"
            )
        except Exception as e:
            # self._logger.error(
            #     f"Failed to publish task '{task_id}' to Pub/Sub on queue "
            #     f"'{self._queue_name}': {str(e)}",
            #     exc_info=True,
            # )
            raise

    async def receive_tasks(
        self,
        max_count: int = 1,
        visibility_timeout_seconds: int = 30,
    ) -> List[Dict[str, Any]]:
        """
        Receive tasks from the Pub/Sub subscription.

        Args:
            max_count: Maximum number of messages to receive
            visibility_timeout_seconds: Duration in seconds for ack deadline

        Returns:
            List of task dictionaries, each containing:
                - 'task_id' (str): Unique identifier for the task
                - 'data' (Dict[str, Any]): Task payload/parameters
                - 'ack_id' (str): Pub/Sub acknowledgment ID used for completing or failing the task
        """
        self._logger.debug(f"Receiving up to {max_count} tasks from queue '{self._queue_name}'")

        self._create_topic_subscription()

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Pull messages from the subscription in a thread pool
            response = await loop.run_in_executor(
                None,
                lambda: self._subscriber.pull(
                    request={
                        "subscription": self._subscription_path,
                        "max_messages": max_count,
                    }
                ),
            )

            tasks = []
            for received_message in response.received_messages:
                # Modify the ack deadline for this message in a thread pool
                await loop.run_in_executor(
                    None,
                    lambda: self._subscriber.modify_ack_deadline(
                        request={
                            "subscription": self._subscription_path,
                            "ack_ids": [received_message.ack_id],
                            "ack_deadline_seconds": visibility_timeout_seconds,
                        }
                    ),
                )

                # Parse message data
                message_data = json.loads(received_message.message.data.decode("utf-8"))

                tasks.append(
                    {
                        "task_id": message_data["task_id"],
                        "data": message_data["data"],
                        "ack_id": received_message.ack_id,  # Used to complete/fail the task
                    }
                )

            self._logger.debug(f"Received {len(tasks)} tasks from subscription")
            return tasks
        except Exception as e:
            # self._logger.error(
            #     f"Error receiving tasks from Pub/Sub queue '{self._queue_name}': {str(e)}",
            #     exc_info=True,
            # )
            raise

    async def complete_task(self, task_handle: Any) -> None:
        """
        Mark a task as completed and remove from the queue.

        Args:
            task_handle: ack_id from receive_tasks
        """
        self._logger.debug(
            f"Completing task with ack_id '{task_handle}' on queue '{self._queue_name}'"
        )

        self._create_topic_subscription()

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Acknowledge the message in a thread pool
            await loop.run_in_executor(
                None,
                lambda: self._subscriber.acknowledge(
                    request={
                        "subscription": self._subscription_path,
                        "ack_ids": [task_handle],
                    }
                ),
            )
            self._logger.debug(f"Completed task with ack_id: {task_handle}")
        except Exception as e:
            # self._logger.error(
            #     f"Error completing task with ack_id '{task_handle}' on Pub/Subqueue "
            #     f"'{self._queue_name}': {str(e)}",
            #     exc_info=True,
            # )
            raise

    async def fail_task(self, task_handle: Any) -> None:
        """
        Mark a task as failed, allowing it to be retried.

        Args:
            task_handle: ack_id from receive_tasks
        """
        self._logger.debug(
            f"Failing task with ack_id: '{task_handle}' on queue '{self._queue_name}'"
        )

        self._create_topic_subscription()

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Set ack deadline to 0 in a thread pool
            await loop.run_in_executor(
                None,
                lambda: self._subscriber.modify_ack_deadline(
                    request={
                        "subscription": self._subscription_path,
                        "ack_ids": [task_handle],
                        "ack_deadline_seconds": 0,
                    }
                ),
            )
            self._logger.debug(f"Failed task with ack_id: {task_handle}")
        except Exception as e:
            # self._logger.error(
            #     f"Error failing task with ack_id '{task_handle}' on Pub/Sub queue "
            #     f"'{self._queue_name}': {str(e)}",
            #     exc_info=True,
            # )
            raise

    async def get_queue_depth(self) -> int:
        """
        Get the current depth (number of messages) in the queue.

        Returns:
            Approximate number of messages in the queue
        """
        self._logger.debug(f"Getting queue depth for queue '{self._queue_name}'")

        self._create_topic_subscription()

        try:
            # Get the event loop
            loop = asyncio.get_event_loop()

            # Attempt to pull a few messages in a thread pool
            pull_response = await loop.run_in_executor(
                None,
                lambda: self._subscriber.pull(
                    request={
                        "subscription": self._subscription_path,
                        "max_messages": 10,  # Request up to 10 messages to get a sample
                        "return_immediately": True,  # Don't block waiting for messages
                    }
                ),
            )

            # If we received any messages, we need to modify their ack deadline
            # to make them immediately available for regular processing
            if pull_response.received_messages:
                ack_ids = [msg.ack_id for msg in pull_response.received_messages]

                # Modify ack deadline in a thread pool
                await loop.run_in_executor(
                    None,
                    lambda: self._subscriber.modify_ack_deadline(
                        request={
                            "subscription": self._subscription_path,
                            "ack_ids": ack_ids,
                            "ack_deadline_seconds": 0,  # Make immediately available again
                        }
                    ),
                )

                # Count how many messages we found
                message_count = len(pull_response.received_messages)
                self._logger.debug(f"Queue depth estimated at {message_count}+ messages")
                return message_count

            # If we didn't receive any messages, the queue might be empty
            self._logger.debug(f"Queue '{self._queue_name}' appears to be empty")
            return 0

        except Exception as e:
            # Log error and return 0 as fallback
            # self._logger.error(
            #     f"Error getting queue depth for Pub/Sub queue '{self._queue_name}': {str(e)}",
            #     exc_info=True,
            # )
            raise

    async def purge_queue(self) -> None:
        """Remove all messages from the queue by recreating the subscription."""
        self._logger.debug(f"Purging queue '{self._queue_name}'")

        # Get the event loop
        loop = asyncio.get_event_loop()

        # Delete and recreate the subscription
        try:
            # Delete subscription in a thread pool
            await loop.run_in_executor(
                None,
                lambda: self._subscriber.delete_subscription(
                    request={"subscription": self._subscription_path}
                ),
            )
            self._logger.info(f"Deleted subscription {self._subscription_name}")
        except Exception as e:
            # self._logger.error(
            #     f"Failed to delete subscription '{self._subscription_name}' for Pub/Sub "
            #     f"queue '{self._queue_name}': {str(e)}"
            # )
            raise

        # Wait a moment for deletion to complete
        await asyncio.sleep(2)  # Use asyncio.sleep instead of time.sleep

        try:
            # Recreate subscription in a thread pool
            await loop.run_in_executor(
                None,
                lambda: self._subscriber.create_subscription(
                    request={
                        "name": self._subscription_path,
                        "topic": self._topic_path,
                        "message_retention_duration": {"seconds": 7 * 24 * 60 * 60},
                        "ack_deadline_seconds": 30,
                    }
                ),
            )
            self._logger.info(
                f"Recreated subscription '{self._subscription_name}' for Pub/Sub queue "
                f"'{self._queue_name}', queue is now empty"
            )
        except Exception as e:
            # self._logger.error(
            #     f"Error recreating subscription '{self._subscription_name}' for Pub/Sub "
            #     f"queue '{self._queue_name}': {str(e)}",
            #     exc_info=True,
            # )
            raise

    async def delete_queue(self) -> None:
        """Delete both the Pub/Sub subscription and topic entirely."""
        self._logger.debug(f"Deleting queue '{self._queue_name}'")

        # Get the event loop
        loop = asyncio.get_event_loop()

        # Delete subscription first
        try:
            # Delete subscription in a thread pool
            await loop.run_in_executor(
                None,
                lambda: self._subscriber.delete_subscription(
                    request={"subscription": self._subscription_path}
                ),
            )
            self._logger.info(f"Successfully deleted subscription {self._subscription_name}")
            self._subscription_exists = False
        except gcp_exceptions.NotFound as e:
            self._logger.info(f"Subscription '{self._subscription_name}' does not exist")
        except Exception as e:
            # self._logger.error(
            #     f"Error deleting subscription '{self._subscription_name}' for Pub/Sub "
            #     f"queue '{self._queue_name}': {str(e)}"
            # )
            raise

        # Then delete the topic
        try:
            # Delete topic in a thread pool
            await loop.run_in_executor(
                None, lambda: self._publisher.delete_topic(request={"topic": self._topic_path})
            )
            self._logger.info(f"Successfully deleted topic {self._topic_name}")
            self._topic_exists = False
        except gcp_exceptions.NotFound as e:
            self._logger.info(f"Topic {self._topic_name} does not exist")
        except Exception as e:
            # self._logger.error(
            #     f"Error deleting topic '{self._topic_name}' for Pub/Sub queue "
            #     f"'{self._queue_name}': {str(e)}"
            # )
            raise
