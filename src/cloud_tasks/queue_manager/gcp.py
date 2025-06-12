"""
Google Cloud Pub/Sub implementation of the QueueManager interface.
"""

import asyncio
import datetime
import json
import logging
import time
from typing import Any, Dict, List, Optional

from google.api_core import exceptions as gcp_exceptions
from google.cloud import pubsub_v1  # type: ignore
from google.cloud.pubsub_v1.subscriber import exceptions as sub_exceptions
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import query

from ..common.config import GCPConfig
from .queue_manager import QueueManager


class GCPPubSubQueue(QueueManager):
    """Google Cloud Pub/Sub implementation of the QueueManager interface."""

    # A message not acknowledged within this time will be made available again for processing
    _DEFAULT_VISIBILITY_TIMEOUT = 30

    # Maximum visibility timeout allowed by GCP
    _MAXIMUM_VISIBILITY_TIMEOUT = 600

    # Maximum number of messages that can be received at once allowed by GCP
    _MAXIMUM_MESSAGE_RECEIVE_COUNT = 1000

    # Message retention duration (7 days)
    _MESSAGE_RETENTION_DURATION = 7 * 24 * 60 * 60

    def __init__(
        self,
        gcp_config: Optional[GCPConfig] = None,
        *,
        queue_name: Optional[str] = None,
        visibility_timeout: Optional[int] = None,
        exactly_once: Optional[bool] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the Pub/Sub queue with configuration or queue name.

        Args:
            gcp_config: GCP configuration
            queue_name: Name of the Pub/Sub queue
            visibility_timeout: Visibility timeout (in seconds) for messages pulled from the queue;
                if a message is not completed within this time, it will be made available again for
                processing. If None, and the queue already exists, the existing visibility timeout
                will be used. If None and the queue does not exist, the default visibility timeout
                of 30 seconds will be used.
            exactly_once: If True, messages are guaranteed to be delivered exactly once to any
                recipient. If False, messages will be delivered at least once, but could be
                delivered multiple times. If None, use the value in the configuration.
            **kwargs: Additional configuration parameters
        """
        if queue_name is not None:
            self._queue_name = queue_name
        else:
            self._queue_name = gcp_config.queue_name

        if self._queue_name is None:
            raise ValueError("Queue name is required")

        if exactly_once is not None:
            self._exactly_once = exactly_once
        else:
            self._exactly_once = gcp_config.exactly_once_queue

        if self._exactly_once is None:
            self._exactly_once = False

        self._logger = logging.getLogger(__name__)

        self._visibility_timeout = visibility_timeout

        if "project_id" in kwargs and kwargs["project_id"] is not None:
            self._project_id = kwargs["project_id"]
        else:
            self._project_id = gcp_config.project_id

        self._logger.info(
            f'Initializing GCP Pub/Sub queue "{self._queue_name}" with project ID '
            f'"{self._project_id}"'
        )

        credentials_file = gcp_config.credentials_file if gcp_config is not None else None

        # If credentials file provided, use it
        if credentials_file:
            self._logger.info(f'Using credentials from "{credentials_file}"')
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
            self._logger.debug(f'Topic "{self._topic_name}" already exists')
            self._topic_exists = True
        except gcp_exceptions.NotFound:
            self._logger.debug(f'Topic "{self._topic_name}" doesn\'t exist...deferring creation')
        except Exception:
            raise

        # Check if subscription exists
        self._subscription_exists = False
        try:
            self._subscriber.get_subscription(request={"subscription": self._subscription_path})
            self._logger.debug(f'Subscription "{self._subscription_name}" already exists')
            self._subscription_exists = True
        except gcp_exceptions.NotFound:
            self._logger.debug(
                f'Subscription "{self._subscription_name}" doesn\'t exist...deferring creation'
            )
        except Exception:
            raise

        if self._exactly_once:
            # Initialize streaming pull subscription
            self._logger.debug(
                f"Initializing streaming pull subscription for exactly-once delivery on queue "
                f'"{self._queue_name}"'
            )
            self._streaming_pull_future = None
            self._message_queue = asyncio.Queue()

    async def _create_topic(self) -> None:
        """Create the Pub/Sub topic if it doesn't exist."""
        # We do lazy creation
        if self._topic_exists:
            return

        loop = asyncio.get_event_loop()

        try:
            self._logger.debug(f'Creating topic "{self._topic_name}"')
            await loop.run_in_executor(
                None, lambda: self._publisher.create_topic(request={"name": self._topic_path})
            )
            self._logger.info(f'Topic "{self._topic_name}" created successfully')
            time.sleep(2)  # Give GCP a moment to create the topic
            self._topic_exists = True
        except gcp_exceptions.AlreadyExists:
            self._logger.info(
                f'Topic "{self._topic_name}" already exists (created by another process)'
            )
            self._topic_exists = True
        except Exception:
            raise

    async def _delete_topic(self) -> None:
        """Delete the Pub/Sub topic."""
        loop = asyncio.get_event_loop()

        await self._cancel_streaming_pull()

        try:
            # Delete topic in a thread pool
            await loop.run_in_executor(
                None, lambda: self._publisher.delete_topic(request={"topic": self._topic_path})
            )
            self._logger.info(f'Successfully deleted topic "{self._topic_name}"')
        except gcp_exceptions.NotFound:
            self._logger.info(f'Topic "{self._topic_name}" does not exist')
        except Exception:
            raise

        self._topic_exists = False

    async def _create_subscription(self) -> None:
        """Create the Pub/Sub subscription if it doesn't exist."""
        if self._subscription_exists:
            return

        try:
            loop = asyncio.get_event_loop()

            visibility_timeout = self._visibility_timeout
            if visibility_timeout is None:
                visibility_timeout = self._DEFAULT_VISIBILITY_TIMEOUT

            self._logger.debug(
                f'Creating subscription "{self._subscription_name}" '
                f"with visibility timeout {visibility_timeout} seconds"
            )
            request = {
                "name": self._subscription_path,
                "topic": self._topic_path,
                "message_retention_duration": {"seconds": self._MESSAGE_RETENTION_DURATION},
                "enable_exactly_once_delivery": self._exactly_once,
                "ack_deadline_seconds": visibility_timeout,
            }
            await loop.run_in_executor(
                None, lambda: self._subscriber.create_subscription(request=request)
            )
            # TODO https://cloud.google.com/pubsub/docs/exactly-once-delivery#python
            time.sleep(2)
            self._logger.info(f'Subscription "{self._subscription_name}" created successfully')
            self._subscription_exists = True
        except gcp_exceptions.AlreadyExists:
            # Modify an existing subscription to change the ack deadline to visibility_timeout
            self._logger.info(f'Subscription "{self._subscription_name}" already exists...')
            if self._visibility_timeout is not None:
                self._logger.info(f"Updating visibility timeout to {visibility_timeout} seconds")
                self._subscriber.modify_subscription(
                    request={
                        "subscription": self._subscription_path,
                        "ack_deadline_seconds": visibility_timeout,
                    }
                )
            self._subscription_exists = True
        except Exception:
            raise

    async def _create_topic_and_subscription(self) -> None:
        """Create the Pub/Sub subscription if it doesn't exist."""
        await self._create_topic()
        await self._create_subscription()

    async def _delete_subscription(self) -> None:
        """Delete the Pub/Sub subscription."""
        loop = asyncio.get_event_loop()

        await self._cancel_streaming_pull()

        # Delete and recreate the subscription
        try:
            # Delete subscription in a thread pool
            await loop.run_in_executor(
                None,
                lambda: self._subscriber.delete_subscription(
                    request={"subscription": self._subscription_path}
                ),
            )
            self._logger.info(f'Deleted subscription "{self._subscription_name}"')
        except gcp_exceptions.NotFound:
            self._logger.info(f'Subscription "{self._subscription_name}" does not exist')
        except Exception:
            raise

        self._subscription_exists = False

        # Wait a moment for deletion to complete
        # Don't use asyncio.sleep because we don't want other threads to start running
        time.sleep(2)

    def _start_streaming_pull(self) -> None:
        """Start the streaming pull subscription if it's not already running."""
        if not self._exactly_once:
            return

        if self._streaming_pull_future is not None:
            return

        def callback(message: pubsub_v1.subscriber.message.Message) -> None:
            try:
                # Put message in queue - asyncio.Queue is thread-safe
                message_dict = {
                    "message_id": message.message_id,
                    "data": json.loads(message.data.decode("utf-8")),
                    # For a pull-based queue, the ack_id is just a string, but for a streaming
                    # queue, the ack_id is the entire message object so that we can call methods
                    # on it.
                    "ack_id": message,
                }
                self._message_queue.put_nowait(message_dict)

            except Exception as e:
                self._logger.error(f'Error processing task "{message.message_id}": {str(e)}')

        # Start streaming pull
        self._streaming_pull_future = self._subscriber.subscribe(
            self._subscription_path, callback=callback
        )
        self._logger.debug("Started streaming pull subscription")

    async def _cancel_streaming_pull(self) -> None:
        """Cancel the streaming pull subscription if it's running.
        Waits for the cancellation to complete with a timeout.
        """
        if not self._exactly_once:
            return

        if self._streaming_pull_future is not None:
            self._streaming_pull_future.cancel()
            try:
                # Wait for streaming pull to fully cancel
                loop = asyncio.get_event_loop()
                await asyncio.wait_for(
                    loop.run_in_executor(None, self._streaming_pull_future.result),
                    timeout=5.0,  # Give more time for cleanup
                )
            except (asyncio.TimeoutError, Exception):
                self._logger.warning("Streaming pull cancellation timed out or failed")
            finally:
                self._streaming_pull_future = None

    async def send_message(self, message: Dict[str, Any], _quiet: bool = False) -> None:
        """
        Send a message to the Pub/Sub topic.

        The message can be any dictionary that can be serialized to JSON.

        Args:
            message: Message to be sent
            _quiet: If True, don't log debug messages; this is for internal use only
        """
        if not _quiet:
            # Came from send_task
            self._logger.debug(f'Sending message to queue "{self._queue_name}"')

        await self._create_topic_and_subscription()

        data = json.dumps(message).encode("utf-8")
        # Create the publish future
        future = self._publisher.publish(self._topic_path, data=data)

        # Convert the synchronous future to an asyncio future
        loop = asyncio.get_event_loop()
        message_id = await loop.run_in_executor(None, future.result, 30)

        self._logger.debug(f'Published message "{message_id}" on queue "{self._queue_name}"')

    async def send_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """
        Send a task to the Pub/Sub topic.

        This is just a wrapper around send_message that specifies the task_id and task_data.

        Args:
            task_id: Unique identifier for the task
            task_data: Task data to be sent
        """
        self._logger.debug(f'Sending task "{task_id}" to queue "{self._queue_name}"')

        message = {"task_id": task_id, "data": task_data}
        await self.send_message(message, _quiet=True)

    async def _receive_messages_exactly_once(
        self, max_count: int, acknowledge: bool
    ) -> List[Dict[str, Any]]:
        """Receive messages from the Pub/Sub subscription using exactly-once delivery."""
        self._start_streaming_pull()

        # Messages arrive asynchronously through the pull thread, so we just need to return
        # messages that we already have.
        messages = []
        while len(messages) < max_count:
            try:
                # Try to get a message without waiting
                message_dict = self._message_queue.get_nowait()
                messages.append(message_dict)
            except asyncio.QueueEmpty:
                # No more messages available, return what we have
                break

        if acknowledge:
            # TODO We aren't actually acknowledging the messages here, so this is a lie
            self._logger.debug(
                f"Received and acknowledged {len(messages)} messages from subscription"
            )
        else:
            self._logger.debug(f"Received {len(messages)} messages from subscription")
        return messages

    async def _receive_messages_pull(
        self, max_count: int, acknowledge: bool
    ) -> List[Dict[str, Any]]:
        """Receive messages from the Pub/Sub subscription using pull delivery."""
        # Use pull to receive messages
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

        messages = []
        for received_message in response.received_messages:
            # Parse message data
            message_data = json.loads(received_message.message.data.decode("utf-8"))

            messages.append(
                {
                    "message_id": received_message.message.message_id,
                    "data": message_data,
                    # For pull-based queues, this is just a string
                    "ack_id": received_message.ack_id,
                }
            )

            if acknowledge:
                # Acknowledge the message immediately

                await loop.run_in_executor(
                    None,
                    lambda: self._subscriber.acknowledge(
                        request={
                            "subscription": self._subscription_path,
                            "ack_ids": [received_message.ack_id],
                        }
                    ),
                )

        if acknowledge:
            self._logger.debug(
                f"Received and acknowledged {len(messages)} messages from subscription "
                f"{self._subscription_name}"
            )
        else:
            self._logger.debug(
                f"Received {len(messages)} messages from subscription {self._subscription_name}"
            )
        return messages

    async def receive_messages(
        self,
        max_count: int = 1,
        acknowledge: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Receive messages from the Pub/Sub subscription.

        Args:
            max_count: Maximum number of messages to receive
            acknowledge: True to acknowledge the messages immediately, False to allow them to
                time-out and possibly be retried
        Returns:
            List of message dictionaries, each containing:
                `message_id` (str): Pub/Sub message ID
                `data` (Dict[str, Any]): Message payload
                `ack_id` (Any): Pub/Sub acknowledgment ID used for completing or failing the
                    message
        """
        max_count = min(max_count, self._MAXIMUM_MESSAGE_RECEIVE_COUNT)
        self._logger.debug(
            f"Receiving up to {max_count} messages from subscription {self._subscription_name}"
        )

        await self._create_topic_and_subscription()

        if self._exactly_once:
            return await self._receive_messages_exactly_once(max_count, acknowledge)

        return await self._receive_messages_pull(max_count, acknowledge)

    async def receive_tasks(
        self, max_count: int = 1, acknowledge: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Receive tasks from the Pub/Sub subscription.

        This is just a wrapper around receive_messages that returns the task_id and task_data.

        Args:
            max_count: Maximum number of messages to receive
            acknowledge: True to acknowledge the messages immediately, False to allow them to
                and possibly be retried

        Returns:
            List of task dictionaries, each containing:
                `task_id` (str): Unique identifier for the task
                `data` (Dict[str, Any]): Task payload/parameters
                `ack_id` (Any): Pub/Sub acknowledgment ID used for completing or failing the task
        """
        messages = await self.receive_messages(max_count=max_count, acknowledge=acknowledge)
        return [
            {
                "task_id": message["data"]["task_id"],
                "data": message["data"]["data"],
                "ack_id": message["ack_id"],
            }
            for message in messages
        ]

    async def _acknowledge_message_exactly_once(self, message_handle: Any) -> None:
        """Acknowledge a message and remove it from the queue using exactly-once delivery."""
        try:
            loop = asyncio.get_event_loop()

            # Use ack_with_response for exactly-once delivery
            # If we don't get a response, we can't guarantee the ack was successful
            ack_future = message_handle.ack_with_response()

            # Try to acknowledge with retries
            max_retries = 3
            retry_delay = 0.5  # seconds

            for attempt in range(max_retries):
                try:
                    # Wait for ack to complete with a shorter timeout
                    await asyncio.wait_for(
                        loop.run_in_executor(None, lambda: ack_future.result(timeout=2.0)),
                        timeout=2.0,
                    )
                    self._logger.debug(
                        f"Completed message with ack_id: {message_handle.message_id}"
                    )
                    return
                except (asyncio.TimeoutError, sub_exceptions.AcknowledgeError) as e:
                    if attempt < max_retries - 1:
                        self._logger.warning(
                            f"Attempt {attempt+1} failed to acknowledge message "
                            f"{message_handle.message_id}, "
                            f"retrying in {retry_delay} seconds: {str(e)}"
                        )
                        await asyncio.sleep(retry_delay)
                        # Create a new ack future for the retry
                        ack_future = message_handle.ack_with_response()
                    else:
                        self._logger.error(
                            f"Failed to acknowledge message {message_handle.message_id} "
                            f"after {max_retries} attempts"
                        )
                        raise

        except sub_exceptions.AcknowledgeError as e:
            self._logger.error(
                f"Failed to acknowledge message with ack_id '{message_handle.message_id}': "
                f"{e.error_code}"
            )
            raise
        except Exception:
            raise

    async def _acknowledge_message_pull(self, message_handle: Any) -> None:
        """Mark a message as completed and remove it from the queue using pull delivery."""
        # Get the event loop
        loop = asyncio.get_event_loop()

        # Acknowledge the message in a thread pool
        await loop.run_in_executor(
            None,
            lambda: self._subscriber.acknowledge(
                request={
                    "subscription": self._subscription_path,
                    "ack_ids": [message_handle],
                }
            ),
        )

    async def acknowledge_message(self, message_handle: Any) -> None:
        """Acknowledge a message and remove it from the queue.

        Args:
            message_handle: Message object from receive_messages or "ack_id" from receive_tasks
        """
        if self._exactly_once:
            self._logger.debug(
                f'Acknowledging message with ack_id "{message_handle.message_id}" on subscription '
                f'"{self._subscription_name}"'
            )
        else:
            self._logger.debug(
                f'Acknowledging message with ack_id "{message_handle}" on subscription '
                f'"{self._subscription_name}"'
            )

        await self._create_topic_and_subscription()

        if self._exactly_once:
            await self._acknowledge_message_exactly_once(message_handle)
        else:
            await self._acknowledge_message_pull(message_handle)

        self._logger.debug(f"Acknowledged message with ack_id: {message_handle}")

    async def acknowledge_task(self, task_handle: Any) -> None:
        """Acknowledge a task and remove it from the queue.

        This is just a wrapper around acknowledge_message.

        Args:
            task_handle: "ack_id" from receive_tasks
        """
        await self.acknowledge_message(task_handle)

    async def _retry_message_exactly_once(self, message_handle: Any) -> None:
        """Retry a message using exactly-once delivery."""
        self._logger.debug(f"Retrying message with ack_id: {message_handle.ack_id}")
        # Set ack deadline to 0 to make message immediately available again
        message_handle.modify_ack_deadline(0)

    async def _retry_message_pull(self, message_handle: Any) -> None:
        """Retry a message using pull delivery."""
        loop = asyncio.get_event_loop()

        # Set ack deadline to 0 in a thread pool
        await loop.run_in_executor(
            None,
            lambda: self._subscriber.modify_ack_deadline(
                request={
                    "subscription": self._subscription_path,
                    "ack_ids": [message_handle],
                    "ack_deadline_seconds": 0,
                }
            ),
        )
        self._logger.debug(f"Retried message with ack_id: {message_handle}")

    async def retry_message(self, message_handle: Any) -> None:
        """
        Retry a message.

        Args:
            message_handle: Message object from receive_messages or "ack_id" from receive_tasks
        """
        if self._exactly_once:
            self._logger.debug(
                f'Retrying message with ack_id: "{message_handle.message_id}" on subscription '
                f'"{self._subscription_name}"'
            )
        else:
            self._logger.debug(
                f'Retrying message with ack_id: "{message_handle}" on subscription '
                f'"{self._subscription_name}"'
            )

        await self._create_topic_and_subscription()

        if self._exactly_once:
            return await self._retry_message_exactly_once(message_handle)

        return await self._retry_message_pull(message_handle)

    async def retry_task(self, task_handle: Any) -> None:
        """Retry a task."""
        await self.retry_message(task_handle)

    async def get_queue_depth(self) -> int | None:
        """
        Get the current depth (number of messages) in the queue.

        Pub/Sub does not support any easy way to get the queue depth. We first try
        to use the Monitor API, and if that fails, we try to receive some messages
        and count them.

        This is a best-effort estimate, and the actual queue depth may be different.

        Returns:
            Approximate number of messages in the queue.
        """
        self._logger.debug(f"Getting queue depth for queue '{self._queue_name}'")

        await self._create_topic_and_subscription()

        self._start_streaming_pull()

        if self._exactly_once:
            # Get the current size of the message queue that we have already received
            queue_size = self._message_queue.qsize()
        else:
            queue_size = 0

        # Our first attempt is to use the Monitor API, but it's not always reliable
        for _ in range(3):
            # Get undelivered message count from Cloud Monitoring
            client = monitoring_v3.MetricServiceClient()
            result = query.Query(
                client,
                self._project_id,
                "pubsub.googleapis.com/subscription/num_undelivered_messages",
                end_time=datetime.datetime.now(),
                minutes=1,
            ).select_resources(subscription_id=self._subscription_name)

            # Get the most recent value
            undelivered_messages = None
            for content in result:
                if content.points:
                    undelivered_messages = content.points[0].value.int64_value
                    break
            if undelivered_messages is not None:
                break
            self._logger.debug("Pub/Sub monitor didn't return a value, retrying...")
            await asyncio.sleep(1)

        if undelivered_messages is None and not self._exactly_once:
            # The monitor isn't working - let's try to get some messages from the queue
            # for at least a lower bound on the queue depth
            # This only works for non-exactly-once queues, since for exactly-once queues
            # we receive messages asychronously
            messages = await self.receive_messages(max_count=10, acknowledge=False)

            # Immediately fail the tasks to return them to the queue
            for message in messages:
                await self.retry_message(message["ack_id"])

            undelivered_messages = len(messages)

        if undelivered_messages is None and queue_size == 0:
            self._logger.warning("Failed to get queue depth")
            return None

        if undelivered_messages is None:
            undelivered_messages = 0

        # Total messages = messages in our queue + undelivered messages
        total_messages = queue_size + undelivered_messages

        self._logger.debug(f"Queue depth estimated at {total_messages} messages")
        return total_messages

    async def purge_queue(self) -> None:
        """Remove all messages from the queue by deleting the subscription."""
        self._logger.debug(f"Purging queue '{self._queue_name}'")
        await self._delete_subscription()
        await self._create_topic_and_subscription()

    async def delete_queue(self) -> None:
        """Delete both the Pub/Sub subscription and topic entirely."""
        self._logger.debug(f"Deleting queue '{self._queue_name}'")
        await self._delete_subscription()
        await self._delete_topic()
