"""
Azure Service Bus implementation of the TaskQueue interface.
"""

import json
import logging
import asyncio
from typing import Any, Dict, List
from datetime import timedelta

from azure.servicebus import ServiceBusClient, ServiceBusMessage  # type: ignore
from azure.servicebus.management import ServiceBusAdministrationClient  # type: ignore

from .queue_manager import QueueManager
from ..common.config import AzureConfig


class AzureServiceBusQueue(QueueManager):
    """Azure Service Bus implementation of the TaskQueue interface."""

    def __init__(self, azure_config: AzureConfig) -> None:
        """
        Initialize the Azure Service Bus queue with configuration.

        Args:
            queue_name: Name of the Service Bus queue
            config: Azure configuration with tenant_id, client_id, client_secret, and subscription_id
        """
        self._service_bus_client = None
        self._admin_client = None
        self._queue_name = None
        self._connection_string = None
        self._logger = logging.getLogger(__name__)

        try:
            self._queue_name = azure_config.queue_name

            # Construct connection string from config
            tenant_id = azure_config.tenant_id
            client_id = azure_config.client_id
            client_secret = azure_config.client_secret
            namespace_name = azure_config.namespace_name

            # Create connection string using SAS key (assuming it's provided in the config)
            if azure_config.connection_string:
                self._connection_string = azure_config.connection_string
            else:
                self._connection_string = (
                    f"Endpoint=sb://{namespace_name}.servicebus.windows.net/;"
                    f"SharedAccessKeyName=RootManageSharedAccessKey;"
                    f"SharedAccessKey={client_secret}"
                )

            # Create admin client for queue management
            self._admin_client = ServiceBusAdministrationClient.from_connection_string(
                self._connection_string
            )

            # Create service bus client for sending/receiving messages
            self._service_bus_client = ServiceBusClient.from_connection_string(
                conn_str=self._connection_string, logging_enable=True
            )

            # TODO Make lazy queue creation like gcp/aws
            # # Create queue if it doesn't exist
            # try:
            #     # Check if queue exists - Azure SDK uses get_queue rather than queue_exists
            #     await self._admin_client.get_queue(queue_name)
            # except Exception:
            #     # Create the queue if it doesn't exist
            #     await self._admin_client.create_queue(
            #         queue_name,
            #         max_delivery_count=10,  # Number of delivery attempts before dead-letter
            #         lock_duration=timedelta(seconds=30),  # Default lock duration in seconds
            #         max_size_in_megabytes=1024,  # 1GB queue size
            #         requires_duplicate_detection=True,  # Prevent duplicate messages
            #         duplicate_detection_history_time_window=timedelta(
            #             minutes=1
            #         ),  # 1 minute window for duplication detection
            #     )

        except Exception as e:
            self._logger.error(f"Failed to initialize Azure Service Bus queue: {str(e)}")
            raise

    async def send_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """
        Send a task to the Service Bus queue.

        Args:
            task_id: Unique identifier for the task
            task_data: Task data to be processed
        """
        self._logger.debug(f"Sending task '{task_id}' to queue '{self._queue_name}'")

        message = {"task_id": task_id, "data": task_data}
        message_body = json.dumps(message)

        try:
            # Create a Service Bus message with properties
            service_bus_message = ServiceBusMessage(
                body=message_body,
                message_id=task_id,
                content_type="application/json",
                subject="task",
            )

            # Get the event loop
            loop = asyncio.get_event_loop()

            # Send message to queue in a thread pool
            async with self._service_bus_client:
                sender = self._service_bus_client.get_queue_sender(queue_name=self._queue_name)
                await loop.run_in_executor(None, sender.send_messages, service_bus_message)

        except Exception as e:
            self._logger.error(f"Failed to publish message for task {task_id}: {str(e)}")
            raise RuntimeError(f"Failed to publish task to Azure Service Bus: {str(e)}")

    async def receive_tasks(
        self,
        max_count: int = 1,
        visibility_timeout: int = 30,  # TODO Default visibility timeout in seconds
    ) -> List[Dict[str, Any]]:
        """
        Receive tasks from the Service Bus queue with a lock.

        Args:
            max_count: Maximum number of messages to receive
            visibility_timeout: Duration in seconds for message lock

        Returns:
            List of task dictionaries with task_id, data, and lock_token
        """
        self._logger.debug(f"Receiving up to {max_count} tasks from queue '{self._queue_name}'")

        try:
            tasks = []
            loop = asyncio.get_event_loop()

            # Create receiver for the queue
            async with self._service_bus_client:
                receiver = self._service_bus_client.get_queue_receiver(
                    queue_name=self._queue_name, max_wait_time=10
                )

                # Receive messages in a thread pool
                received_messages = await loop.run_in_executor(
                    None,
                    lambda: receiver.receive_messages(max_message_count=max_count, max_wait_time=5),
                )

                for message in received_messages:
                    # Parse message body
                    message_body = json.loads(message.body.decode("utf-8"))

                    # Renew lock with the specified visibility timeout in a thread pool
                    await loop.run_in_executor(
                        None,
                        lambda: receiver.renew_message_lock(message, timeout=visibility_timeout),
                    )

                    tasks.append(
                        {
                            "task_id": message_body["task_id"],
                            "data": message_body["data"],
                            "lock_token": message.lock_token,  # Used to complete/fail the task
                        }
                    )

            return tasks
        except Exception as e:
            self._logger.error(f"Error receiving tasks: {str(e)}")
            return []

    async def acknowledge_task(self, task_handle: Any) -> None:
        """
        Mark a task as completed and remove from the queue.

        Args:
            task_handle: lock_token from receive_tasks
        """
        self._logger.debug(
            f"Completing task with lock_token '{task_handle}' on queue '{self._queue_name}'"
        )

        try:
            loop = asyncio.get_event_loop()

            async with self._service_bus_client:
                receiver = self._service_bus_client.get_queue_receiver(queue_name=self._queue_name)
                # Complete the message using its lock token in a thread pool
                await loop.run_in_executor(None, receiver.acknowledge_message, task_handle)
        except Exception as e:
            self._logger.error(f"Error completing task: {str(e)}")
            raise

    async def retry_task(self, task_handle: Any) -> None:
        """
        Mark a task as failed, allowing it to be retried.

        Args:
            task_handle: lock_token from receive_tasks
        """
        self._logger.debug(
            f"Failing task with lock_token '{task_handle}' on queue '{self._queue_name}'"
        )

        try:
            loop = asyncio.get_event_loop()

            async with self._service_bus_client:
                receiver = self._service_bus_client.get_queue_receiver(queue_name=self._queue_name)
                # Abandon the message in a thread pool
                await loop.run_in_executor(None, receiver.abandon_message, task_handle)
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

        try:
            loop = asyncio.get_event_loop()

            # Get queue runtime properties in a thread pool
            queue_properties = await loop.run_in_executor(
                None, lambda: self._admin_client.get_queue_runtime_properties(self._queue_name)
            )

            return queue_properties.active_message_count

        except Exception as e:
            self._logger.error(f"Error getting queue depth: {str(e)}")
            return 0

    async def purge_queue(self) -> None:
        """Remove all messages from the queue by deleting and recreating it."""
        self._logger.debug(f"Purging queue '{self._queue_name}'")

        try:
            loop = asyncio.get_event_loop()

            # Delete the queue if it exists in a thread pool
            await loop.run_in_executor(
                None, lambda: self._admin_client.delete_queue(self._queue_name)
            )

            # Wait a moment for deletion to complete
            await asyncio.sleep(2)

            # Create a new queue with the same properties in a thread pool
            await loop.run_in_executor(
                None,
                lambda: self._admin_client.create_queue(
                    self._queue_name,
                    max_delivery_count=10,
                    lock_duration=timedelta(seconds=30),
                    max_size_in_megabytes=1024,
                    requires_duplicate_detection=True,
                    duplicate_detection_history_time_window=timedelta(minutes=1),
                ),
            )
        except Exception as e:
            self._logger.error(f"Error purging queue: {str(e)}")
            raise

    async def delete_queue(self) -> None:
        """Delete the Service Bus queue entirely."""
        self._logger.debug(f"Deleting queue '{self._queue_name}'")

        try:
            loop = asyncio.get_event_loop()

            # Delete the queue in a thread pool
            await loop.run_in_executor(
                None, lambda: self._admin_client.delete_queue(self._queue_name)
            )
            self._logger.info(f"Successfully deleted queue {self._queue_name}")
        except Exception as e:
            self._logger.error(f"Error deleting queue: {str(e)}")
            raise
