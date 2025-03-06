"""
Azure Service Bus implementation of the TaskQueue interface.
"""
import json
from typing import Any, Dict, List, Optional
from datetime import timedelta

from azure.servicebus import ServiceBusClient, ServiceBusMessage  # type: ignore
from azure.servicebus.management import ServiceBusAdministrationClient  # type: ignore

from cloud_tasks.common.base import TaskQueue


class AzureServiceBusQueue(TaskQueue):
    """Azure Service Bus implementation of the TaskQueue interface."""

    def __init__(self):
        self.service_bus_client = None
        self.admin_client = None
        self.queue_name = None
        self.connection_string = None

    async def initialize(self, queue_name: str, config: Dict[str, Any]) -> None:
        """
        Initialize the Azure Service Bus queue with configuration.

        Args:
            queue_name: Name of the Service Bus queue
            config: Azure configuration with tenant_id, client_id, client_secret, and subscription_id
        """
        self.queue_name = queue_name

        # Construct connection string from config
        tenant_id = config['tenant_id']
        client_id = config['client_id']
        client_secret = config['client_secret']
        namespace_name = config.get('namespace_name', f"{queue_name}-namespace")

        # Create connection string using SAS key (assuming it's provided in the config)
        # In a real implementation, you would get this from authentication or Azure SDK
        if 'connection_string' in config:
            self.connection_string = config['connection_string']
        else:
            # This is a simplified example - in production, you would generate this properly
            # using Azure identity libraries
            self.connection_string = (
                f"Endpoint=sb://{namespace_name}.servicebus.windows.net/;"
                f"SharedAccessKeyName=RootManageSharedAccessKey;"
                f"SharedAccessKey={client_secret}"
            )

        # Create admin client for queue management
        self.admin_client = ServiceBusAdministrationClient.from_connection_string(
            self.connection_string
        )

        # Create service bus client for sending/receiving messages
        self.service_bus_client = ServiceBusClient.from_connection_string(
            conn_str=self.connection_string,
            logging_enable=True
        )

        # Create queue if it doesn't exist
        try:
            # Check if queue exists - Azure SDK uses get_queue rather than queue_exists
            self.admin_client.get_queue(queue_name)
        except Exception:
            # Create the queue if it doesn't exist
            self.admin_client.create_queue(
                queue_name,
                max_delivery_count=10,  # Number of delivery attempts before dead-letter
                lock_duration=timedelta(seconds=30),  # Default lock duration in seconds
                max_size_in_megabytes=1024,  # 1GB queue size
                requires_duplicate_detection=True,  # Prevent duplicate messages
                duplicate_detection_history_time_window=timedelta(minutes=1)  # 1 minute window for duplication detection
            )

    async def send_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """
        Send a task to the Service Bus queue.

        Args:
            task_id: Unique identifier for the task
            task_data: Task data to be processed
        """
        message = {
            'task_id': task_id,
            'data': task_data
        }

        # Convert message to JSON string
        message_body = json.dumps(message)

        # Create a Service Bus message with properties
        service_bus_message = ServiceBusMessage(
            body=message_body,
            message_id=task_id,
            content_type='application/json',
            subject='task',
        )

        # Send message to queue
        with self.service_bus_client.get_queue_sender(queue_name=self.queue_name) as sender:
            sender.send_messages(service_bus_message)

    async def receive_tasks(self, max_count: int = 1, visibility_timeout_seconds: int = 30) -> List[Dict[str, Any]]:
        """
        Receive tasks from the Service Bus queue with a lock.

        Args:
            max_count: Maximum number of messages to receive
            visibility_timeout_seconds: Duration in seconds for message lock

        Returns:
            List of task dictionaries with task_id, data, and lock_token
        """
        tasks = []

        # Create receiver for the queue
        with self.service_bus_client.get_queue_receiver(
            queue_name=self.queue_name,
            max_wait_time=10  # 10 seconds max wait time
        ) as receiver:
            # Receive up to max_count messages
            received_messages = receiver.receive_messages(
                max_message_count=max_count,
                max_wait_time=5  # Wait up to 5 seconds for messages
            )

            for message in received_messages:
                # Parse message body
                message_body = json.loads(message.body.decode('utf-8'))

                # Renew lock with the specified visibility timeout
                # Note: Azure Service Bus takes lock renewal in seconds
                receiver.renew_message_lock(message, timeout=visibility_timeout_seconds)

                tasks.append({
                    'task_id': message_body['task_id'],
                    'data': message_body['data'],
                    'lock_token': message.lock_token  # Used to complete/fail the task
                })

        return tasks

    async def complete_task(self, task_handle: Any) -> None:
        """
        Mark a task as completed and remove from the queue.

        Args:
            task_handle: lock_token from receive_tasks
        """
        with self.service_bus_client.get_queue_receiver(queue_name=self.queue_name) as receiver:
            # Complete the message using its lock token
            receiver.complete_message(task_handle)

    async def fail_task(self, task_handle: Any) -> None:
        """
        Mark a task as failed, allowing it to be retried.

        Args:
            task_handle: lock_token from receive_tasks
        """
        with self.service_bus_client.get_queue_receiver(queue_name=self.queue_name) as receiver:
            # Abandon the message, making it available for immediate reprocessing
            receiver.abandon_message(task_handle)

    async def get_queue_depth(self) -> int:
        """
        Get the current depth (number of messages) in the queue.

        Returns:
            Approximate number of messages in the queue
        """
        # Get queue runtime properties
        queue_properties = self.admin_client.get_queue_runtime_properties(self.queue_name)

        # Return active message count
        return queue_properties.active_message_count

    async def purge_queue(self) -> None:
        """Remove all messages from the queue by deleting and recreating it."""
        # Delete the queue if it exists
        try:
            # Check if queue exists before deleting
            self.admin_client.get_queue(self.queue_name)
            self.admin_client.delete_queue(self.queue_name)

            # Create a new queue with the same properties
            self.admin_client.create_queue(
                self.queue_name,
                max_delivery_count=10,
                lock_duration=timedelta(seconds=30),
                max_size_in_megabytes=1024,
                requires_duplicate_detection=True,
                duplicate_detection_history_time_window=timedelta(minutes=1)
            )
        except Exception:
            # Queue doesn't exist or another error occurred
            pass