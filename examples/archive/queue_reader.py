#!/usr/bin/env python3
"""
Simple example program that reads all messages from a queue and displays them on the terminal.

Usage:
    python queue_reader.py --config config.yaml --provider aws --queue-name your-queue-name

This will read and display all messages from the specified queue without removing them
from the queue by default. Use the --delete flag to delete messages after reading them.
"""

import argparse
import asyncio
import json
import logging
import sys
from typing import Dict, Any, List

import yaml

from cloud_tasks.common.config import load_config, ConfigError
from cloud_tasks.queue_manager import create_queue
from cloud_tasks.common.logging_config import configure_logging

# Configure logging
configure_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


async def read_queue(args: argparse.Namespace) -> None:
    """
    Read all messages from a queue and display them.

    Args:
        args: Command-line arguments
    """
    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}")
        config = load_config(args.config)

        # Create task queue
        logger.info(f"Connecting to {args.provider} queue: {args.queue_name}")
        task_queue = await create_queue(
            provider=args.provider,
            queue_name=args.queue_name,
            config=config
        )

        # Get queue depth
        queue_depth = await task_queue.get_queue_depth()
        logger.info(f"Queue depth: {queue_depth}")

        if queue_depth == 0:
            logger.info("Queue is empty. No messages to display.")
            return

        # Read all messages
        total_messages = 0
        batch_size = min(10, queue_depth)  # Process in batches of 10 or less
        visibility_timeout = 30  # Hide messages for 30 seconds while processing

        logger.info(f"Reading messages from queue (batch size: {batch_size})...")
        print("\n" + "="*80)
        print(f"MESSAGES FROM {args.provider.upper()} QUEUE: {args.queue_name}")
        print("="*80)

        while True:
            # Receive a batch of messages
            messages = await task_queue.receive_tasks(
                max_count=batch_size,
                visibility_timeout_seconds=visibility_timeout
            )

            if not messages:
                # No more messages
                break

            # Display the messages
            for i, message in enumerate(messages, 1):
                total_messages += 1
                task_id = message.get('task_id', 'unknown')
                receipt_handle = None

                # Handle different task_handle names from different providers
                if 'receipt_handle' in message:  # AWS SQS
                    receipt_handle = message['receipt_handle']
                elif 'ack_id' in message:  # GCP Pub/Sub
                    receipt_handle = message['ack_id']
                elif 'lock_token' in message:  # Azure Service Bus
                    receipt_handle = message['lock_token']

                # Pretty-print the message data
                print(f"\nMESSAGE {total_messages}:")
                print(f"Task ID: {task_id}")
                print(f"Receipt Handle: {receipt_handle[:20]}..." if receipt_handle and len(receipt_handle) > 20 else f"Receipt Handle: {receipt_handle}")
                print("Data:")

                try:
                    # Pretty print the data if it's a dictionary
                    data = message.get('data', {})
                    if isinstance(data, dict):
                        print(json.dumps(data, indent=2))
                    else:
                        print(data)
                except Exception as e:
                    print(f"Error displaying data: {e}")
                    print(message.get('data', {}))

                # Delete the message if requested
                if args.delete and receipt_handle:
                    try:
                        await task_queue.complete_task(receipt_handle)
                        print("Message deleted from queue.")
                    except Exception as e:
                        logger.error(f"Error deleting message: {e}")
                        print(f"Error: Could not delete message: {e}")

            # If we received fewer messages than the batch size, we're done
            if len(messages) < batch_size:
                break

        print("\n" + "="*80)
        print(f"TOTAL MESSAGES READ: {total_messages}")
        print("="*80)

        if not args.delete:
            logger.info("Messages were not deleted from the queue. Use --delete to remove them after reading.")
        else:
            logger.info(f"Messages were deleted from the queue.")

    except ConfigError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading queue: {e}")
        sys.exit(1)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Read and display messages from a queue')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--provider', choices=['aws', 'gcp', 'azure'], required=True, help='Cloud provider')
    parser.add_argument('--queue-name', required=True, help='Name of the task queue')
    parser.add_argument('--delete', action='store_true', help='Delete messages after reading them')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')

    args = parser.parse_args()

    # Set up logging level based on verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Run the queue reader
    asyncio.run(read_queue(args))


if __name__ == '__main__':
    main()