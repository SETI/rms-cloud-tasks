# Queue Reader Example

This example demonstrates how to read and display all messages from a cloud queue using the `cloud_tasks` module.

## Overview

The `queue_reader.py` script connects to a cloud provider queue (AWS SQS, GCP Pub/Sub, or Azure Service Bus), retrieves all messages, and displays them on the terminal. By default, it doesn't remove messages from the queue, allowing you to safely inspect queue contents without altering them.

## Features

- Supports AWS, GCP, and Azure cloud providers
- Reads messages in batches for efficiency
- Displays message IDs, receipt handles, and contents
- Option to delete messages after reading them
- Proper error handling and logging

## Prerequisites

1. Python 3.7+
2. Access to a cloud provider queue (AWS SQS, GCP Pub/Sub, or Azure Service Bus)
3. Proper configuration file with cloud provider credentials

## Usage

Run the script with the following command:

```bash
python queue_reader.py --config <config_file> --provider <provider> --queue-name <queue_name> [--delete] [--verbose]
```

### Arguments

- `--config`: Path to the configuration file with cloud provider credentials (required)
- `--provider`: Cloud provider - one of 'aws', 'gcp', or 'azure' (required)
- `--queue-name`: Name of the queue to read messages from (required)
- `--delete`: (Optional) Delete messages after reading them
- `--verbose` or `-v`: (Optional) Enable verbose output for debugging

### Example Commands

Read messages from an AWS SQS queue without deleting them:
```bash
python queue_reader.py --config config.yaml --provider aws --queue-name my-task-queue
```

Read and delete messages from a GCP Pub/Sub queue:
```bash
python queue_reader.py --config config.yaml --provider gcp --queue-name my-task-queue --delete
```

Read messages from an Azure Service Bus queue with verbose logging:
```bash
python queue_reader.py --config config.yaml --provider azure --queue-name my-task-queue --verbose
```

## Configuration File

The script expects a YAML configuration file with cloud provider credentials. Here's an example configuration structure:

```yaml
# Configuration file example
aws:
  access_key: YOUR_AWS_ACCESS_KEY
  secret_key: YOUR_AWS_SECRET_KEY
  region: us-west-2

gcp:
  project_id: YOUR_GCP_PROJECT_ID
  credentials_file: /path/to/gcp-credentials.json

azure:
  tenant_id: YOUR_AZURE_TENANT_ID
  client_id: YOUR_AZURE_CLIENT_ID
  client_secret: YOUR_AZURE_CLIENT_SECRET
  subscription_id: YOUR_AZURE_SUBSCRIPTION_ID
```

See the main repository documentation for more details on configuration options.

## How it Works

1. The script loads the configuration file using the `load_config` function
2. It creates a task queue client for the specified provider
3. It checks the queue depth to determine if there are messages to read
4. It repeatedly calls `receive_tasks` to retrieve batches of messages
5. For each message, it displays the task ID, receipt handle, and data
6. If the `--delete` flag is provided, it calls `complete_task` to remove each message

## Notes

- Messages are not deleted from the queue by default
- The visibility timeout is set to 30 seconds - messages will reappear in the queue after this time
- The script handles the different receipt handle formats for each cloud provider