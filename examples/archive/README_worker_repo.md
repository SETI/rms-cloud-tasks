# Number Adder Worker

This worker processes tasks that require adding two numbers together. Each task is processed independently, making it well-suited for distributed cloud computing.

## How It Works

1. The worker receives a task from a queue
2. It extracts the two numbers (`num1` and `num2`) from the task data
3. It adds the numbers together
4. It writes the result to a file named `{task_id}.out` in the `results` directory

## Task Format

Each task should have the following format:

```json
{
  "id": "unique-task-id",
  "data": {
    "num1": 42,
    "num2": 58
  }
}
```

## Implementations

This repository contains three worker implementations:

### 1. Simple Worker (worker.py)

A straightforward implementation that relies on the cloud_tasks framework to:
- Pull code from GitHub
- Connect to cloud queues
- Handle scheduling and scaling

### 2. Cloud-Native Worker (worker_cloud.py)

A more advanced implementation that directly integrates with cloud services:
- Connects directly to cloud provider queues (SQS, Pub/Sub, Service Bus)
- Handles spot/preemptible instance termination notices
- Uploads results to cloud storage (S3, GCS, Azure Blob)
- Includes retry logic and robust error handling
- Performs graceful shutdown when instances are scheduled for termination

### 3. Adapted Worker (worker_adapted.py)

A demonstration of how to adapt existing worker code to use the cloud task adapter module:
- Separates cloud integration code from business logic
- Uses the `cloud_tasks.worker` module to handle cloud-specific functionality
- Minimal changes to existing worker code
- Supports all cloud providers through a unified interface

## Requirements

- Python 3.8+
- Dependencies listed in `requirements.txt`

## Usage

### Using with the cloud_tasks System

The worker is designed to be used with the cloud_tasks system, which will:

1. Pull this repository on worker startup
2. Set up a configuration file
3. Run `worker.py --config=/path/to/config.json`
4. Handle task polling and completion

### Using the Cloud-Native Worker

The cloud-native worker can be deployed directly to cloud instances:

```bash
# Deploy to an AWS EC2 instance
python worker_cloud.py --config=/path/to/cloud_config.json
```

The cloud configuration should include provider details, credentials, and queue information:

```json
{
  "provider": "aws",
  "job_id": "adder-job-001",
  "queue_name": "number-adder-queue",
  "result_bucket": "task-results-bucket",
  "result_prefix": "additions",
  "config": {
    "access_key": "YOUR_AWS_ACCESS_KEY",
    "secret_key": "YOUR_AWS_SECRET_KEY",
    "region": "us-west-2"
  }
}
```

## Local Testing

### Simple Worker

To test the simple worker locally:

```bash
# Create a test config file
cat > test_config.json << EOF
{
  "sample_task": {
    "id": "test-task",
    "data": {
      "num1": 42,
      "num2": 58
    }
  }
}
EOF

# Run the worker
python worker.py --config=test_config.json
```

### Cloud-Native Worker

To test the cloud-native worker with a sample task:

```bash
# Create a test config file
cat > test_cloud_config.json << EOF
{
  "provider": "aws",
  "job_id": "test-job",
  "queue_name": "test-queue",
  "sample_task": {
    "id": "test-task",
    "data": {
      "num1": 42,
      "num2": 58
    }
  }
}
EOF

# Run the cloud worker
python worker_cloud.py --config=test_cloud_config.json
```

This should create a file `results/test-task.out` containing the result `100`.

### Adapted Worker

To test the adapted worker with a sample task:

```bash
# Create a test config file
cat > test_adapted_config.json << EOF
{
  "provider": "aws",
  "job_id": "test-job",
  "queue_name": "test-queue",
  "sample_task": {
    "task_id": "test-task",
    "num1": 42,
    "num2": 58
  }
}
EOF

# Run the adapted worker
python worker_adapted.py --config=test_adapted_config.json
```

This should create a file `results/test-task.out` containing the result `100`.