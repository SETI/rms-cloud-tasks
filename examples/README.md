# Multi-Cloud Task Processing System Examples

This directory contains example files for using the multi-cloud task processing system.

## Configuration

The `config.yaml` file contains example configurations for all supported cloud providers. You'll need to update this with your own credentials and settings before use.

## Task Definition

The `tasks.json` file shows how to define tasks for processing. Each task must include an `id` and a `data` object with parameters specific to your workload.

## Running a Job

Here are some example commands for running jobs:

### AWS Example

```bash
# Run a job on AWS
python -m cloud_tasks.cli run \
  --config examples/config.yaml \
  --tasks examples/tasks.json \
  --provider aws \
  --queue-name my-task-queue \
  --worker-repo https://github.com/your-org/worker-code.git \
  --max-instances 5 \
  --cpu 2 \
  --memory 4 \
  --disk 20 \
  --tasks-per-instance 5
```

### GCP Example

```bash
# Run a job on GCP
python -m cloud_tasks.cli run \
  --config examples/config.yaml \
  --tasks examples/tasks.json \
  --provider gcp \
  --queue-name my-task-queue \
  --worker-repo https://github.com/your-org/worker-code.git \
  --max-instances 5 \
  --cpu 2 \
  --memory 4 \
  --disk 20 \
  --tasks-per-instance 5
```

### Azure Example

```bash
# Run a job on Azure
python -m cloud_tasks.cli run \
  --config examples/config.yaml \
  --tasks examples/tasks.json \
  --provider azure \
  --queue-name my-task-queue \
  --worker-repo https://github.com/your-org/worker-code.git \
  --max-instances 5 \
  --cpu 2 \
  --memory 4 \
  --disk 20 \
  --tasks-per-instance 5
```

## Checking Job Status

```bash
# Check the status of a running job
python -m cloud_tasks.cli status \
  --config examples/config.yaml \
  --provider aws \
  --queue-name my-task-queue \
  --job-id job-1234567890 \
  --verbose
```

## Stopping a Job

```bash
# Stop a running job
python -m cloud_tasks.cli stop \
  --config examples/config.yaml \
  --provider aws \
  --queue-name my-task-queue \
  --job-id job-1234567890 \
  --purge-queue
```

## Worker Repository Structure

Your worker repository should have a specific structure to work correctly with this system:

```
worker-repo/
├── requirements.txt  # Dependencies for the worker code
├── worker.py         # Main worker script (must support --config parameter)
└── ...               # Other worker code
```

The worker code will be provided with a configuration file in JSON format containing all the necessary information to connect to the task queue and process tasks.

# Number Adder Example

This example demonstrates how to use the multi-cloud task processing system to run distributed number addition jobs.

## Files

- `addition_tasks.json`: Sample input file containing tasks to add pairs of numbers
- `cloud_config.yaml`: Sample cloud provider configuration (requires your credentials)
- `cloud_worker_config.json`: Sample configuration for the standalone cloud worker
- `worker_repo/`: Worker implementations

## Worker Implementations

The example includes two worker implementations:

1. **Simple Worker** (`worker.py`): Uses the cloud_tasks framework to handle cloud integration
2. **Cloud-Native Worker** (`worker_cloud.py`): Directly integrates with cloud services and can be deployed to cloud instances without the cloud_tasks framework

## Expected Results

After running the tasks, the following files will be created in the `results/` directory:

- `task-1.out`: Contains `100` (42 + 58)
- `task-2.out`: Contains `300` (100 + 200)
- `task-3.out`: Contains `6912` (1234 + 5678)
- `task-4.out`: Contains `5.85` (3.14 + 2.71)
- `task-5.out`: Contains `15` (-10 + 25)

## Usage

### Running with cloud_tasks

Use the following command to process the tasks using AWS as the cloud provider:

```bash
python -m cloud_tasks.cli run \
  --config examples/cloud_config.yaml \
  --tasks examples/addition_tasks.json \
  --provider aws \
  --queue-name number-adder-queue \
  --worker-repo https://github.com/your-org/adder-worker.git \
  --max-instances 3 \
  --cpu 1 \
  --memory 2 \
  --disk 10 \
  --tasks-per-instance 2
```

**Note**: For this to work in production, you would need to:
1. Update the `cloud_config.yaml` with your actual cloud credentials
2. Push the worker code to a GitHub repository (referenced by `--worker-repo`)

### Deploying the Cloud-Native Worker Directly

The cloud-native worker can be deployed directly to cloud instances:

```bash
# On an AWS EC2 instance
python worker_cloud.py --config=/path/to/cloud_worker_config.json
```

This is useful when you want to:
- Manually deploy workers without using the cloud_tasks orchestration
- Run on existing infrastructure
- Have more control over the worker lifecycle
- Customize cloud resource usage

### Checking Job Status

```bash
python -m cloud_tasks.cli status \
  --config examples/cloud_config.yaml \
  --provider aws \
  --queue-name number-adder-queue \
  --job-id job-1234567890
```

Where `job-1234567890` is the job ID provided when you started the job (or generated automatically).

### Stopping the Job

```bash
python -m cloud_tasks.cli stop \
  --config examples/cloud_config.yaml \
  --provider aws \
  --queue-name number-adder-queue \
  --job-id job-1234567890
```

## Local Testing

To test the worker locally without deploying to cloud:

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

# Navigate to the worker directory
cd examples/worker_repo

# Run the simple worker
python worker.py --config=../../test_config.json

# Or run the cloud-native worker
python worker_cloud.py --config=../../test_config.json
```

This should create a file `results/test-task.out` containing the result `100`.