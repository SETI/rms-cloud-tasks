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

The example includes three worker implementations:

1. **Simple Worker** (`worker.py`): Uses the cloud_tasks framework to handle cloud integration
2. **Cloud-Native Worker** (`worker_cloud.py`): Directly integrates with cloud services and can be deployed to cloud instances without the cloud_tasks framework
3. **Adapted Worker** (`worker_adapted.py`): Demonstrates how to adapt any existing worker code to use the cloud task adapter module

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

### Using the Cloud Task Adapter

The cloud task adapter provides a simple way to adapt any existing worker code to run in a cloud environment:

```bash
# Run the adapted worker with AWS configuration
python worker_adapted.py --config=../adapted_worker_config.json
```

This approach is ideal when:
- You have existing worker code that you want to run in the cloud
- You want to minimize changes to your core processing logic
- You need to integrate with cloud services but want to keep the integration code separate

The adapter handles:
- Cloud provider connections (AWS, GCP, Azure)
- Task queue integration
- Instance termination monitoring
- Result storage in cloud buckets
- Error handling and retries

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

# Or run the adapted worker
python worker_adapted.py --config=../../test_config.json
```

This should create a file `results/test-task.out` containing the result `100`.

## Parallel Processing

The latest version of the cloud task processing system supports true parallel processing using Python's multiprocessing module. This allows tasks to run in separate processes, bypassing the Global Interpreter Lock (GIL) and utilizing multiple CPU cores.

### Parallel Worker

The `worker_adapted.py` example demonstrates how to use multiprocessing with the cloud task adapter:

```bash
# Run with parallel processing (default is CPU count - 1 processes)
python worker_adapted.py --config=../parallel_worker_config.json

# Explicitly specify the number of worker processes
python worker_adapted.py --config=../parallel_worker_config.json --workers=4
```

### Configuration

To configure the number of worker processes, add the `num_workers` option to your configuration file:

```json
{
  "worker_options": {
    "num_workers": 4  // Number of parallel processes to use
  }
}
```

### Benefits of Parallel Processing

- **True Parallelism**: Tasks run in separate processes, allowing for parallel execution across multiple CPU cores
- **Improved Performance**: CPU-bound tasks see significant speedup when distributed across multiple processes
- **Resource Isolation**: Each process has its own memory space, preventing memory contention
- **Fault Tolerance**: If one process crashes, other processes continue running

### Example Files

- `parallel_tasks.json`: Sample tasks for testing parallel processing
- `parallel_worker_config.json`: Configuration specifying 4 worker processes

## Using Spot/Preemptible Instances

For cost-effective processing, you can use spot instances (AWS), preemptible VMs (GCP), or spot VMs (Azure) which can save up to 90% on compute costs.

### Running with Spot Instances

```bash
# Run on AWS with spot instances
python -m cloud_tasks run-job \
  --provider aws \
  --job-id adder-job \
  --input-file examples/addition_tasks.json \
  --queue-name addition-queue \
  --use-spot \
  --provider-config examples/cloud_config.yaml \
  --worker-repo https://github.com/user/worker-repo.git

# Run on GCP with preemptible instances
python -m cloud_tasks run-job \
  --provider gcp \
  --job-id adder-job \
  --input-file examples/addition_tasks.json \
  --queue-name addition-queue \
  --use-spot \
  --provider-config examples/cloud_config.yaml \
  --worker-repo https://github.com/user/worker-repo.git

# Run on Azure with spot instances
python -m cloud_tasks run-job \
  --provider azure \
  --job-id adder-job \
  --input-file examples/addition_tasks.json \
  --queue-name addition-queue \
  --use-spot \
  --provider-config examples/cloud_config.yaml \
  --worker-repo https://github.com/user/worker-repo.git
```

### Handling Spot Instance Termination

When using spot instances, your code should handle potential termination notices from the cloud provider. The Cloud Tasks framework includes built-in handling for this:

1. It monitors for termination notices from the cloud provider
2. When a termination notice is received, it stops accepting new tasks
3. It attempts to complete all in-progress tasks
4. It uploads any results before the instance is terminated

### Price Comparison

Here's an example of potential cost savings for a computation-intensive job:

| Provider | Instance Type | On-Demand Price | Spot/Preemptible Price | Savings |
|----------|---------------|-----------------|------------------------|---------|
| AWS      | c5.xlarge     | $0.17/hour      | $0.05/hour             | 70%     |
| GCP      | n1-standard-4 | $0.19/hour      | $0.04/hour             | 79%     |
| Azure    | Standard_D4_v3| $0.19/hour      | $0.06/hour             | 68%     |

*Prices are approximate and will vary by region and availability

## Choosing Regions for Optimal Pricing

Cloud Tasks can automatically select the cheapest region for your workloads or you can specify a particular region:

### Automatic Region Selection

When no region is specified, Cloud Tasks will:
1. Scan pricing across all available regions for your instance type requirements
2. Select the region with the lowest hourly price
3. Log the selected region and price information

```bash
# AWS with automatic region selection
python -m cloud_tasks run-job \
  --provider aws \
  --job-id region-job \
  --input-file examples/addition_tasks.json \
  --queue-name addition-queue \
  --provider-config examples/cloud_config_no_region.yaml \
  --worker-repo https://github.com/user/worker-repo.git

# GCP with automatic region selection
python -m cloud_tasks run-job \
  --provider gcp \
  --job-id region-job \
  --input-file examples/addition_tasks.json \
  --queue-name addition-queue \
  --provider-config examples/cloud_config_no_region.yaml \
  --worker-repo https://github.com/user/worker-repo.git

# Azure with automatic region selection
python -m cloud_tasks run-job \
  --provider azure \
  --job-id region-job \
  --input-file examples/addition_tasks.json \
  --queue-name addition-queue \
  --provider-config examples/cloud_config_no_region.yaml \
  --worker-repo https://github.com/user/worker-repo.git
```

### Manual Region Selection

You can specify a region either in the configuration file or via the command line:

```bash
# Using command line region parameter (overrides config file)
python -m cloud_tasks run-job \
  --provider aws \
  --job-id region-job \
  --input-file examples/addition_tasks.json \
  --queue-name addition-queue \
  --region us-west-2 \
  --provider-config examples/cloud_config.yaml \
  --worker-repo https://github.com/user/worker-repo.git
```

### Regional Price Variations

Instance prices can vary significantly between regions. Here's an example of regional price variations for the same instance type:

| Provider | Instance Type | Region       | Price/Hour |
|----------|---------------|--------------|------------|
| AWS      | c5.xlarge     | us-east-1    | $0.17      |
| AWS      | c5.xlarge     | us-west-2    | $0.19      |
| AWS      | c5.xlarge     | eu-west-1    | $0.21      |
| AWS      | c5.xlarge     | ap-southeast-1 | $0.24   |
| GCP      | n1-standard-4 | us-central1  | $0.19      |
| GCP      | n1-standard-4 | europe-west1 | $0.22      |
| GCP      | n1-standard-4 | asia-east1   | $0.23      |
| Azure    | Standard_D4_v3| eastus       | $0.19      |
| Azure    | Standard_D4_v3| westeurope   | $0.24      |
| Azure    | Standard_D4_v3| southeastasia| $0.26      |

*Prices are approximate and subject to change