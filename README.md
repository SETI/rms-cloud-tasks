# Cloud Tasks

A framework for running distributed tasks on cloud providers with automatic instance management.

## Features

- Run tasks on AWS, GCP, or Azure with a unified API
- Automatically scale worker instances based on queue depth
- Cost-effective instance selection using cloud provider pricing APIs
- Support for spot/preemptible instances to reduce costs
- Intelligent region selection to minimize costs
- Graceful shutdown handling for spot instance termination
- Flexible task queueing and processing
- Simple worker implementation
- Consistent logging with microsecond precision

## Instance Selection

Cloud Tasks intelligently selects the most cost-effective instance type for your workloads:

- Uses each cloud provider's pricing API to get accurate, up-to-date pricing
- Filters instances that meet your minimum CPU, memory, and disk requirements
- Selects the instance with the lowest hourly cost
- Supports both standard on-demand instances and discounted spot/preemptible instances
- Falls back to a cost heuristic if pricing APIs are unavailable

### Region Selection

Cloud Tasks can intelligently choose the most cost-effective region to run your workloads:

- Automatically checks pricing across all available regions when no region is specified
- Selects the region with the lowest price for your compute requirements
- Provides warnings when falling back to automatic region selection
- Allows you to specify a preferred region when you need to control data locality
- Works seamlessly with spot/preemptible instances for maximum cost savings

### Spot/Preemptible Instances

Spot instances (AWS), preemptible VMs (GCP), and spot VMs (Azure) offer significantly reduced prices (up to 90% cheaper) with the tradeoff that they can be terminated by the cloud provider with little notice. Cloud Tasks supports these instances with:

- Proper configuration for each cloud provider's spot offerings
- Graceful termination handling when instances are reclaimed
- Integration with cloud provider termination notice APIs
- Automatic fallback to on-demand pricing API if spot pricing is unavailable

## Usage

### Basic Example

```python
from cloud_tasks import InstanceOrchestrator, Worker

# Create an orchestrator that manages AWS EC2 instances
orchestrator = InstanceOrchestrator(
    provider="aws",
    job_id="my-processing-job",
    cpu_required=2,
    memory_required_gb=4,
    disk_required_gb=20,
    min_instances=1,
    max_instances=10,
    use_spot_instances=True,  # Use spot instances for cost savings
    region="us-west-2",       # Specify region (optional, will use cheapest if omitted)
    queue_name="my-processing-job-queue",  # Specify queue name
    access_key="YOUR_ACCESS_KEY",
    secret_key="YOUR_SECRET_KEY"
)

# Start the orchestrator
await orchestrator.start()
```

### Command Line Interface

```bash
# Run a job on AWS using spot instances in a specific region
python -m cloud_tasks run-job \
  --provider aws \
  --job-id my-processing-job \
  --input-file tasks.json \
  --queue-name my-task-queue \
  --cpu 2 \
  --memory 4 \
  --disk 20 \
  --min-instances 1 \
  --max-instances 10 \
  --use-spot \
  --region us-west-2 \
  --provider-config aws_config.yaml \
  --worker-repo https://github.com/user/worker-repo.git

# Run a job on AWS using spot instances with automatic region selection (cheapest)
python -m cloud_tasks run-job \
  --provider aws \
  --job-id my-processing-job \
  --input-file tasks.json \
  --queue-name my-task-queue \
  --cpu 2 \
  --memory 4 \
  --disk 20 \
  --min-instances 1 \
  --max-instances 10 \
  --use-spot \
  --provider-config aws_config.yaml \
  --worker-repo https://github.com/user/worker-repo.git
```

## Configuration

Each cloud provider requires specific configuration:

### AWS

```yaml
aws:
  region: us-west-2  # Optional: omit for automatic cheapest region selection
  access_key: YOUR_ACCESS_KEY
  secret_key: YOUR_SECRET_KEY
```

### GCP

```yaml
gcp:
  project_id: your-project-id
  region: us-central1  # Optional: omit for automatic cheapest region selection
  zone: us-central1-a  # Optional if region is specified
  credentials_file: /path/to/credentials.json
```

### Azure

```yaml
azure:
  subscription_id: your-subscription-id
  resource_group: your-resource-group
  location: eastus  # Optional: omit for automatic cheapest location selection
  tenant_id: your-tenant-id
  client_id: your-client-id
  client_secret: your-client-secret
```

## Installation

```bash
pip install cloud-tasks
```

Or for development:

```bash
git clone https://github.com/username/cloud-tasks.git
cd cloud-tasks
pip install -e .
```

## Development

### Running Tests

```bash
pytest
```

## License

Apache-2.0

## Logging System

Cloud Tasks includes a custom logging system that provides:

- Consistent timestamp format across all components
- Millisecond precision in log timestamps (3 digits)
- Configurable log levels
- Structured logging that works well with log aggregation systems

To use the logging system in your own code:

```python
from cloud_tasks.common.logging_config import configure_logging
import logging

# Configure the root logger with millisecond support
configure_logging(level=logging.INFO)

# Get a logger for your module
logger = logging.getLogger(__name__)

# Use the logger as normal
logger.info("Processing task %s", task_id)
```

This produces log entries with millisecond precision:
```
2025-03-12 21:28:49.123 - module_name - INFO - Processing task abc-123
```
