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
- Intuitive attribute-style configuration access

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

# List available VM images for a provider
python -m cloud_tasks list_images \
  --config config.yaml \
  --provider aws \
  --sort-by "name,source"

# List available instance types with pricing information
python -m cloud_tasks list_instances \
  --config config.yaml \
  --provider aws \
  --instance-types "t3 m5" \
  --size-filter "2:4:10" \
  --limit 10 \
  --use-spot \
  --sort-by "price,vcpu"
```

## Configuration

The configuration file supports both global defaults and provider-specific settings.

### Global Run Configuration

The global `run` section defines default values for all cloud providers:

```yaml
run:
  cpu: 2                 # Default CPU cores per instance
  memory_gb: 4           # Default memory in GB per instance
  disk_gb: 20            # Default disk space in GB per instance
  image: ubuntu-2404-lts # Default VM image to use
  startup_script: |      # Default startup script
    #!/bin/bash
    apt-get update -y
    apt-get install -y python3 python3-pip git
```

### Provider-Specific Configuration

Each cloud provider requires specific configuration and can override the global defaults:

#### AWS

```yaml
aws:
  region: us-west-2        # AWS region
  access_key: YOUR_ACCESS_KEY
  secret_key: YOUR_SECRET_KEY
  instance_types: ["t3", "m5"] # Optional: Restrict instances to specific types/families

  # Optional overrides for this provider
  cpu: 4                   # Override global CPU setting
  memory_gb: 8             # Override global memory setting
  disk_gb: 30              # Override global disk setting
  image: ami-0123456789abcdef0  # Custom AMI ID or name
  startup_script: |        # AWS-specific startup script
    #!/bin/bash
    apt-get update -y
    apt-get install -y aws-cli
```

#### GCP

```yaml
gcp:
  project_id: your-project-id
  region: us-central1      # Optional: omit for automatic cheapest region selection
  zone: us-central1-a      # Optional if region is specified
  credentials_file: /path/to/credentials.json  # Optional: uses default credentials if omitted
  instance_types: ["n1", "e2"] # Optional: Restrict instances to specific types/families

  # Optional overrides for this provider
  cpu: 2                   # Override global CPU setting
  memory_gb: 4             # Override global memory setting
  disk_gb: 20              # Override global disk setting
  image: ubuntu-2404-lts   # Image family or full resource path
  startup_script: |        # GCP-specific startup script
    #!/bin/bash
    apt-get update -y
    apt-get install -y google-cloud-sdk
```

#### Azure

```yaml
azure:
  subscription_id: your-subscription-id
  resource_group: your-resource-group
  location: eastus        # Optional: omit for automatic cheapest location selection
  tenant_id: your-tenant-id
  client_id: your-client-id
  client_secret: your-client-secret
  instance_types: ["Standard_B", "Standard_D"] # Optional: Restrict VM sizes to specific types/families

  # Optional overrides for this provider
  cpu: 2                  # Override global CPU setting
  memory_gb: 4            # Override global memory setting
  disk_gb: 20             # Override global disk setting
  image: Canonical:UbuntuServer:24_04-lts:latest  # URN format or resource ID
  startup_script: |       # Azure-specific startup script
    #!/bin/bash
    apt-get update -y
    apt-get install -y azure-cli
```

### Command Line Overrides

You can override any configuration value from the command line:

```bash
python -m cloud_tasks run \
  --config config.yaml \
  --tasks tasks.json \
  --provider aws \
  --cpu 8 \                        # Override CPU setting
  --memory 16 \                    # Override memory setting
  --disk 100 \                     # Override disk setting
  --image ami-0123456789abcdef0 \  # Override image setting
  --startup-script-file setup.sh \ # Override startup script with file contents
  --use-spot \
  --job-id my-processing-job \
  --instance-types t3 m5          # Restrict to t3 and m5 instance families
```

Priority of settings is: Command Line > Provider-Specific Config > Global Run Config > System Defaults

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
