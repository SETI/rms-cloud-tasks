# Multi-Cloud Task Processing System

A scalable, cloud-agnostic task processing system that distributes independent tasks across compute instances, optimizing for cost and reliability with minimal overhead.

## Features

- Distribute tasks across multiple cloud providers (AWS, GCP, Azure)
- Automatic scaling based on workload
- Cost optimization through intelligent instance selection
- Fault tolerance with automatic retries
- Spot/preemptible instance support
- Simple, JSON-based task definition

## Components

The system consists of three main components:

1. **Task Queue Manager**: Handles task distribution, visibility timeout, and requeuing of failed tasks
2. **Instance Orchestrator**: Provisions and terminates instances based on workload
3. **Worker Module**: Polls for tasks, processes them, and handles graceful termination

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Configuration

Create a configuration file with your cloud provider credentials:

```yaml
# config.yaml
aws:
  access_key: YOUR_AWS_ACCESS_KEY
  secret_key: YOUR_AWS_SECRET_KEY
  region: us-west-2

gcp:
  project_id: YOUR_GCP_PROJECT_ID
  credentials_file: /path/to/credentials.json  # Optional, uses Application Default Credentials if omitted

azure:
  subscription_id: YOUR_AZURE_SUBSCRIPTION_ID
  tenant_id: YOUR_AZURE_TENANT_ID
  client_id: YOUR_AZURE_CLIENT_ID
  client_secret: YOUR_AZURE_CLIENT_SECRET
```

### Authentication

#### AWS
AWS authentication requires an access key and secret key, and optionally a region (defaults to us-east-1).

#### Google Cloud
Google Cloud supports multiple authentication methods:

1. **Explicit credentials file** - Set `credentials_file` in the configuration:
   ```yaml
   gcp:
     project_id: YOUR_GCP_PROJECT_ID
     credentials_file: /path/to/credentials.json
   ```

2. **Application Default Credentials (ADC)** - When `credentials_file` is omitted, the system uses ADC in the following order:
   - `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a credentials file
   - User's gcloud CLI configuration at `$HOME/.config/gcloud/application_default_credentials.json`
   - Metadata server credentials when running on Google Cloud

   To set up ADC, run:
   ```bash
   gcloud auth application-default login
   ```

#### Azure
Azure authentication requires a subscription ID, tenant ID, client ID, and client secret.

### Running Tasks

1. Define your tasks in a JSON file:

```json
[
  {"id": "task-1", "data": {"input": "value1"}},
  {"id": "task-2", "data": {"input": "value2"}},
  {"id": "task-3", "data": {"input": "value3"}}
]
```

2. Start the processing:

```bash
python -m cloud_tasks.cli run --config config.yaml --tasks tasks.json --worker-repo https://github.com/your-org/worker-code.git --max-instances 10
```

## Development

### Running Tests

```bash
pytest
```

## License

Apache-2.0
