Cloud Tasks Documentation
========================

Welcome to Cloud Tasks documentation. Cloud Tasks is a framework for running distributed
tasks on cloud providers with automatic instance management.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   cli
   worker

Introduction
-----------

Cloud Tasks is a powerful framework that allows you to:

- Run tasks on AWS, GCP, or Azure with a unified API (NOTE: Azure is support is not
  currently functional)
- Use cost-effective instance selection using cloud provider pricing APIs
- Support spot/preemptible instances to reduce costs
- Handle graceful shutdown for spot instance termination
- Leverage flexible task queueing and processing
- Use simple worker implementation

Installation
-----------

You can install Cloud Tasks using pip:

.. code-block:: bash

   pip install cloud-tasks

For development of `cloud-tasks` features, clone the repository and install in development mode:

.. code-block:: bash

   git clone https://github.com/username/cloud-tasks.git
   cd cloud-tasks
   pip install -e .

Quick Start
----------

Here's a quick example of using the Cloud Tasks CLI to load tasks into a queue and create
a pool of instances:

.. code-block:: bash

   # Load tasks from a JSON file into an AWS queue
   python -m cloud_tasks.cli load_queue \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --queue-name my-task-queue \
     --tasks tasks.json

   # Show the current status of a queue
   python -m cloud_tasks.cli show_queue \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --queue-name my-task-queue \
     --detail

   # List available regions for a provider
   python -m cloud_tasks.cli manage_pool \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --job-id my-job-id \
     --min-instances 2 \
     --max-instances 4 \
     --instance-types "t3 m5"

Configuration
------------

Cloud Tasks uses a YAML configuration file that supports both global defaults and
provider-specific settings. Provider-specific settings override global defaults. Command
line options override all configuration file settings.

.. code-block:: yaml

   # Global defaults for all providers
   run:
     min-cpu: 2                 # Min vCPUs per instance
     max-cpu: 4                 # Optional: maximum vCPUs per instance
     min-memory-gb: 4           # Required memory in GB per instance
     min-disk-gb: 20            # Required disk space in GB per instance
     image: ubuntu-2404-lts # Required VM image to use
     startup_script: |      # Optional startup script
       #!/bin/bash
       apt-get update -y
       apt-get install -y python3 python3-pip git

   # AWS provider configuration
   aws:
     region: us-west-2
     access_key: YOUR_ACCESS_KEY
     secret_key: YOUR_SECRET_KEY
     queue_name: my-task-queue
     instance_types: ["t3", "m5"]  # Optional: restrict to specific types

   # GCP provider configuration
   gcp:
     project_id: your-project-id
     region: us-central1
     zone: us-central1-a
     credentials_file: /path/to/credentials.json
     queue_name: my-task-queue

   # Azure provider configuration
   azure:
     subscription_id: your-subscription-id
     resource_group: your-resource-group
     location: eastus
     tenant_id: your-tenant-id
     client_id: your-client-id
     client_secret: your-client-secret
     queue_name: my-task-queue

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`