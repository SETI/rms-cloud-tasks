Configuration
=============

Large number of CPUs might overwhelm network bandwidth or disk bandwidth,
Small number of CPUs might result in inefficient use of disk if the same static
data has to be downloaded to each instance.


The configuration file supports both global defaults and provider-specific settings.

Global Run Configuration
------------------------

The global ``run`` section defines default values for all cloud providers:

.. code-block:: yaml

    run:
      cpu: 2                 # Default CPU cores per instance
      memory_gb: 4           # Default memory in GB per instance
      disk_gb: 20            # Default disk space in GB per instance
      image: ubuntu-2404-lts # Default VM image to use
      startup_script: |      # Default startup script
        #!/bin/bash
        apt-get update -y
        apt-get install -y python3 python3-pip git

Provider-Specific Configuration
-------------------------------

Each cloud provider requires specific configuration and can override the global defaults:

AWS
~~~

.. code-block:: yaml

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

GCP
~~~

.. code-block:: yaml

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

Azure
~~~~~

.. code-block:: yaml

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

Command Line Overrides
----------------------

You can override any configuration value from the command line:

.. code-block:: bash

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

.. note::
   Priority of settings is: Command Line > Provider-Specific Config > Global Run Config > System Defaults

Complete Configuration Example
------------------------------

.. code-block:: yaml

    # Global defaults for all providers
    run:
      min-cpu: 2                 # Min vCPUs per instance
      max-cpu: 4                 # Optional: maximum vCPUs per instance
      min-memory-gb: 4           # Required memory in GB per instance
      min-disk-gb: 20            # Required disk space in GB per instance
      image: ubuntu-2404-lts     # Required VM image to use
      startup_script: |          # Optional startup script
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
