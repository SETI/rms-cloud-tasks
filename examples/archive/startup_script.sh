#!/bin/bash
# Example startup script for worker instances
# This script demonstrates how to set up a worker environment

# Log the startup
echo "Starting worker setup at $(date)" > /var/log/worker-setup.log

# Update system and install dependencies
apt-get update -y
apt-get install -y python3 python3-pip git curl jq

# Install cloud-specific tools based on provider
PROVIDER=$(curl -s http://169.254.169.254/latest/meta-data/services/partition 2>/dev/null) || PROVIDER=""

if [ "$PROVIDER" == "aws" ]; then
  # AWS-specific setup
  echo "Detected AWS environment, installing AWS tools" >> /var/log/worker-setup.log
  pip install awscli boto3
elif [ -f /sys/class/dmi/id/product_name ] && [ "$(cat /sys/class/dmi/id/product_name)" == "Google Compute Engine" ]; then
  # GCP-specific setup
  echo "Detected GCP environment, installing GCP tools" >> /var/log/worker-setup.log
  curl https://sdk.cloud.google.com | bash -s -- --disable-prompts
  export PATH=$PATH:/root/google-cloud-sdk/bin
  pip install google-cloud-storage google-cloud-pubsub
elif [ -f /sys/class/dmi/id/chassis_asset_tag ] && [ "$(cat /sys/class/dmi/id/chassis_asset_tag)" == "7783-7084-3265-9085-8269-3286-77" ]; then
  # Azure-specific setup
  echo "Detected Azure environment, installing Azure tools" >> /var/log/worker-setup.log
  pip install azure-storage-blob azure-servicebus
else
  echo "Could not detect cloud provider, installing all tools" >> /var/log/worker-setup.log
  pip install awscli boto3 google-cloud-storage google-cloud-pubsub azure-storage-blob azure-servicebus
fi

# Clone worker code (replaced by orchestrator with actual repo)
git clone https://github.com/example/worker-repo.git /opt/worker
cd /opt/worker

# Install worker dependencies
pip install -r requirements.txt

# Set up logging directory
mkdir -p /var/log/worker
chmod 755 /var/log/worker

# Final setup message
echo "Worker setup completed at $(date)" >> /var/log/worker-setup.log

# Worker process will be started by the orchestrator's configuration