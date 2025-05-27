#!/bin/bash

# Exit on error
set -e

# Start RabbitMQ in the background
service rabbitmq-server start

# Wait for RabbitMQ to start (timeout after 30 seconds)
timeout 30 rabbitmqctl wait -q /var/lib/rabbitmq/mnesia/rabbit@`hostname`.pid || {
  echo "RabbitMQ failed to start"
  exit 1
}

# Configure RabbitMQ
rabbitmq-plugins enable rabbitmq_management || echo "Failed to enable rabbitmq_management plugin"
rabbitmqctl add_user app_user app_password || echo "User already exists"
rabbitmqctl add_vhost app_vhost || echo "Vhost already exists"
rabbitmqctl set_user_tags app_user administrator
rabbitmqctl set_permissions -p app_vhost app_user ".*" ".*" ".*" || echo "Failed to set permissions"

# Run the Python application
exec uv run python main.py