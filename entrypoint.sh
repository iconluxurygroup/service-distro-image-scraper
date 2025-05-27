#!/bin/bash

# Start RabbitMQ in the background
service rabbitmq-server start

# Wait for RabbitMQ to fully start
until rabbitmqctl wait -q /var/lib/rabbitmq/mnesia/rabbit@`hostname`.pid; do
  echo "Waiting for RabbitMQ to start..."
  sleep 1
done

# Configure RabbitMQ
rabbitmq-plugins enable rabbitmq_management
rabbitmqctl add_user app_user app_password
rabbitmqctl add_vhost app_vhost
rabbitmqctl set_user_tags app_user administrator
rabbitmqctl set_permissions -p app_vhost app_user ".*" ".*" ".*"

# Run the Python application
exec uv run python main.py