#!/bin/bash
rabbitmq-server -detached  # Start RabbitMQ in detached mode
rabbitmqctl wait -t 60000 /var/lib/rabbitmq/mnesia/rabbit.pid || { echo "RabbitMQ failed to start"; exit 1; }
rabbitmqctl add_user app_user app_password || echo "User already exists"
rabbitmqctl add_vhost app_vhost || echo "Vhost already exists"
rabbitmqctl set_user_tags app_user administrator
rabbitmqctl set_permissions -p app_vhost app_user ".*" ".*" ".*" || echo "Failed to set permissions"
exec rabbitmq-server  # Run RabbitMQ in foreground to keep container running