#!/bin/bash
set -e
USER=$(cat /run/secrets/rabbit_user)
PASS=$(cat /run/secrets/rabbit_password)
rabbitmq-server -detached
rabbitmqctl wait -t 60000 /var/lib/rabbitmq/mnesia/rabbit.pid || { echo "RabbitMQ failed to start" >> /var/log/rabbitmq/setup.log; exit 1; }
rabbitmqctl add_user "$USER" "$PASS" || echo "User already exists" >> /var/log/rabbitmq/setup.log
rabbitmqctl add_vhost app_vhost || echo "Vhost already exists" >> /var/log/rabbitmq/setup.log
rabbitmqctl set_user_tags "$USER" administrator
rabbitmqctl set_permissions -p app_vhost "$USER" ".*" ".*" ".*" || echo "Failed to set permissions" >> /var/log/rabbitmq/setup.log
exec rabbitmq-server