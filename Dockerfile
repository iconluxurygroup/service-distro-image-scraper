# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
        apt-transport-https \
        curl \
        gnupg \
        lsb-release \
        unixodbc \
        unixodbc-dev \
        libpq-dev \
        build-essential \
        libopencv-dev \
        rabbitmq-server \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Clean the apt cache and update with --fix-missing
RUN apt-get clean && \
    apt-get update --fix-missing

# Install necessary packages
RUN apt-get install -y apt-transport-https curl gnupg lsb-release unixodbc unixodbc-dev

# Add Microsoft package repository and install msodbcsql17
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update --fix-missing && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    ACCEPT_EULA=Y apt-get install -y mssql-tools

# Set PATH for mssql-tools
ENV PATH="/opt/mssql-tools/bin:${PATH}"

# Verify unixODBC
RUN which odbcinst && odbcinst -j

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency files
COPY pyproject.toml /app/
COPY uv.lock /app/
# Copy the rest of the application
COPY app/ /app/

# Generate or update uv.lock based on pyproject.toml
RUN uv lock

# Synchronize the virtual environment with uv.lock
RUN uv sync

# Set hostname for RabbitMQ to avoid dynamic hostname issues
RUN echo "NODENAME=rabbit@localhost" > /etc/rabbitmq/rabbitmq-env.conf && \
    echo "127.0.0.1 localhost" >> /etc/hosts

# Expose ports for RabbitMQ (AMQP and Management UI) and application
EXPOSE 5672 15672 8080

# Start RabbitMQ if not running, configure it, and run the Python application
CMD (rabbitmqctl status >/dev/null 2>&1 || (service rabbitmq-server start && timeout 60 rabbitmqctl wait -q /var/lib/rabbitmq/mnesia/rabbit@localhost.pid || { echo "RabbitMQ failed to start"; rabbitmqctl status; exit 1; })) && \
    rabbitmq-plugins enable rabbitmq_management || echo "Failed to enable rabbitmq_management plugin" && \
    rabbitmqctl add_user app_user app_password || echo "User already exists" && \
    rabbitmqctl add_vhost app_vhost || echo "Vhost already exists" && \
    rabbitmqctl set_user_tags app_user administrator && \
    rabbitmqctl set_permissions -p app_vhost app_user ".*" ".*" ".*" || echo "Failed to set permissions" && \
    uv run python main.py