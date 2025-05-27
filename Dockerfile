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
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install necessary packages
RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
        apt-transport-https \
        curl \
        gnupg \
        lsb-release \
        unixodbc \
        unixodbc-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

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
COPY rabbitmq/ /app/rabbitmq/
# Generate or update uv.lock based on pyproject.toml
RUN uv lock

# Synchronize the virtual environment with uv.lock
RUN uv sync

# Expose port for the application
EXPOSE 8080

# Command will be overridden by docker-compose.yml
CMD ["uv", "run", "python", "main.py"]