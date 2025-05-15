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
        && apt-get clean && rm -rf /var/lib/apt/lists/*

# Add Microsoft package repository
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update --fix-missing && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 mssql-tools && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set PATH for mssql-tools
ENV PATH="/opt/mssql-tools/bin:${PATH}"

# Verify unixODBC
RUN which odbcinst && odbcinst -j

# Install uv
RUN pip install --no-cache-dir uv==0.4.18

# Copy dependency files
COPY app/pyproject.toml /app/
COPY app/uv.lock /app/
RUN uv pip install --system -r uv.lock

# Copy the rest of the application
COPY app/ /app/

# Expose ports
EXPOSE 8080
EXPOSE 8265

# Run main.py
CMD ["python", "main.py"]