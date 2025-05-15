# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies for opencv-python, torch, pyodbc, and other packages
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

# Add Microsoft package repository and install msodbcsql17
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update --fix-missing && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 mssql-tools && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set PATH to include mssql-tools
ENV PATH="/opt/mssql-tools/bin:${PATH}"

# Verify installation of unixODBC
RUN which odbcinst && odbcinst -j

# Install uv for dependency management
RUN pip install --no-cache-dir uv

# Copy pyproject.toml and generate requirements.txt using uv
COPY app/pyproject.toml /app/
COPY app/uv.lock /app/
RUN uv pip compile pyproject.toml -o requirements.txt && \
    uv pip install --system -r requirements.txt
COPY app/ /app/
# Make ports available (8080 for Uvicorn, 8265 for Ray dashboard)
EXPOSE 8080
EXPOSE 8265

# Run main.py when the container launches
CMD ["python", "main.py"]