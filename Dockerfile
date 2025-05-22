# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
# Doing this before copying the entire application allows us to cache the installed dependencies layer
# and not reinstall them on every build unless requirements.txt changes
RUN pip install -r requirements.txt
#pip install --upgrade pip && \
    #pip install --no-cache-dir -r requirements.txt
#RUN apt-get install unixodbc

# Now copy the rest of the application into the container
COPY icon_image_lib/ icon_image_lib/
COPY main.py .
COPY app_config.py .
COPY email_utils.py .
COPY s3_utils.py .
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

# Set PATH to include mssql-tools
ENV PATH="/opt/mssql-tools/bin:${PATH}"

# Verify installation of unixODBC
RUN which odbcinst

# LABEL "com.datadoghq.ad.logs"='[<LOGS_CONFIG>]'
# Make port 8000 available to the world outside this container
EXPOSE 8080

# Run main.py when the container launches
CMD ["python", "main.py"]
