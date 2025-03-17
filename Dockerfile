# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app
# Upgrade pip in its own layer
RUN pip install --upgrade pip
# Copy and install requirements
COPY app/requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

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

# Copy the rest of the application into the container
COPY app/ /app/

# Verify ODBC installation
RUN odbcinst -j

# Make port 8080 available to the world outside this container
EXPOSE 8080
EXPOSE 8265
# Run main.py when the container launches
CMD python main.py