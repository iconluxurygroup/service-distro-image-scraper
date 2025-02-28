# Use an official Python runtime as a parent image
FROM python:3.10-slim
#FROM rayproject/ray
# Set the working directory in the container
WORKDIR /app
COPY /app /app/
# Copy the requirements file first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
USER root
# Clean the apt cache and update with --fix-missing
RUN apt-get clean && \
    apt-get update --fix-missing

# Install necessary packages in smaller steps
RUN apt-get install -y apt-transport-https && \
    apt-get install -y curl && \
    apt-get install -y gnupg && \
    apt-get install -y lsb-release && \
    apt-get install -y unixodbc unixodbc-dev

# Add Microsoft package repository and install msodbcsql17
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update --fix-missing && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    ACCEPT_EULA=Y apt-get install -y mssql-tools && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

# Verify installation of unixODBC
RUN which odbcinst

COPY install_sql_server_dev.sh .

# Make the install script executable and run it
# RUN chmod +x install_sql_server_dev.sh
# RUN ./install_sql_server_dev.sh

# Verify ODBC installation
RUN odbcinst -j
# Upgrade all Python packages to the latest versions
#RUN pip list --outdated --format=columns | tail -n +3 | awk '{print $1}' | xargs -n1 pip install -U

# Make port 8080 available to the world outside this container
EXPOSE 8080
# Run ray start
#RUN ray start --head

# Run main.py when the container launches
# CMD ["python", "main.py"]
CMD ray start --head && python main.py