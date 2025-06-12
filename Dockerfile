# Use official Python slim image for a smaller footprint
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    gnupg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Add Microsoft SQL Server ODBC repo
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list -o /etc/apt/sources.list.d/mssql-release.list

# Install MS ODBC Driver + unixodbc from Microsoft repo
RUN apt-get update \
    && apt-get remove -y unixodbc unixodbc-dev unixodbc-common libodbc1 libodbcinst2 || true \
    && dpkg --purge unixodbc unixodbc-dev unixodbc-common libodbc1 libodbcinst2 || true \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
