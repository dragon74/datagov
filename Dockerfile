# Dockerfile for Google Cloud Run Job to run datagov.py
FROM python:3.11-slim

# Install system dependencies (if any)
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set workdir
WORKDIR /app

# Copy requirements and source code
COPY datagov.py ./
COPY data_urls.json ./

# Install Python dependencies
RUN pip install --no-cache-dir google-cloud-storage ijson

# Set environment variables (can be overridden at deploy time)
ENV PYTHONUNBUFFERED=1

# Command to run the script
ENTRYPOINT ["python", "datagov.py"]
