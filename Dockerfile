# Use a small Python image
FROM python:3.12-slim

# Make Python print logs immediately
ENV PYTHONUNBUFFERED=1

# Install what we need for kafka-python & DuckDB
RUN apt-get update && \
    apt-get install -y gcc build-essential ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Set working folder inside the container
WORKDIR /app

# Install Python libraries
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the scripts we need
COPY producer.py consumer.py ./

# Make folders for data and logs
RUN mkdir -p /data /logs

# Default: start Python shell 
ENTRYPOINT ["python"]