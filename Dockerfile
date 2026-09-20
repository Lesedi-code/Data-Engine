# Usse an official light weight Python image

FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Prevent Python fromwriting pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies (needed for Postgres connectivity like psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and configurations
COPY . . 

# Run the pipeline by default 
CMD ["python", "src/pipeline.py"]
