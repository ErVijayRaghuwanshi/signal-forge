# Use an official, lightweight Python base image
FROM python:3.12-slim

# Install Astral's uv package manager by copying the compiled binary from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory inside the container
WORKDIR /app

# Copy dependency requirements first to leverage Docker layer caching
COPY requirements.txt .

# Install Python dependencies using uv (massively accelerated resolution & caching!)
# Installs directly to the system python container context
RUN uv pip install --system --no-cache -r requirements.txt

# Copy the rest of the application files
COPY . .

# Create the default output directory for generated log files
RUN mkdir -p /app/output

# Set main.py as the execution entrypoint
ENTRYPOINT ["python", "main.py"]

# Set default arguments to continuously stream CDR/IPDR to a local Kafka broker
CMD ["--format", "kafka", "--stream", "--speed", "60.0", "--kafka-bootstrap", "localhost:9092"]
