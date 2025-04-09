FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install wheel
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies with increased verbosity and timeout
RUN pip install --no-cache-dir --verbose --timeout 300 -r requirements.txt

# Copy the rest of the application
COPY . .

# Command to run the application
CMD ["python", "price_collector/updater2.py"] 