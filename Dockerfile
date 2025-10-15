FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libgmp-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY stellaris_pool/requirements.txt ./stellaris_pool/
RUN pip install --no-cache-dir -r stellaris_pool/requirements.txt

# Copy application code
COPY stellaris_pool/ ./stellaris_pool/
COPY stellaris/ ./stellaris/

# Create data directory
RUN mkdir -p /app/data/pool

# Expose pool API port
EXPOSE 8000

# Run the pool server
CMD ["python", "-m", "uvicorn", "stellaris_pool.main:app", "--host", "0.0.0.0", "--port", "8000"]
