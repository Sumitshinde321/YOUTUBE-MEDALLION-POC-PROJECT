FROM python:3.10-slim

WORKDIR /app

# Install build dependencies for any compiled python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend source code (including frontend/)
COPY . .

# Expose port 10000 (Render default)
EXPOSE 10000

# Start FastAPI application via uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]

