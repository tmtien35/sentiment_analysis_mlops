# 1. Use standard lightweight python base image
FROM python:3.12-slim

# 2. Set working directory in container
WORKDIR /app

# 3. Install minimal OS-level build dependencies for compiling psycopg2 and database hooks
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# 4. Copy requirements and install with pip cache-clearing for a slim image
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the entire workspace into the container
COPY . .

# 6. Expose essential ports: FastAPI (8000), Streamlit (8501), MLflow (5000)
EXPOSE 8000 8501 5000

# 7. Set root Python path environment variable
ENV PYTHONPATH=/app
