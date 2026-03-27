from python:3.12-slim-bookworm

workdir /app

env PYTHONDONTWRITEBYTECODE=1
env PYTHONUNBUFFERED=1

run apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
env PATH="${JAVA_HOME}/bin:${PATH}"

copy requirements.txt /app/requirements.txt

run pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

copy app /app/app
copy config /app/config
copy scripts /app/scripts
copy sql /app/sql

cmd ["python", "-m", "app.jobs.spark_full_job"]
