FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    git \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-postgres==1.8.0 \
    dbt-bigquery==1.8.0

WORKDIR /usr/app

ENTRYPOINT ["sleep", "infinity"]
