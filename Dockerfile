FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && useradd --create-home --shell /bin/bash appuser \
    && rm -rf /var/lib/apt/lists/*

COPY . /app

RUN python -m pip install --upgrade pip \
    && python -m pip install boto3

RUN mkdir -p /app/runtime /app/data/downloads \
    && chown -R appuser:appuser /app \
    && chmod +x /app/scripts/container-entrypoint.sh

EXPOSE 8765

ENTRYPOINT ["/app/scripts/container-entrypoint.sh"]
