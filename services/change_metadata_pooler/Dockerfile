# Build from repo root:
#   docker build -f services/change_metadata_pooler/Dockerfile -t change-metadata-pooler .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=8080

WORKDIR /app

# Install the adapter package (provides pooler_core + CloudSqlGateway).
COPY pyproject.toml hatch.toml README.md LICENSE /tmp/dbt-bigquery-ext/
COPY src /tmp/dbt-bigquery-ext/src
RUN pip install --no-cache-dir /tmp/dbt-bigquery-ext \
    && rm -rf /tmp/dbt-bigquery-ext

COPY services/change_metadata_pooler/app /app/app
RUN pip install --no-cache-dir \
    "fastapi>=0.115.0" \
    "uvicorn[standard]>=0.32.0" \
    "google-api-python-client>=2.100.0"

EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
