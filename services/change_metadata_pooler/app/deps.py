"""Shared clients (gateway + BigQuery) for request handlers."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from google.cloud import bigquery

from dbt.adapters.bigquery.credentials import (
    BigQueryConnectionMethod,
    BigQueryCredentials,
)
from dbt.adapters.bigquery.gateway.client import CloudSqlGateway
from dbt.adapters.bigquery.gateway.config import CloudSqlGatewayConfig
from dbt.adapters.bigquery.gateway.pooler_core import (
    ChangeMetadataPoolerCore,
    GoogleBqClient,
)

from app.settings import Settings


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()


@lru_cache(maxsize=1)
def get_gateway() -> CloudSqlGateway:
    settings = get_settings()
    # method=oauth uses ADC (Cloud Run SA). schema is required by BigQueryCredentials
    # but unused for gateway SQL.
    creds = BigQueryCredentials(
        method=BigQueryConnectionMethod.OAUTH,
        database=settings.bq_project,
        schema="pooler",
    )
    cfg = CloudSqlGatewayConfig(
        instance_connection_name=settings.instance_connection_name,
        database=settings.database,
        user=settings.iam_user,
        ip_type=settings.ip_type,
        schema_name=settings.schema_name,
        init_on_connect=True,
        auto_migrate=True,
    )
    gw = CloudSqlGateway(creds, cfg)
    gw.ensure_schema()
    return gw


@lru_cache(maxsize=1)
def get_bq_client() -> Any:
    settings = get_settings()
    return bigquery.Client(project=settings.bq_project, location=settings.bq_location)


def get_pooler() -> ChangeMetadataPoolerCore:
    settings = get_settings()
    return ChangeMetadataPoolerCore(
        GoogleBqClient(get_bq_client()),
        get_gateway(),
        default_threads=settings.worker_pool_size,
        pooler_sa_email=settings.pooler_sa_email,
    )
