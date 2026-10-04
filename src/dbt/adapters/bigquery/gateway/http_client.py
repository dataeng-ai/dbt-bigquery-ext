"""HTTP client for the change-metadata pooler Cloud Run service."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Mapping, Optional, Sequence

logger = logging.getLogger("dbt.adapters.bigquery.gateway.http_client")


def _fetch_id_token(audience: str) -> str:
    """Fetch a Google OIDC ID token for Cloud Run IAM invoker auth."""
    try:
        import google.auth.transport.requests
        import google.oauth2.id_token
    except ImportError as exc:
        raise RuntimeError(
            "google-auth is required to call gateway.pooler_url"
        ) from exc

    request = google.auth.transport.requests.Request()
    return google.oauth2.id_token.fetch_id_token(request, audience)


class PoolerHttpClient:
    """Thin client for ``/v1/*`` pooler endpoints."""

    def __init__(self, base_url: str, *, timeout_s: float = 300.0):
        self._base = base_url.rstrip("/")
        self._timeout = timeout_s

    def _request(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Mapping[str, Any]] = None,
        body: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        url = f"{self._base}{path}"
        if query:
            qs = urllib.parse.urlencode(
                {k: v for k, v in query.items() if v is not None}
            )
            url = f"{url}?{qs}"
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        token = _fetch_id_token(self._base)
        headers["Authorization"] = f"Bearer {token}"

        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                raw = resp.read().decode("utf-8")
                if not raw:
                    return None
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"pooler HTTP {method} {path} failed: {exc.code} {detail}"
            ) from exc

    def pool(
        self,
        relations: Optional[Sequence[Mapping[str, Any]]] = None,
        *,
        end_ts: Optional[str] = None,
        worker_pool_size: int = 0,
        invocation_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        body: Dict[str, Any] = {"worker_pool_size": worker_pool_size}
        if relations is not None:
            body["relations"] = list(relations)
        if end_ts is not None:
            body["end_ts"] = end_ts
        if invocation_id is not None:
            body["invocation_id"] = invocation_id
        result = self._request("POST", "/v1/pool", body=body)
        if isinstance(result, dict) and "results" in result:
            return list(result["results"])
        if isinstance(result, list):
            return result
        return []

    def ensure_affected_partitions(
        self,
        project: str,
        dataset: str,
        table: str,
        start_ts: str,
        end_ts: str,
        *,
        ensure_fresh: bool = True,
    ) -> Dict[str, Any]:
        result = self._request(
            "GET",
            "/v1/partitions",
            query={
                "project": project,
                "dataset": dataset,
                "table": table,
                "start": start_ts,
                "end": end_ts,
                "ensure_fresh": "true" if ensure_fresh else "false",
            },
        )
        if not isinstance(result, dict):
            raise RuntimeError(f"unexpected /v1/partitions response: {result!r}")
        return result
