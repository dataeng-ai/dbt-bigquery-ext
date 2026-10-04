# change-metadata-pooler

Cloud Run service that pools BigQuery `CHANGES` metadata into a Cloud SQL
gateway (`change_tracking_registry` / `change_tracking_log`). Same Python core
as the dbt adapter (`pooler_core`).

## HTTP API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Liveness |
| `POST` | `/v1/tables/register` | Enable scheduled pooling for a table |
| `POST` | `/v1/tables/unregister` | Soft-disable |
| `GET` | `/v1/tables` | List registry |
| `POST` | `/v1/register/preview` | List tables for `project.dataset` / probe `project.dataset.table` |
| `POST` | `/v1/register` | Register dataset tables or a single table (access-aware) |
| `GET` | `/v1/permissions` | Open permission issues (dataset/project level) |
| `POST` | `/v1/permissions/grant` | Grant with user OAuth token + retry probe |
| `POST` | `/v1/pool` | On-demand pool (`relations` optional → all enabled) |
| `POST` | `/v1/pool/scheduled` | Scheduler target |
| `GET` | `/v1/partitions` | Ensure-fresh affected partitions |

UI: `/ui/register`, `/ui/permissions` (plus tables/runs). Grants are **dataset or
project** IAM/ACL only — never table-level.

**Grant & retry** uses your user token (IAP identity alone cannot call IAM APIs).
Paste output of `gcloud auth print-access-token`. Optional: set `OAUTH_CLIENT_ID`
to a **Web application** OAuth client whose Authorized JavaScript origins include
the Cloud Run UI URL (IAP OAuth clients will fail with `invalid_client` /
`no registered origin`).

Auth:

| Surface | Auth |
| --- | --- |
| Browser UI (`/`, `/ui/...`) | **Cloud Run IAP** — Google org login, no `Authorization` header |
| Scheduler / automation | Sibling service `*-worker` with Cloud Run IAM invoker + OIDC |
| JSON `/v1/*` on the UI service | Same IAP gate when opened from a browser |

IAP accessors: grant `roles/iap.httpsResourceAccessor` on the Cloud Run service
(user, group, or `domain:example.com`).

## Deploy (parameterized)

Two equivalent paths — pick one; both take the same variables:

1. **gcloud bash** — [`deploy/gcloud.sh`](deploy/gcloud.sh)
2. **Terraform (local state)** — [`deploy/terraform/`](deploy/terraform/)

```bash
# Build image first (required before terraform apply)
export PROJECT=my-gcp-project
export REGION=us-central1
export INSTANCE_CONNECTION_NAME=my-gcp-project:us-central1:metadata
export CLOUDSQL_IAM_USER=change-metadata-pooler@my-gcp-project.iam
export BQ_PROJECT=my-gcp-project
export NETWORK=projects/my-host-project/global/networks/my-vpc
export SUBNET=projects/my-host-project/regions/us-central1/subnetworks/my-subnet
export CREATE_SQL_IAM_USER=1   # creates Cloud SQL IAM DB user if missing

./deploy/gcloud.sh
```

Terraform: copy `deploy/terraform/terraform.tfvars.example` → `terraform.tfvars`,
push an image with the same tag as `image_tag`, then `terraform init && apply`.
State is local (`terraform.tfstate`); swap the backend when integrating into
org infra.

## dbt adapter

```yaml
gateway:
  cloudsql: { ... }
  pooler_url: https://change-metadata-pooler-xxxxx.run.app
```

When `pooler_url` is set, `gateway_pool_change_metadata` and
`gateway_get_affected_partitions` call this service over HTTP.
The dbt `on-run-start` graph pool remains available as a **dev/POC** path only.
