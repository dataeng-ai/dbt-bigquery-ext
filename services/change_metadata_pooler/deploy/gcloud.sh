#!/usr/bin/env bash
# Parameterized gcloud deploy for change-metadata-pooler (Cloud Run + Scheduler).
# Mirrors deploy/terraform; use either path — same resources when vars match.
#
# Required env (or flags below):
#   PROJECT                  GCP project for Cloud Run / Artifact Registry
#   REGION                   e.g. us-central1
#   INSTANCE_CONNECTION_NAME Cloud SQL instance (project:region:instance)
#   CLOUDSQL_IAM_USER        IAM DB user (e.g. pooler@my-gcp-project.iam)
#   BQ_PROJECT               BigQuery execution / billing project
#
# Optional:
#   SERVICE_NAME             default: change-metadata-pooler
#   AR_REPO                  default: change-metadata-pooler
#   SA_NAME                  default: change-metadata-pooler
#   VPC_CONNECTOR            full resource name, or empty for Direct VPC
#   NETWORK / SUBNET         for Direct VPC egress (Shared VPC OK)
#   SCHEDULE                 cron, default: */10 * * * * (every 10 min)
#   SCHEDULER_SA             SA email for OIDC to Cloud Run (default: runtime SA)
#   IMAGE_TAG                default: git sha or timestamp
#   SKIP_SCHEDULER           set to 1 to skip Cloud Scheduler job
#   CREATE_SQL_IAM_USER      set to 1 to attempt Cloud SQL IAM user create
#   SQL_ADMIN_PROJECT        project hosting Cloud SQL (for IAM user / client role)
#   OAUTH_CLIENT_ID          Web OAuth client id for GIS "Grant & retry" (UI service)
#   POOLER_SA_EMAIL          Full runtime SA email (default: SA_NAME@PROJECT.iam.gserviceaccount.com)
#   ALSO_DEPLOY_WORKER       set to 1 to also update SERVICE_NAME-worker with same image/env
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PROJECT="${PROJECT:?PROJECT is required}"
REGION="${REGION:-us-central1}"
SERVICE_NAME="${SERVICE_NAME:-change-metadata-pooler}"
AR_REPO="${AR_REPO:-change-metadata-pooler}"
SA_NAME="${SA_NAME:-change-metadata-pooler}"
INSTANCE_CONNECTION_NAME="${INSTANCE_CONNECTION_NAME:?INSTANCE_CONNECTION_NAME is required}"
CLOUDSQL_IAM_USER="${CLOUDSQL_IAM_USER:?CLOUDSQL_IAM_USER is required}"
BQ_PROJECT="${BQ_PROJECT:-$PROJECT}"
BQ_LOCATION="${BQ_LOCATION:-US}"
CLOUDSQL_DATABASE="${CLOUDSQL_DATABASE:-metadata}"
CLOUDSQL_SCHEMA="${CLOUDSQL_SCHEMA:-public}"
CLOUDSQL_IP_TYPE="${CLOUDSQL_IP_TYPE:-private}"
WORKER_POOL_SIZE="${WORKER_POOL_SIZE:-8}"
SCHEDULE="${SCHEDULE:-*/10 * * * *}"
SKIP_SCHEDULER="${SKIP_SCHEDULER:-0}"
CREATE_SQL_IAM_USER="${CREATE_SQL_IAM_USER:-0}"
SQL_ADMIN_PROJECT="${SQL_ADMIN_PROJECT:-${INSTANCE_CONNECTION_NAME%%:*}}"
VPC_CONNECTOR="${VPC_CONNECTOR:-}"
NETWORK="${NETWORK:-}"
SUBNET="${SUBNET:-}"
IMAGE_TAG="${IMAGE_TAG:-$(date -u +%Y%m%d%H%M%S)}"
OAUTH_CLIENT_ID="${OAUTH_CLIENT_ID:-}"
ALSO_DEPLOY_WORKER="${ALSO_DEPLOY_WORKER:-0}"

SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
POOLER_SA_EMAIL="${POOLER_SA_EMAIL:-$SA_EMAIL}"
AR_HOST="${REGION}-docker.pkg.dev"
IMAGE="${AR_HOST}/${PROJECT}/${AR_REPO}/${SERVICE_NAME}:${IMAGE_TAG}"

log() { printf '+ %s\n' "$*"; }

log "project=$PROJECT region=$REGION service=$SERVICE_NAME"

# --- APIs ---
log "enable APIs"
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  sqladmin.googleapis.com \
  --project="$PROJECT"

# --- Artifact Registry ---
if ! gcloud artifacts repositories describe "$AR_REPO" \
  --project="$PROJECT" --location="$REGION" >/dev/null 2>&1; then
  log "create Artifact Registry repo $AR_REPO"
  gcloud artifacts repositories create "$AR_REPO" \
    --project="$PROJECT" \
    --location="$REGION" \
    --repository-format=docker \
    --description="change-metadata-pooler images"
fi

# --- Runtime SA ---
if ! gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT" >/dev/null 2>&1; then
  log "create service account $SA_EMAIL"
  gcloud iam service-accounts create "$SA_NAME" \
    --project="$PROJECT" \
    --display-name="Change metadata pooler"
fi

# IAM bindings can race SA creation — retry briefly
bind_role() {
  local project="$1" role="$2"
  local i
  for i in 1 2 3 4 5; do
    if gcloud projects add-iam-policy-binding "$project" \
      --member="serviceAccount:${SA_EMAIL}" \
      --role="$role" \
      --condition=None \
      --quiet >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  log "WARNING: failed to bind $role on $project for $SA_EMAIL"
}

bind_role "$BQ_PROJECT" "roles/bigquery.jobUser"
bind_role "$BQ_PROJECT" "roles/bigquery.dataViewer"
bind_role "$BQ_PROJECT" "roles/bigquery.dataEditor"
bind_role "$SQL_ADMIN_PROJECT" "roles/cloudsql.client"

if [[ "$CREATE_SQL_IAM_USER" == "1" ]]; then
  SQL_INSTANCE="${INSTANCE_CONNECTION_NAME##*:}"
  # Cloud SQL IAM SA user name omits ".gserviceaccount.com"
  SQL_IAM_NAME="${SA_EMAIL%.gserviceaccount.com}"
  log "ensure Cloud SQL IAM user $SQL_IAM_NAME on $SQL_INSTANCE"
  if ! gcloud sql users list --instance="$SQL_INSTANCE" --project="$SQL_ADMIN_PROJECT" \
    --format='value(name)' | grep -qx "$SQL_IAM_NAME"; then
    gcloud sql users create "$SQL_IAM_NAME" \
      --instance="$SQL_INSTANCE" \
      --project="$SQL_ADMIN_PROJECT" \
      --type=cloud_iam_service_account || true
  fi
fi

# --- Build & push (Cloud Build by default; set USE_LOCAL_DOCKER=1 for local docker) ---
USE_LOCAL_DOCKER="${USE_LOCAL_DOCKER:-0}"
if [[ "$USE_LOCAL_DOCKER" == "1" ]]; then
  log "configure docker auth"
  gcloud auth configure-docker "$AR_HOST" --quiet
  log "build $IMAGE (local docker)"
  docker build -f "$SERVICE_DIR/Dockerfile" -t "$IMAGE" "$ROOT"
  log "push $IMAGE"
  docker push "$IMAGE"
else
  log "build $IMAGE (Cloud Build)"
  gcloud builds submit "$ROOT" \
    --project="$PROJECT" \
    --config="$SERVICE_DIR/deploy/cloudbuild.yaml" \
    --substitutions="_IMAGE=${IMAGE}"
fi

# --- Cloud Run ---
ENV_VARS="INSTANCE_CONNECTION_NAME=${INSTANCE_CONNECTION_NAME},CLOUDSQL_IAM_USER=${CLOUDSQL_IAM_USER},CLOUDSQL_DATABASE=${CLOUDSQL_DATABASE},CLOUDSQL_SCHEMA=${CLOUDSQL_SCHEMA},CLOUDSQL_IP_TYPE=${CLOUDSQL_IP_TYPE},BQ_PROJECT=${BQ_PROJECT},BQ_LOCATION=${BQ_LOCATION},WORKER_POOL_SIZE=${WORKER_POOL_SIZE},POOLER_SA_EMAIL=${POOLER_SA_EMAIL}"
if [[ -n "$OAUTH_CLIENT_ID" ]]; then
  ENV_VARS="${ENV_VARS},OAUTH_CLIENT_ID=${OAUTH_CLIENT_ID}"
fi

deploy_run_service() {
  local name="$1"
  local args=(
    run deploy "$name"
    --project="$PROJECT"
    --region="$REGION"
    --image="$IMAGE"
    --service-account="$SA_EMAIL"
    --no-allow-unauthenticated
    --port=8080
    --timeout=900
    --cpu=1
    --memory=1Gi
    --min-instances=0
    --max-instances=5
    --set-env-vars="$ENV_VARS"
  )
  if [[ -n "$VPC_CONNECTOR" ]]; then
    args+=(--vpc-connector="$VPC_CONNECTOR" --vpc-egress=private-ranges-only)
  elif [[ -n "$NETWORK" && -n "$SUBNET" ]]; then
    args+=(--network="$NETWORK" --subnet="$SUBNET" --vpc-egress=private-ranges-only)
  else
    log "WARNING: no VPC_CONNECTOR or NETWORK/SUBNET — private Cloud SQL may be unreachable"
  fi
  log "deploy Cloud Run $name"
  gcloud "${args[@]}"
}

deploy_run_service "$SERVICE_NAME"
if [[ "$ALSO_DEPLOY_WORKER" == "1" ]]; then
  deploy_run_service "${SERVICE_NAME}-worker"
fi

SERVICE_URL="$(gcloud run services describe "$SERVICE_NAME" \
  --project="$PROJECT" --region="$REGION" --format='value(status.url)')"
log "service_url=$SERVICE_URL"

# Invoker for scheduler SA (and deployer for smoke tests)
SCHEDULER_SA="${SCHEDULER_SA:-$SA_EMAIL}"
gcloud run services add-iam-policy-binding "$SERVICE_NAME" \
  --project="$PROJECT" \
  --region="$REGION" \
  --member="serviceAccount:${SCHEDULER_SA}" \
  --role="roles/run.invoker" \
  --quiet >/dev/null || true

if [[ "$SKIP_SCHEDULER" != "1" ]]; then
  JOB_NAME="${SERVICE_NAME}-scheduled"
  log "upsert Cloud Scheduler job $JOB_NAME"
  if gcloud scheduler jobs describe "$JOB_NAME" \
    --project="$PROJECT" --location="$REGION" >/dev/null 2>&1; then
    gcloud scheduler jobs update http "$JOB_NAME" \
      --project="$PROJECT" \
      --location="$REGION" \
      --schedule="$SCHEDULE" \
      --uri="${SERVICE_URL}/v1/pool/scheduled" \
      --http-method=POST \
      --oidc-service-account-email="$SCHEDULER_SA" \
      --oidc-token-audience="$SERVICE_URL" \
      --message-body='{}' \
      --headers=Content-Type=application/json
  else
    gcloud scheduler jobs create http "$JOB_NAME" \
      --project="$PROJECT" \
      --location="$REGION" \
      --schedule="$SCHEDULE" \
      --uri="${SERVICE_URL}/v1/pool/scheduled" \
      --http-method=POST \
      --oidc-service-account-email="$SCHEDULER_SA" \
      --oidc-token-audience="$SERVICE_URL" \
      --message-body='{}' \
      --headers=Content-Type=application/json \
      --time-zone=UTC
  fi
fi

cat <<EOF

Deploy complete.
  URL:     $SERVICE_URL
  Image:   $IMAGE
  SA:      $SA_EMAIL

Smoke test (as an identity with roles/run.invoker):
  TOKEN=\$(gcloud auth print-identity-token --audiences="$SERVICE_URL")
  curl -sS -H "Authorization: Bearer \$TOKEN" "$SERVICE_URL/healthz"
EOF
