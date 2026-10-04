terraform {
  required_version = ">= 1.5.0"
  # Local state by default — copy backend block into your infra repo when ready.
  backend "local" {
    path = "terraform.tfstate"
  }
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.30.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

locals {
  sa_email        = google_service_account.pooler.email
  pooler_sa_email = var.pooler_sa_email != "" ? var.pooler_sa_email : google_service_account.pooler.email
  image           = "${var.region}-docker.pkg.dev/${var.project}/${var.ar_repo}/${var.service_name}:${var.image_tag}"
  sql_project     = split(":", var.instance_connection_name)[0]
}

resource "google_project_service" "apis" {
  for_each = toset([
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudscheduler.googleapis.com",
    "iam.googleapis.com",
    "sqladmin.googleapis.com",
  ])
  project            = var.project
  service            = each.value
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "repo" {
  project       = var.project
  location      = var.region
  repository_id = var.ar_repo
  format        = "DOCKER"
  description   = "change-metadata-pooler images"
  depends_on    = [google_project_service.apis]
}

resource "google_service_account" "pooler" {
  project      = var.project
  account_id   = var.sa_name
  display_name = "Change metadata pooler"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.bq_project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${local.sa_email}"
}

resource "google_project_iam_member" "bq_data_viewer" {
  project = var.bq_project
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${local.sa_email}"
}

resource "google_project_iam_member" "cloudsql_client" {
  project = local.sql_project
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${local.sa_email}"
}

resource "google_cloud_run_v2_service" "pooler" {
  name     = var.service_name
  project  = var.project
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = local.sa_email
    timeout         = "900s"
    scaling {
      min_instance_count = 0
      max_instance_count = 5
    }
    containers {
      image = local.image
      ports {
        container_port = 8080
      }
      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
      }
      env {
        name  = "INSTANCE_CONNECTION_NAME"
        value = var.instance_connection_name
      }
      env {
        name  = "CLOUDSQL_IAM_USER"
        value = var.cloudsql_iam_user
      }
      env {
        name  = "CLOUDSQL_DATABASE"
        value = var.cloudsql_database
      }
      env {
        name  = "CLOUDSQL_SCHEMA"
        value = var.cloudsql_schema
      }
      env {
        name  = "CLOUDSQL_IP_TYPE"
        value = var.cloudsql_ip_type
      }
      env {
        name  = "BQ_PROJECT"
        value = var.bq_project
      }
      env {
        name  = "BQ_LOCATION"
        value = var.bq_location
      }
      env {
        name  = "WORKER_POOL_SIZE"
        value = tostring(var.worker_pool_size)
      }
      env {
        name  = "POOLER_SA_EMAIL"
        value = local.pooler_sa_email
      }
      dynamic "env" {
        for_each = var.oauth_client_id != "" ? [1] : []
        content {
          name  = "OAUTH_CLIENT_ID"
          value = var.oauth_client_id
        }
      }
    }

    dynamic "vpc_access" {
      for_each = var.vpc_connector != "" ? [1] : []
      content {
        connector = var.vpc_connector
        egress    = "PRIVATE_RANGES_ONLY"
      }
    }

    dynamic "vpc_access" {
      for_each = var.vpc_connector == "" && var.network != "" && var.subnet != "" ? [1] : []
      content {
        network_interfaces {
          network    = var.network
          subnetwork = var.subnet
        }
        egress = "PRIVATE_RANGES_ONLY"
      }
    }
  }

  depends_on = [
    google_project_service.apis,
    google_artifact_registry_repository.repo,
  ]

  lifecycle {
    ignore_changes = [
      # Image may be updated by gcloud.sh / CI without TF apply
      template[0].containers[0].image,
      client,
      client_version,
    ]
  }
}

resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  count    = var.create_scheduler ? 1 : 0
  project  = var.project
  location = var.region
  name     = google_cloud_run_v2_service.pooler.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${local.sa_email}"
}

resource "google_cloud_scheduler_job" "pool_scheduled" {
  count     = var.create_scheduler ? 1 : 0
  project   = var.project
  region    = var.region
  name      = "${var.service_name}-scheduled"
  schedule  = var.schedule
  time_zone = "UTC"

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.pooler.uri}/v1/pool/scheduled"
    body        = base64encode("{}")
    headers = {
      Content-Type = "application/json"
    }
    oidc_token {
      service_account_email = local.sa_email
      audience              = google_cloud_run_v2_service.pooler.uri
    }
  }

  depends_on = [google_cloud_run_v2_service_iam_member.scheduler_invoker]
}
