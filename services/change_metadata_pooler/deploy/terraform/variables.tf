variable "project" {
  type        = string
  description = "GCP project for Cloud Run, Artifact Registry, Scheduler"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "service_name" {
  type    = string
  default = "change-metadata-pooler"
}

variable "ar_repo" {
  type    = string
  default = "change-metadata-pooler"
}

variable "sa_name" {
  type    = string
  default = "change-metadata-pooler"
}

variable "image_tag" {
  type        = string
  description = "Container image tag already pushed to Artifact Registry"
}

variable "instance_connection_name" {
  type        = string
  description = "Cloud SQL instance connection name (project:region:instance)"
}

variable "cloudsql_iam_user" {
  type        = string
  description = "Cloud SQL IAM DB user (service-account@project.iam)"
}

variable "cloudsql_database" {
  type    = string
  default = "metadata"
}

variable "cloudsql_schema" {
  type    = string
  default = "public"
}

variable "cloudsql_ip_type" {
  type    = string
  default = "private"
}

variable "bq_project" {
  type        = string
  description = "BigQuery execution / billing project"
}

variable "bq_location" {
  type    = string
  default = "US"
}

variable "worker_pool_size" {
  type    = number
  default = 8
}

variable "vpc_connector" {
  type        = string
  default     = ""
  description = "Full VPC connector resource name, or empty to use Direct VPC"
}

variable "network" {
  type        = string
  default     = ""
  description = "VPC network for Direct VPC egress (e.g. projects/HOST/global/networks/NAME)"
}

variable "subnet" {
  type        = string
  default     = ""
  description = "Subnet for Direct VPC egress"
}

variable "create_scheduler" {
  type    = bool
  default = true
}

variable "schedule" {
  type    = string
  default = "*/10 * * * *"
}

variable "oauth_client_id" {
  type        = string
  default     = ""
  description = "Web OAuth client id for GIS Grant & retry (UI)"
}

variable "pooler_sa_email" {
  type        = string
  default     = ""
  description = "Full runtime SA email; defaults to sa_name@project"
}
