output "service_url" {
  value = google_cloud_run_v2_service.pooler.uri
}

output "service_account_email" {
  value = google_service_account.pooler.email
}

output "image" {
  value = local.image
}

output "artifact_registry_repo" {
  value = google_artifact_registry_repository.repo.id
}
