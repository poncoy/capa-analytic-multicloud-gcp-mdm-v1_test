
resource "google_storage_bucket" "gcs_rapi-artifacts" {  
    name            = replace(local.lis_env.artifacts_bucket,"env_name",local.lis_env.prefijo_entorno_corto)
    location        = local.lis_env.region
    project         = local.lis_env.project
    uniform_bucket_level_access = true
    force_destroy   = true
    lifecycle_rule {
      action {
        type = "Delete"
      }
      condition {
            age                        = 2
            days_since_custom_time     = 0
            days_since_noncurrent_time = 0
            matches_prefix             = ["dataflow/tmp/"]
            matches_storage_class      = []
            matches_suffix             = []
            num_newer_versions         = 0
            with_state                 = "ANY"
      }
    }
    versioning {
        enabled = false
    }
}

resource "google_storage_bucket" "gcs_rapi-tfstate" {
    name            = replace(local.lis_env.terraform_state,"env_name",local.lis_env.prefijo_entorno_corto)
    location        = local.lis_env.region
    project         = local.lis_env.project
    uniform_bucket_level_access = true
    force_destroy   = true    
}




# terraform import google_storage_bucket.gcs_rapi-tfstate ue4-nonprod-stg-gcs-rapi-tfstate
# terraform import google_storage_bucket.gcs_rapi-artifacts ue4-nonprod-stg-gcs-rapi-artifacts

