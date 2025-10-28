## local variable
locals {
  timestamp = formatdate("YYMMDDhhmmss", timestamp())
  root_dir_gcf = abspath("../../src/gcf/")  
  lis_config_gcf = jsondecode(file(abspath("./config/config_gcf.json")))
}

#======== gcf_instance_http ==========
# Compress source code
data "archive_file" "zip_files_instances" {
  for_each = local.lis_config_gcf
  type        = "zip"  
  output_path = "/tmp/${each.value.name}.zip"
  source_dir  = "${local.root_dir_gcf}/${each.value.name}/"
}
# Add source code zip to bucket
resource "google_storage_bucket_object" "gcs_zip_in_instances" {  
  for_each = local.lis_config_gcf  
  name   = "cloud_functions/${each.value.name}/${data.archive_file.zip_files_instances[each.key].output_md5}.zip"  
  bucket = each.value.bucket_stg_gcf
  source = data.archive_file.zip_files_instances[each.key].output_path
  depends_on = [data.archive_file.zip_files_instances]
}
# Add source instance
resource "google_cloudfunctions2_function" "gcf_instances" {
  for_each = local.lis_config_gcf
    provider = google-beta
    name    = each.value.name
    project = local.env.project
    location  = local.env.region
    description = each.value.description
    build_config {
      runtime = each.value.runtime
      entry_point = each.value.entry_point
      environment_variables = each.value.environment_variables
      source {
        storage_source {
          bucket = local.env.artifacts_bucket
          object = google_storage_bucket_object.gcs_zip_in_instances[each.key].name
        }
      }      
    }
    service_config {
      max_instance_count  = each.value.max_instances
      min_instance_count = each.value.min_instances
      available_memory    = each.value.available_memory_mb
      timeout_seconds     = each.value.timeout
      environment_variables = {          
          PROJECT_GBT = local.env.project
          GBT_INSTANCE = "${local.env.prefijo_name}-${local.env.prefijo_entorno}-gbt-uni"
      }
      ingress_settings = each.value.ingress_settings        
      all_traffic_on_latest_revision = true
      service_account_email = local.env.cuenta_sa.sa_ejec_gcf
    }
    labels = each.value.labels
#    trigger_http          = true
#    vpc_connector         = each.value.vpc_connector
    depends_on = [google_storage_bucket_object.gcs_zip_in_instances]
}

