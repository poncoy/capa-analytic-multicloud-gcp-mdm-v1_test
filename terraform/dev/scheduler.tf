locals {
    CONST_PRE_SCHEDULE="${local.env.prefijo_name}-${local.env.prefijo_entorno}-gsc"
    lis_config_gsc = jsondecode(file(abspath("./config/config_gsc.json")))
    
}

resource "google_cloud_scheduler_job" "job_ingesta" {
for_each = local.lis_config_gsc
  name             = "${local.CONST_PRE_SCHEDULE}-${each.value.name}"    
  description      = each.value.description
  schedule         = each.value.schedule
  #time_zone        = "America/New_York"
  attempt_deadline = each.value.attempt_deadline

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "POST"    
    uri         = "https://dataflow.googleapis.com/v1b3/projects/${local.env.project}/locations/${local.env.region}/flexTemplates:launch"
    oauth_token {
      service_account_email = local.env.cuenta_sa[each.value.name]
    }

    body        = base64encode(<<-EOT
    {
     "launchParameter": {
       "jobName": "${local.lis_config_gdf[each.value.name].job_name}",
       "parameters": {
         "setup_file": "${local.lis_config_gdf[each.value.name].params_file_setup}",
         "use_case": "${local.lis_config_gdf[each.value.name].use_case}"         
       },
       "environment": {
         "ipConfiguration": "WORKER_IP_PRIVATE",
         "network": "${local.env.network}",
         "subnetwork": "${local.env.subnetwork}",        
         "tempLocation": "${local.lis_config_gdf[each.value.name].root_temp_location_base}",
         "stagingLocation": "${local.lis_config_gdf[each.value.name].root_stg_location}",
         "serviceAccountEmail": "${local.env.cuenta_sa[each.value.name]}"
       },
       "containerSpecGcsPath": "${local.lis_config_gdf[each.value.name].root_template_location_base}"
     }
   }
  EOT
    )
            
  }
}
