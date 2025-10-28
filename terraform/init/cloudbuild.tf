

## ========== para terraform ===========
resource "google_cloudbuild_trigger" "gcb_cicd" {  
for_each = local.lis_config_gcb
  name        = replace(each.value.name,"env_name",local.lis_env.prefijo_entorno_corto)
  description = each.value.description
  location    = local.lis_env.region
  
  git_file_source {
          path      = each.value.file_yaml
          repo_type = "GITHUB" 
          revision  = "^${local.lis_env.branch_name}$"
          uri       = "https://github.com/Rimac-Seguros/capa-analytic-multicloud-gcp-mdm-v1" 
        }
  source_to_build {
    uri       = "https://github.com/Rimac-Seguros/capa-analytic-multicloud-gcp-mdm-v1"
    ref       = "refs/heads/${local.lis_env.branch_name}" 
    repo_type = "GITHUB"
  }
  
  pubsub_config {          
          topic        = replace(replace(each.value.pubsub_topic,"env_prj",local.lis_env.project),"env_name",local.lis_env.prefijo_entorno_corto)
        }
  service_account = "projects/${local.lis_env.project}/serviceAccounts/${local.lis_env.cuenta_sa.sa_despliegue}"

  substitutions = {
    _PATH_GDF= each.value.path_gdf
    _ARTIFACTS_BUCKET_NAME= replace(local.lis_env.artifacts_bucket,"env_name",local.lis_env.prefijo_entorno_corto)
    _IMAGE_NAME= each.value.image_name
    _IMAGE_TAG= "latest"
    _REGION= local.lis_env.region
    _SA_EMAIL= local.lis_env.cuenta_sa[each.value.sa_template]
    _ENV_NAME=local.lis_env.prefijo_entorno_corto
    _ENV_PRJ =local.lis_env.project
  }

  tags = ["noexpire", "CapaRapida"]
  depends_on = [google_pubsub_subscription.sus_cu]
}


resource "google_cloudbuild_trigger" "gcb_menu" {  
  name        = "ue4-${local.lis_env.prefijo_entorno_corto}-com-gcb-rapi-menu"
  description = "Menu para despliegues mediante mensajes de pubsub"
  location    = local.lis_env.region
  
  git_file_source {
          path      = "cloudbuild.yaml"
          repo_type = "GITHUB" 
          revision  = "^${local.lis_env.branch_name}$"
          uri       = "https://github.com/Rimac-Seguros/capa-analytic-multicloud-gcp-mdm-v1" 
        }
  source_to_build {
    uri       = "https://github.com/Rimac-Seguros/capa-analytic-multicloud-gcp-mdm-v1"
    ref       = "refs/heads/${local.lis_env.branch_name}"
    repo_type = "GITHUB"
  }
  github {
          name  = "capa-analytic-multicloud-gcp-mdm-v1"
          owner = "Rimac-Seguros" 
          push {
              branch       = "^${local.lis_env.branch_name}$"
              invert_regex = false
            }
        }
  service_account = "projects/${local.lis_env.project}/serviceAccounts/${local.lis_env.cuenta_sa.sa_despliegue}"
  depends_on = [google_pubsub_subscription.sus_cu]
}
