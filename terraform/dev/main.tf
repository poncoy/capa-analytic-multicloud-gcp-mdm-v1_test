locals {
  CONST_BUCKET="${local.env.prefijo_name}_${local.env.prefijo_entorno}_gcs"
  root_dir = abspath("./")    
  files_config_json = fileset(local.root_dir, "config/*.json")
  lis_config = {for item in local.files_config_json: replace(replace(item,".json",""),"config/","") => jsondecode(file("${local.root_dir}/${item}"))}
}

#----------------------- === STORAGE ===   ----------------------------
#resource "google_storage_bucket" "bucket" {
#  for_each = var.dataflow_item
#  location = local.env.region
#  name = "${local.CONST_BUCKET}_${each.key}"
#  uniform_bucket_level_access = true  
#  force_destroy = true
#}






