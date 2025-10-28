locals {
    CONST_BIGTABLE="${local.env.prefijo_name}-${local.env.prefijo_entorno}-gbt"
    lis_config_gbt = jsondecode(file("${local.root_dir}/config/config_gbt.json"))

    all_instances = flatten([
        for a in keys(var.bigtable_instances) : [
          for item in keys(local.lis_config_gbt) : {
            key=item
            value = local.lis_config_gbt[item]
          }if item==a
        ]    
    ])
    all_instances-map= { for item in local.all_instances: item.key => item.value }

    all_tables = flatten([ 
      for a in keys(local.all_instances-map) :[
        for x in local.all_instances-map[a] : {
          key="${a}__${x.table_name}"
          value={
            "familyId":x.familyId,
            "column_family":x.column_family
          }
        }
      ]              
    ])    
    all_tables-map= { for item in local.all_tables: item.key => item.value }
}

#===================== BIGTABLE instances ===========
resource "google_bigtable_instance" "gbt_instances" {
  for_each = var.bigtable_instances
  name = "${local.CONST_BIGTABLE}-${each.key}" 
  cluster {
    cluster_id   = "${each.key}-cluster"
    storage_type = var.bigtable_instances[each.key].storage_type
    zone    = local.env.zone   
    autoscaling_config {
      min_nodes = 1
      max_nodes = var.bigtable_instances[each.key].num_nodes
      cpu_target = 50
    }    
  }
  deletion_protection = false
  #labels = { intancia = "${local.CONST_BIGTABLE}_${each.key}" }
}

resource "google_bigtable_table" "table" {    
  for_each = local.all_tables-map
  name          = each.key
  instance_name = "${local.CONST_BIGTABLE}-${split("__",each.key)[0]}"
  #split_keys =[each.value.familyId]
  dynamic "column_family"  {
    for_each =[for x in each.value.column_family:{
                col_family= x.name }
                ]    
    content {
      family=column_family.value.col_family
    }
  }
  lifecycle { prevent_destroy = false  }
  depends_on = [google_bigtable_instance.gbt_instances]
}