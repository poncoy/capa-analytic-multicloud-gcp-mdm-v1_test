

resource "google_bigquery_table" "gbq_landing" {
for_each = local.lis_config_gbq.landing 
  dataset_id = each.value.dataset
  table_id   = each.value.table
  project = local.lis_env.project
  time_partitioning {
    type = "DAY"
    field ="periodo"
  }
  labels = {
    env = "default"
  }

  schema = file("${each.value.ddl_root}/${each.value.dataset}.${each.value.table}.json")
  deletion_protection=false



  lifecycle {
        ignore_changes = [last_modified_time]
      }

}

resource "google_bigquery_table" "gbq_landing_merge" {
for_each = local.lis_config_gbq.landing 
  dataset_id = each.value.dataset
  table_id   = "${each.value.table}_merge"
  project = local.lis_env.project
  time_partitioning {
    type = "DAY"
    field ="periodo"
  }
  labels = {
    env = "default"
  }

  schema = file("${each.value.ddl_root}/${each.value.dataset}.${each.value.table}.json")
  deletion_protection=false
  lifecycle {
        ignore_changes = [last_modified_time,schema]
      }

}

resource "google_bigquery_table" "gbq_pre" {
for_each = local.lis_config_gbq.pre_universal 
  dataset_id = each.value.dataset
  table_id   = each.value.table
  project = local.lis_env.project
  time_partitioning {
    type = "DAY"
    field =each.value.part_field
  }
  labels = {
    env = "default"
  }

  schema = file("${each.value.ddl_root}/${each.value.dataset}.${each.value.table}.json")
  deletion_protection=false
  lifecycle {
          ignore_changes = [last_modified_time]
        }
}
