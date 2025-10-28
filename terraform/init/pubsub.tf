
## ==================== TOPICOS ==================== ##
## ========== para los casos de uso ===========
resource "google_pubsub_topic" "gps_cu" {
  name = "${local.CONST_PRE_PUSUB}_cu"
}
## ========== para la replica ===========
resource "google_pubsub_topic" "gps_replica" {
  name = "${local.CONST_PRE_PUSUB}_replica"
}
## ========== para terraform ===========
resource "google_pubsub_topic" "gps_terraform" {
  name = "${local.CONST_PRE_PUSUB}_terraform"
}


resource "google_pubsub_topic" "gps_bigtable" {
  name = "${local.CONST_PRE_PUSUB}_bigtable"
}


## ========== otras existentes ===========
resource "google_pubsub_subscription" "sus_cu" {
  project = local.lis_env.project
  name  = "${local.CONST_PRE_PUSUB}_suscription_cu"
  topic = google_pubsub_topic.gps_cu.name
}
resource "google_pubsub_subscription" "sus_replica" {
  project = local.lis_env.project
  name  = "${local.CONST_PRE_PUSUB}_suscription_replica"
  topic = google_pubsub_topic.gps_replica.name  
}
resource "google_pubsub_subscription" "sus_terraform" {
  project = local.lis_env.project
  name  = "${local.CONST_PRE_PUSUB}_suscription_terraform"
  topic = google_pubsub_topic.gps_terraform.name
}
resource "google_pubsub_subscription" "sus_bigtable" {
  project = local.lis_env.project
  name  = "${local.CONST_PRE_PUSUB}_suscription_bigtable"
  topic = google_pubsub_topic.gps_terraform.name
}
