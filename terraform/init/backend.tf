terraform {
  backend "gcs" {
    bucket = "ue4-nonprod-init-gcs-rapi-tfstate"
    prefix = "init"
  }
}