terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.37.0"
    }

    archive = {
      source = "hashicorp/archive"
      version = "2.2.0"
    }
  }
}

provider "google" {
  project = local.lis_env.project
  region  = local.lis_env.region
}

provider "google-beta" {
  project = local.lis_env.project
  region  = local.lis_env.region
}