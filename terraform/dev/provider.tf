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
  project = local.env.project
  region  = local.env.region
}

provider "google-beta" {
  project = local.env.project
  region  = local.env.region
}