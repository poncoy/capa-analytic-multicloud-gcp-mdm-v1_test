/*valor fijado para cada entorno no se cambia*/
terraform {
  backend "gcs" {
    bucket = "ue4-nonprod-stg-gcs-rapi-tfstate"
    prefix = "develop"
  }
}

locals {  
  env=jsondecode(file(abspath("./config/config_env.json")))
  
}