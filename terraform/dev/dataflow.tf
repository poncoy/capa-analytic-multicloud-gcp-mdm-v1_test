locals {
    CONST_DATAFLOW="${local.env.prefijo_name}_${local.env.prefijo_entorno}_gdf"   
    lis_config_gdf = jsondecode(file(abspath("./config/config_gdf.json")))
}

