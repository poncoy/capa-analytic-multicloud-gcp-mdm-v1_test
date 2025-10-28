locals {
  ambiente="develop"  
  lis_env        = jsondecode(file("../../config/${local.ambiente}/config_env.json"))
  lis_config_gbq = jsondecode(file("../../config/${local.ambiente}/config_gbq.json"))  
  lis_config_gcb = jsondecode(file("../../config/${local.ambiente}/config_gcb.json"))    
  lis_config_gbt = jsondecode(file("../../config/${local.ambiente}/config_gbt.json"))    
  CONST_PRE_PUSUB="${local.lis_env.prefijo_name}-${local.lis_env.prefijo_entorno}-gps"  
}