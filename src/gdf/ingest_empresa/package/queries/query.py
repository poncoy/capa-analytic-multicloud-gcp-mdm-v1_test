# QUERY_PERSONA__SCORE_VENTA_SALUD = """SELECT 
#     id_persona,
#     timestamp(upload_timestamp) upload_timestamp,
#     per_persistenciavn_ami_cls,per_propensionvn_ami_cls,
#     scr_persistenciavn_ami,scr_siniestralidadvn_ami
#     FROM `rs-nprd-dlk-ia-dev-aif-d3d9.deliver.persona__score_venta_salud` 
#     WHERE periodo = (SELECT MAX(PERIODO) FROM `rs-nprd-dlk-ia-dev-aif-d3d9.deliver.persona__score_venta_salud`)  
#      and id_persona in  ("AX-10399504","AX-10602568","AX-10562216");
# """

# QUERY_2 = """SELECT 
#     {KEYS},
#     timestamp(upload_timestamp) upload_timestamp,
#     {COLUMNS}
#     FROM `rs-nprd-dlk-ia-dev-aif-d3d9.deliver.{TABLE}` 
#     WHERE periodo = (SELECT MAX(PERIODO) FROM `rs-nprd-dlk-ia-dev-aif-d3d9.deliver.{TABLE}`)  
#      and id_persona in ("AX-3092180","AX-3661115","AX-10833867");
# """

#Query dinamico para  la obtencion de datos con periodo
QUERY = """
    SELECT 
    {COLUMNS}
    FROM `{TABLE}` 
    WHERE periodo >= date_add(CURRENT_DATE(), INTERVAL -3 MONTH);
"""
