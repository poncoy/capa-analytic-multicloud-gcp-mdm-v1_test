--#######################################################################
 
--# Autor                    Fecha               Version               Detalle
--# JorgeHenriquez26        08/08/2024            1.0                - Se agregan 2 nuevas variables pertenecientes a cliente_persona_detalle, para analisis
--# JorgeHenriquez26        30/09/2024            1.0                - Se actualiza filtro des_agrupacion_n2 = 'VEHÍCULOS' a des_agrupacion_n2 = 'VEHICULOS' por lineamiento
--# JorgeHenriquez26        21/10/2024            1.0                - Se quita exclusion de variable des_agrupacion_n2 "DESGRAVAMEN"
--#######################################################################

with cliente_pers_ult_periodo as (
    select max(periodo) as periodo
    from `{PROJECT_ID}.lan_mdm_ingesta.PER__cliente_persona`
) select distinct
    CONCAT(cp.tip_documento,"#",cp.num_documento,"#",cpd.id_producto,"#",cpd.cod_producto_origen) as C000_id_bloqueo_compra_previa,
    cp.tip_documento as C001_tip_documento,
    cp.num_documento as C001_num_documento,
    cpd.des_agrupacion_n1 as C001_des_agrupacion_n1,
    cpd.des_agrupacion_n2 as C001_des_agrupacion_n2,
    cpd.des_agrupacion_n3 as C001_des_agrupacion_n3,
    SUBSTR(cpd.id_producto,4,LENGTH(cpd.id_producto)-3) as C001_cod_producto_origen,
    iv.num_placa as C001_num_placa,
    cpd.pol_des_subcanal as C001_des_subcanal,
    cp.periodo as C001_periodo
from `{PROJECT_ID}.lan_mdm_ingesta.PER__cliente_persona_detalle_merge` cpd
left join `{PROJECT_ID}.lan_mdm_ingesta.PER__cliente_persona` cp
    on cp.id_cliente_persona = cpd.id_cliente_persona
        and cp.periodo = cpd.periodo
left join unnest (cpd.info_vehiculo) as iv
where
    cpd.pol_des_canal in (
        "CANAL NO TRADICIONAL",
        "CORREDORES"
    )
    and (
        (des_agrupacion_n2 = 'VEHICULOS' and cpd.id_producto != 'AX-9040') or
        des_agrupacion_n2 = 'AM INDIVIDUAL' or
        ( des_riesgo = 'VIDA' and des_agrupacion_n2 not in ('PREVISIONALES')) or
        des_agrupacion_n2 = 'DOMICILIARIO' or
        des_agrupacion_n2 = 'SOAT'
    )
    and cp.tip_documento in ('DNI', 'PA', 'CE')
    and cp.periodo in (select periodo from cliente_pers_ult_periodo)
