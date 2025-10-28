with cliente_pers_ult_periodo as (
    select max(periodo) as periodo
    from 
    `rs-nprd-dlk-dt-anlyt-mlk-0c8a.anl_persona.cliente_persona`    
),
direccion as (
    SELECT id_persona
        , ddc.datos_direccion.des_departamento
        , ddc.datos_direccion.des_provincia
        , ddc.datos_direccion.des_distrito
    FROM `rs-nprd-dlk-dt-anlyt-mlk-0c8a.anl_contactabilidad.datos_contacto` dc
    CROSS JOIN dc.detalle_datos_contacto ddc
    WHERE ddc.des_tipo = 'COMERCIAL' and dc.periodo in (select date_add(periodo, interval -1 month) from cliente_pers_ult_periodo)
),
cmp as (
    SELECT
        cp.cuc,
        cp.id_cliente_persona as id_persona,
        cp.tip_documento,
        cp.num_documento,
        cp.nombres,
        cp.ape_paterno,
        cp.ape_materno,
        cp.fec_nacimiento,
        cp.ind_empleado_rimac,
        cp.num_edad,
        CASE 
            WHEN cp.ind_menor_edad = 'NO DETERMINADO' THEN 'NO'
            ELSE cp.ind_menor_edad
        END as ind_menor_edad,
        cp.nse_rimac,
        cp.des_sexo,
        cp.nse_agrup,
        case
            when upper(cast(cp.ext_ind_dependiente_laboral as string)) in ('S', '1', 'SI')
                then 'SI'
            else 'NO'
        end as ext_ind_dependiente_laboral,
        case
            when upper(cast(cp.ext_ind_tiene_tarjeta_credito as string)) in ('S', '1', 'SI')
                then 'SI'
            else 'NO'
        end as ext_ind_tiene_tarjeta_credito,
        'SI' as ind_cliente,
        cp.periodo,
        cp.ind_blacklist,
        case
            WHEN CAST(cp.ind_ley_datos_personales AS STRING) in ('1')
                THEN 'SI'
            ELSE 'NO'
        end as ind_ley_datos_personales,
        case
            WHEN CAST(cp.ind_consentimiento_comercial AS STRING) in ('1')
                THEN 'SI'
            ELSE 'NO'
        end as ind_consentimiento_comercial,
        cp.ext_des_empresa_laboral,
        cp.ext_ruc_empresa_laboral,
        cp.des_segmentacion_growth,
        cp.ind_fallecido,
        cp.ind_royal,
        cp.ind_cliente_nuevo,
        cp.ind_critico,
        CASE   
            WHEN cp.ind_trabaja_empresa_seguros = 'NO DETERMINADO' THEN 'NO'
            ELSE cp.ind_trabaja_empresa_seguros
        END as ind_trabaja_empresa_seguros,
        CASE 
            WHEN cp.ind_cliente_vip IS NULL THEN 'NO'
            ELSE cp.ind_cliente_vip
        END as ind_cliente_vip,
        CASE 
            WHEN cp.ind_trabajador_empresa_riesgo_desuscrito = 'NO DETERMINADO' THEN 'NO'
            ELSE cp.ind_trabajador_empresa_riesgo_desuscrito
        END as ind_trabajador_empresa_riesgo_desuscrito,
        cp.ind_cliente_corredor_bloqueo_comercial
    FROM `rs-nprd-dlk-dt-anlyt-mlk-0c8a.anl_persona.cliente_persona` cp
    WHERE
        cp.periodo in (select periodo from cliente_pers_ult_periodo)
    UNION ALL
    SELECT
        pp.cuc,
        pp.id_prospecto_persona as id_persona,
        pp.tip_documento,
        pp.num_documento,
        pp.nombres,
        pp.ape_paterno,
        pp.ape_materno,
        pp.fec_nacimiento,
        PP.ind_empleado_rimac,
        pp.num_edad,
        CASE 
            WHEN pp.ind_menor_edad = 'NO DETERMINADO' THEN 'NO'
            ELSE pp.ind_menor_edad
        END as ind_menor_edad,
        pp.nse_rimac,
        pp.des_sexo,
        pp.nse_agrup,
        pp.ext_ind_dependiente_laboral,
        pp.ext_ind_tiene_tarjeta_credito,
        'NO' as ind_cliente,
        pp.periodo,
        pp.ind_blacklist,
        CAST(pp.ind_ley_datos_personales AS STRING) AS ind_ley_datos_personales,
        CAST(pp.ind_consentimiento_comercial AS STRING) AS ind_consentimiento_comercial,
        pp.ext_des_empresa_laboral,
        pp.ext_ruc_empresa_laboral,
        pp.des_segmentacion_growth,
        pp.ind_fallecido,
        pp.ind_royal,
        pp.ind_cliente_nuevo,
        pp.ind_critico,
        CASE
            WHEN pp.ind_trabaja_empresa_seguros = 'NO DETERMINADO' THEN 'NO'
            ELSE pp.ind_trabaja_empresa_seguros
        END as ind_trabaja_empresa_seguros,
        CASE 
            WHEN pp.ind_cliente_vip IS NULL THEN 'NO'
            ELSE pp.ind_cliente_vip
        END as ind_cliente_vip,
        CASE 
            WHEN pp.ind_trabajador_empresa_riesgo_desuscrito = 'NO DETERMINADO' THEN 'NO'
            ELSE pp.ind_trabajador_empresa_riesgo_desuscrito
        END as ind_trabajador_empresa_riesgo_desuscrito,
        'NO APLICA' as ind_cliente_corredor_bloqueo_comercial
    FROM `rs-nprd-dlk-dt-anlyt-mlk-0c8a.anl_persona.prospecto_persona` pp
    WHERE
        pp.periodo in (select periodo from cliente_pers_ult_periodo)
)
SELECT
    CONCAT(cmp.tip_documento,'#',cmp.num_documento,"#",cmp.cuc,"#",cmp.id_persona) as C000_row_key,    
    cmp.id_persona as C000_id_persona,
    cmp.tip_documento as C000_tip_documento,
    cmp.num_documento as C000_num_documento,
    cmp.periodo as C000_periodo,
    cmp.cuc as C000_cod_claveunicocliente,
    IF(cmp.tip_documento<>'RUC',cmp.nombres,'') as C002_nom_persona,
    IF(cmp.tip_documento<>'RUC',cmp.ape_paterno,'')  as C002_ape_paternopersona,
    IF(cmp.tip_documento<>'RUC',cmp.ape_materno,'')  as C002_ape_maternopersona,
    IF(cmp.tip_documento<>'RUC',cmp.fec_nacimiento,NULL)  as C002_fec_nacimiento,
    IF(cmp.tip_documento<>'RUC',cmp.nse_rimac,'')  as C002_cod_nivel_socio_economico,
    IF(cmp.tip_documento<>'RUC',cmp.nse_agrup,'')  as C002_cod_nivel_socio_economico_agrupado,
    IF(cmp.tip_documento<>'RUC',dir.des_departamento,'')  as C002_nom_departamento,
    IF(cmp.tip_documento<>'RUC',dir.des_provincia,'')  as C002_nom_provincia,
    IF(cmp.tip_documento<>'RUC',dir.des_distrito,'')  as C002_nom_distrito,
    IF(cmp.tip_documento<>'RUC',cmp.num_edad,NULL)  as C002_num_edad,
    IF(cmp.tip_documento<>'RUC',cmp.ind_menor_edad,'')  as C002_ind_menor_edad,
    IF(cmp.tip_documento<>'RUC',cmp.ext_ind_dependiente_laboral,'')  as C002_ind_dependiente_laboral,
    IF(cmp.tip_documento<>'RUC',cmp.ind_cliente,'')  as C002_ind_cliente,
    IF(cmp.tip_documento<>'RUC',cmp.ext_ind_tiene_tarjeta_credito,'')  as C002_ind_tiene_tarjeta_credito,
    IF(cmp.tip_documento<>'RUC',cmp.ind_fallecido,'')  as C002_ind_fallecido,
    IF(cmp.tip_documento<>'RUC',cmp.ind_royal,'')  as C002_ind_royal,
    IF(cmp.tip_documento<>'RUC',cmp.ind_cliente_nuevo,'')  as C002_ind_cliente_nuevo,
    IF(cmp.tip_documento<>'RUC',cmp.ind_critico,'')  as C002_ind_critico,
    IF(cmp.tip_documento<>'RUC',cmp.ind_trabaja_empresa_seguros,'')  as C002_ind_trabaja_empresa_seguros,
    IF(cmp.tip_documento<>'RUC',cmp.ind_cliente_vip,'')  as C002_ind_cliente_vip,
    IF(cmp.tip_documento<>'RUC',cmp.ind_empleado_rimac,'')  as C002_ind_empleado_rimac,
    IF(cmp.tip_documento<>'RUC',cmp.ind_trabajador_empresa_riesgo_desuscrito,'NO APLICA')  as C002_ind_trabajador_empresa_riesgo_desuscrito,
    IF(cmp.tip_documento<>'RUC',cmp.ind_cliente_corredor_bloqueo_comercial,'NO APLICA')  as C002_ind_cliente_corredor_bloqueo_comercial
FROM cmp
LEFT JOIN direccion dir
    ON cmp.id_persona = dir.id_persona
WHERE cmp.tip_documento in ('DNI', 'PA', 'CE','RUC')
