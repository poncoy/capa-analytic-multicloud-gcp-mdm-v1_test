with cliente_pers_ult_periodo as (
    select max(periodo) as periodo
    from 
    `{PROJECT_ID}.lan_mdm_ingesta.PER__cliente_persona`    
),
direccion as (
    SELECT id_persona
        , ddc.datos_direccion.des_departamento
        , ddc.datos_direccion.des_provincia
        , ddc.datos_direccion.des_distrito
    FROM `{PROJECT_ID}.lan_mdm_ingesta.CON__datos_contacto` dc
    CROSS JOIN dc.detalle_datos_contacto ddc
    WHERE ddc.des_tipo = 'COMERCIAL' and dc.periodo in (select periodo from cliente_pers_ult_periodo)
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
        cp.num_edad,
        'SI' as ind_cliente,
        cp.periodo,
        cp.des_segmentacion_growth
    FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__cliente_persona` cp
    WHERE
        cp.ind_royal != 'SI'
        and cp.num_edad >= 18
        and cp.periodo in (select periodo from cliente_pers_ult_periodo)
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
        pp.num_edad,
        'NO' as ind_cliente,
        pp.periodo,
        pp.des_segmentacion_growth
    FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__prospecto_persona` pp
    WHERE
        pp.ind_royal != 'SI'
        and pp.num_edad >= 18
        and pp.periodo in (select periodo from cliente_pers_ult_periodo)
)
SELECT
    CONCAT(cmp.tip_documento,'#',cmp.num_documento,"#",cmp.cuc,"#",cmp.id_persona) as C000_row_key,    
    cmp.id_persona as C000_id_persona,
    cmp.tip_documento as C000_tip_documento,
    cmp.num_documento as C000_num_documento,
    cmp.periodo as C000_periodo,
    cmp.cuc as C000_cod_claveunicocliente,    
    cmp.nombres as C004_nom_persona,
    cmp.ape_paterno as C004_ape_paternopersona,
    cmp.ape_materno as C004_ape_maternopersona,
    cmp.fec_nacimiento as C004_fec_nacimiento,
    dir.des_departamento as C004_nom_departamento,
    dir.des_provincia as C004_nom_provincia,
    dir.des_distrito as C004_nom_distrito,
    cmp.num_edad as C004_num_edad,
    cmp.ind_cliente C004_ind_cliente,
    IFNULL(cmp.des_segmentacion_growth,' ') AS C004_des_segmentacion_growth,
FROM cmp
LEFT JOIN direccion dir
    ON cmp.id_persona = dir.id_persona
WHERE cmp.tip_documento in ('DNI', 'PA', 'CE','RUC')