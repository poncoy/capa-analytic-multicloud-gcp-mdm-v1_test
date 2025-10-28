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
        cp.num_edad,
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
        cp.ind_critico
    FROM (
        SELECT a_cpm.* 
            FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__cliente_persona_merge` a_cpm
        UNION ALL
        SELECT b_cpm.* 
            FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__cliente_persona` b_cpm 
            WHERE b_cpm.id_cliente_persona in (SELECT dcm.id_persona FROM `{PROJECT_ID}.lan_mdm_ingesta.CON__datos_contacto_merge` dcm )
        )  cp
    WHERE
        cp.num_edad >= 18
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
        pp.ind_critico
    FROM (
        SELECT a_ppm.* 
            FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__prospecto_persona_merge` a_ppm
        UNION ALL
        SELECT b_ppm.* 
            FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__prospecto_persona` b_ppm 
            WHERE b_ppm.id_prospecto_persona in (SELECT dcm.id_persona FROM `{PROJECT_ID}.lan_mdm_ingesta.CON__datos_contacto_merge` dcm)
        ) pp
    WHERE
        pp.num_edad >= 18
        and pp.periodo in (select periodo from cliente_pers_ult_periodo)
),
scoring_ia as (
    WITH scoring_ia_n2 as (
     SELECT psi.id_persona,
         n2.nombre_tecnico,
         MAX(n2.fec_procesamiento) as fec_proc,
     	ARRAY_AGG(n2) origen
     FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__persona_scoring_ia` psi
         CROSS JOIN UNNEST(psi.probabilidad_nivel_2) n2
         WHERE psi.periodo in (select periodo from cliente_pers_ult_periodo)
             AND psi.fec_procesamiento IN (SELECT MAX(psi2.fec_procesamiento) 
                         FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__persona_scoring_ia` psi2 
                         WHERE psi2.id_persona=psi.id_persona AND psi2.periodo=psi.periodo)
     GROUP BY 1,2
     ), scoring_ia_n1 as (
     SELECT psi.id_persona,
         n1.nombre_tecnico,
         MAX(n1.fec_procesamiento) as fec_proc,
     	ARRAY_AGG(n1) origen
     FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__persona_scoring_ia` psi
         CROSS JOIN UNNEST(psi.probabilidad_nivel_1) n1
         WHERE psi.periodo in (select periodo from cliente_pers_ult_periodo)
             AND psi.fec_procesamiento IN (SELECT MAX(psi2.fec_procesamiento) 
                         FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__persona_scoring_ia` psi2 
                         WHERE psi2.id_persona=psi.id_persona AND psi2.periodo=psi.periodo)
     GROUP BY 1,2
     ), scoring_ia_n1_ind AS (
     SELECT 
     n1.id_persona,
     n1.nombre_tecnico ,
     MIN(_n1.valor) as valor
     FROM scoring_ia_n1 n1
     	CROSS JOIN UNNEST(n1.origen) _n1
     WHERE _n1.nombre_tecnico=n1.nombre_tecnico
         AND _n1.fec_procesamiento=n1.fec_proc
	 GROUP BY n1.id_persona,n1.nombre_tecnico
     ), scoring_ia_n2_ind AS (
     SELECT 
     n2.id_persona,
     n2.nombre_tecnico ,
     MIN(_n2.valor) as valor
     FROM scoring_ia_n2 n2
     	CROSS JOIN UNNEST(n2.origen) _n2
     WHERE _n2.nombre_tecnico=n2.nombre_tecnico
         AND _n2.fec_procesamiento=n2.fec_proc
	 GROUP BY n2.id_persona,n2.nombre_tecnico
     ), scoring_ia_res1 as ( 
     SELECT cmp1.id_persona id1,
     		cmp1.valor as ind_propension_ami,
     		cmp2.valor as ind_persistenciavn_vidaahorro_4_colores,
     		cmp3.valor as ind_per_persistenciavn_vidasepelio,
     		cmp4.valor as ind_persistenciavn_ami,
     		cmp5.valor as ind_score_venta_ami
     FROM (SELECT a.id_persona,a.valor FROM scoring_ia_n1_ind a WHERE a.nombre_tecnico="scr_propension_ami") cmp1,
     	(SELECT a.id_persona,a.valor FROM scoring_ia_n1_ind a WHERE a.nombre_tecnico="scr_persistenciavn_vidaahorro_4_colores") cmp2,
     	(SELECT a.id_persona,a.valor FROM scoring_ia_n1_ind a WHERE a.nombre_tecnico="scr_per_persistenciavn_vidasepelio") cmp3,
     	(SELECT a.id_persona,a.valor FROM scoring_ia_n1_ind a WHERE a.nombre_tecnico="scr_persistenciavn_ami") cmp4,
     	(SELECT a.id_persona,a.valor FROM scoring_ia_n1_ind a WHERE a.nombre_tecnico="scr_score_venta_ami") cmp5	
     WHERE cmp1.id_persona=cmp2.id_persona
     	AND cmp1.id_persona=cmp3.id_persona
     	AND cmp1.id_persona=cmp4.id_persona
     	AND cmp1.id_persona=cmp5.id_persona
     ), scoring_ia_res2 as ( 
     SELECT cmp1.id_persona id2,
     		cmp1.valor AS ind_propension_vehicular, 
     		cmp2.valor AS ind_persistenciavn_vehicular,
     		cmp3.valor AS ind_score_venta_vehicular
     FROM (SELECT a.id_persona,a.valor FROM scoring_ia_n2_ind a WHERE a.nombre_tecnico="scr_propension_vehicular") cmp1,
      (SELECT a.id_persona,a.valor FROM scoring_ia_n2_ind a WHERE a.nombre_tecnico="scr_persistenciavn_vehicular") cmp2,
      (SELECT a.id_persona,a.valor FROM scoring_ia_n2_ind a WHERE a.nombre_tecnico="scr_score_venta_vehicular") cmp3
     WHERE cmp1.id_persona=cmp2.id_persona
     	AND cmp1.id_persona=cmp3.id_persona
     ) SELECT psi.id_persona, r1.*,r2.*
     	FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__persona_scoring_ia` psi
     		LEFT JOIN scoring_ia_res1 r1 ON psi.id_persona=r1.id1
     		LEFT JOIN scoring_ia_res2 r2 ON psi.id_persona=r2.id2
     WHERE psi.periodo in (select periodo from cliente_pers_ult_periodo)
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
    IF(cmp.tip_documento<>'RUC',cmp.ext_ind_dependiente_laboral,'')  as C002_ind_dependiente_laboral,
    IF(cmp.tip_documento<>'RUC',cmp.ind_cliente,'')  as C002_ind_cliente,
    IF(cmp.tip_documento<>'RUC',cmp.ext_ind_tiene_tarjeta_credito,'')  as C002_ind_tiene_tarjeta_credito,
    IF(cmp.tip_documento<>'RUC',cmp.ind_fallecido,'')  as C002_ind_fallecido,
    IF(cmp.tip_documento<>'RUC',cmp.ind_royal,'')  as C002_ind_royal,
    IF(cmp.tip_documento<>'RUC',cmp.ind_cliente_nuevo,'')  as C002_ind_cliente_nuevo,
    IF(cmp.tip_documento<>'RUC',cmp.ind_critico,'')  as C002_ind_critico,
    IFNULL(cmp.ind_blacklist,' ') as C003_ind_blacklist,
    IFNULL(cmp.ind_ley_datos_personales,' ') as C003_ind_ley_datos_personales,
    IFNULL(cmp.ind_consentimiento_comercial,' ') as C003_ind_consentimiento_comercial,
    IFNULL(cmp.ext_des_empresa_laboral,' ') as C003_des_empresa_laboral,
    IFNULL(s.ind_propension_vehicular,' ') AS C003_val_propension_vehicular,
    IFNULL(s.ind_propension_ami,' ') AS C003_val_propension_ami,
    IFNULL(s.ind_persistenciavn_vidaahorro_4_colores,' ') AS C003_val_persistenciavn_vidaahorro_4_colores,
    IFNULL(s.ind_persistenciavn_vehicular,' ') C003_val_persistenciavn_vehicular,
    IFNULL(s.ind_persistenciavn_ami,' ') AS C003_val_persistenciavn_ami,
    IFNULL(s.ind_score_venta_ami,' ') AS C003_val_score_venta_ami,
    IFNULL(s.ind_score_venta_vehicular,' ') AS C003_val_score_venta_vehicular,
    IFNULL(s.ind_per_persistenciavn_vidasepelio,' ') as C003_val_persistenciavn_vida_sepelio,
    IFNULL(cmp.ext_ruc_empresa_laboral,' ') AS C003_num_ruc_empresa_laboral,
    IFNULL(cmp.des_segmentacion_growth,' ') AS C003_des_segmentacion_growth
FROM cmp
LEFT JOIN scoring_ia s 
  ON cmp.id_persona = s.id_persona
LEFT JOIN direccion dir
    ON cmp.id_persona = dir.id_persona
WHERE cmp.tip_documento in ('DNI', 'PA', 'CE','RUC')
