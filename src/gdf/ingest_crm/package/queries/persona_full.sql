WITH cliente_pers_ult_periodo as (
    SELECT
    MAX(periodo) AS periodo
  FROM
    `{PROJECT_ID}.lan_mdm_ingesta.PER__cliente_persona`
),
cmp as (
    SELECT
        cp.cuc,
        cp.id_cliente_persona AS id_persona,
        cp.tip_documento,
        cp.num_documento,
        cp.nse_rimac,
        cp.nse_agrup,
        IF (cp.ext_ind_dependiente_laboral = 1, 'SI','NO') AS ext_ind_dependiente_laboral,
        cp.periodo,
        cp.ind_blacklist,
        cp.ind_ley_datos_personales,
        cp.ind_consentimiento_comercial,
        cp.ext_des_empresa_laboral,
        cp.ext_ruc_empresa_laboral,
        cp.des_segmentacion_growth
        FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__cliente_persona` cp
        WHERE cp.ind_royal != 'SI'
            AND cp.num_edad >= 18
            AND cp.periodo = (select periodo from cliente_pers_ult_periodo)
    UNION ALL
    SELECT
        pp.cuc,
        pp.id_prospecto_persona AS id_persona,
        pp.tip_documento,
        pp.num_documento,
        pp.nse_rimac,
        pp.nse_agrup,
        pp.ext_ind_dependiente_laboral,
        pp.periodo,
        pp.ind_blacklist,
        pp.ind_ley_datos_personales,
        pp.ind_consentimiento_comercial,
        pp.ext_des_empresa_laboral,
        pp.ext_ruc_empresa_laboral,
        pp.des_segmentacion_growth
        FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__prospecto_persona` pp
        WHERE
            pp.ind_royal != 'SI'
            AND pp.num_edad >= 18
            AND pp.periodo = (select periodo from cliente_pers_ult_periodo)
    ),
  scoring_ia AS (
    WITH
        scoring_ia_n2 AS (
            SELECT psi.id_persona,
                n2.nombre_tecnico,
                MAX(n2.fec_procesamiento) AS fec_proc,
                ARRAY_AGG(n2) origen
                FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__persona_scoring_ia` psi
                CROSS JOIN UNNEST(psi.probabilidad_nivel_2) n2
                WHERE psi.periodo = (select periodo from cliente_pers_ult_periodo)
                AND psi.fec_procesamiento IN (SELECT MAX(psi2.fec_procesamiento)
                                FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__persona_scoring_ia` psi2
                                WHERE psi2.id_persona=psi.id_persona AND psi2.periodo=psi.periodo)
                GROUP BY 1,2
            ),
        scoring_ia_n1 AS (
            SELECT psi.id_persona,
                n1.nombre_tecnico,
                MAX(n1.fec_procesamiento) AS fec_proc,
                ARRAY_AGG(n1) origen
                FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__persona_scoring_ia` psi
                CROSS JOIN UNNEST(psi.probabilidad_nivel_1) n1
                WHERE psi.periodo = (select periodo from cliente_pers_ult_periodo)
                    AND psi.fec_procesamiento IN (SELECT MAX(psi2.fec_procesamiento)
                                FROM `{PROJECT_ID}.lan_mdm_ingesta.PER__persona_scoring_ia` psi2
                                WHERE psi2.id_persona=psi.id_persona AND psi2.periodo=psi.periodo)
                GROUP BY 1,2
            ),
        scoring_ia_n1_ind AS (
            SELECT n1.id_persona, n1.nombre_tecnico, MIN(_n1.valor) AS valor
                FROM scoring_ia_n1 n1
                    CROSS JOIN UNNEST(n1.origen) _n1
                WHERE _n1.nombre_tecnico=n1.nombre_tecnico
                    AND _n1.fec_procesamiento=n1.fec_proc
                GROUP BY n1.id_persona,n1.nombre_tecnico
            ),
        scoring_ia_n2_ind AS (
            SELECT n2.id_persona,
                n2.nombre_tecnico,
                MIN(_n2.valor) AS valor
                FROM scoring_ia_n2 n2
                    CROSS JOIN UNNEST(n2.origen) _n2
                WHERE _n2.nombre_tecnico=n2.nombre_tecnico
                    AND _n2.fec_procesamiento=n2.fec_proc
                GROUP BY n2.id_persona,n2.nombre_tecnico
            ),
        scoring_ia_res1 AS (
            SELECT cmp4.id_persona id1,            
                cmp4.valor AS ind_persistenciavn_ami
            FROM (SELECT a.id_persona, a.valor
                    FROM scoring_ia_n1_ind a
                    WHERE a.nombre_tecnico="scr_persistenciavn_ami") cmp4
            ),
        scoring_ia_res21 AS (
            SELECT cmp2.id_persona id21,            
                cmp2.valor AS ind_persistenciavn_vehicular
            FROM (SELECT a.id_persona,a.valor FROM scoring_ia_n2_ind a WHERE a.nombre_tecnico="scr_persistenciavn_vehicular") cmp2
            ),
        scoring_ia_res22 AS (
            SELECT cmp1.id_persona id22,            
                cmp1.valor AS ind_propension_vehicular
            FROM (SELECT a.id_persona,a.valor FROM scoring_ia_n2_ind a WHERE a.nombre_tecnico="scr_propension_vehicular") cmp1
            ),
        scoring_ia_res23 AS (
            SELECT cmp3.id_persona id23,            
                cmp3.valor AS ind_score_venta_vehicular
            FROM (SELECT a.id_persona,a.valor FROM scoring_ia_n2_ind a WHERE a.nombre_tecnico="scr_score_venta_vehicular") cmp3
            ),        
        scoring_ia_res31 AS (
            SELECT cmp2.id_persona id31,            
                cmp2.valor AS ind_persistenciavn_vidaahorro_4_colores
            FROM (SELECT a.id_persona,a.valor FROM scoring_ia_n1_ind a WHERE a.nombre_tecnico="scr_persistenciavn_vidaahorro_4_colores") cmp2
            ),        
        scoring_ia_res32 AS (
            SELECT cmp3.id_persona id32,            
                cmp3.valor AS ind_per_persistenciavn_vidasepelio
            FROM (SELECT a.id_persona, a.valor FROM scoring_ia_n1_ind a WHERE a.nombre_tecnico="scr_per_persistenciavn_vidasepelio") cmp3
            ),        
        scoring_ia_res33 AS (
            SELECT cmp6.id_persona id33,            
                cmp6.valor AS ind_per_propension_flexividcon
            FROM (SELECT a.id_persona, a.valor FROM scoring_ia_n1_ind a WHERE a.nombre_tecnico="scr_per_propension_flexividcon") cmp6
            ),

        scoring_ia_res41 AS (
            SELECT cmp1.id_persona id41,            
                cmp1.valor AS ind_propension_ami
            FROM (SELECT a.id_persona,a.valor FROM scoring_ia_n1_ind a WHERE a.nombre_tecnico="scr_propension_ami") cmp1
            ),
        scoring_ia_res42 AS (
            SELECT cmp5.id_persona id42,            
                cmp5.valor AS ind_score_venta_ami
            FROM (SELECT a.id_persona, a.valor FROM scoring_ia_n1_ind a WHERE a.nombre_tecnico="scr_score_venta_ami") cmp5
            )
    SELECT
            psi.id_persona,
            r1.*,
            r21.*,r22.*,r23.*,
            r31.*,r32.*,r33.*,
            r41.*,r42.*
        FROM
            `{PROJECT_ID}.lan_mdm_ingesta.PER__persona_scoring_ia` psi
            LEFT JOIN scoring_ia_res1 r1 ON psi.id_persona=r1.id1
            LEFT JOIN scoring_ia_res21 r21 ON psi.id_persona=r21.id21
            LEFT JOIN scoring_ia_res22 r22 ON psi.id_persona=r22.id22
            LEFT JOIN scoring_ia_res23 r23 ON psi.id_persona=r23.id23
            LEFT JOIN scoring_ia_res31 r31 ON psi.id_persona=r31.id31
            LEFT JOIN scoring_ia_res32 r32 ON psi.id_persona=r32.id32
            LEFT JOIN scoring_ia_res33 r33 ON psi.id_persona=r33.id33
            LEFT JOIN scoring_ia_res41 r41 ON psi.id_persona=r41.id41
            LEFT JOIN scoring_ia_res42 r42 ON psi.id_persona=r42.id42            
        WHERE
            psi.periodo = (select periodo from cliente_pers_ult_periodo)
    )
SELECT
  CONCAT(cmp.tip_documento,'#',cmp.num_documento,"#",cmp.cuc,"#",cmp.id_persona) AS C000_row_key,
  cmp.id_persona AS C000_id_persona,
  cmp.tip_documento AS C000_tip_documento,
  cmp.num_documento AS C000_num_documento,
  cmp.periodo AS C000_periodo,
  cmp.cuc AS C000_cod_claveunicocliente,
  IF (cmp.tip_documento<>'RUC',cmp.nse_rimac,'') AS C003_cod_nivel_socio_economico,
  IF (cmp.tip_documento<>'RUC',cmp.nse_agrup,'') AS C003_cod_nivel_socio_economico_agrupado,
  IFNULL(cmp.ind_blacklist,' ') AS C003_ind_blacklist,
  IFNULL(cmp.ind_ley_datos_personales,' ') AS C003_ind_ley_datos_personales,
  IFNULL(cmp.ind_consentimiento_comercial,' ') AS C003_ind_consentimiento_comercial,
  IFNULL(cmp.ext_des_empresa_laboral,' ') AS C003_des_empresa_laboral,
  IFNULL(s.ind_propension_vehicular,' ') AS C003_val_propension_vehicular,
  IFNULL(s.ind_propension_ami,' ') AS C003_val_propension_ami,
  IFNULL(s.ind_persistenciavn_vidaahorro_4_colores,' ') AS C003_val_persistenciavn_vidaahorro_4_colores,
  IFNULL(s.ind_persistenciavn_vehicular,' ') C003_val_persistenciavn_vehicular,
  IFNULL(s.ind_persistenciavn_ami,' ') AS C003_val_persistenciavn_ami,
  IFNULL(s.ind_score_venta_ami,' ') AS C003_val_score_venta_ami,
  IFNULL(s.ind_score_venta_vehicular,' ') AS C003_val_score_venta_vehicular,
  IFNULL(s.ind_per_persistenciavn_vidasepelio,' ') AS C003_val_persistenciavn_vida_sepelio,
  IFNULL(cmp.ext_ruc_empresa_laboral,' ') AS C003_num_ruc_empresa_laboral,
  IFNULL(cmp.des_segmentacion_growth,' ') AS C003_des_segmentacion_growth,
  IFNULL(s.ind_per_propension_flexividcon,' ') AS C003_val_propension_flexividcon,
  IF (cmp.tip_documento<>'RUC',cmp.ext_ind_dependiente_laboral,'') AS C003_ind_dependiente_laboral
FROM
  cmp LEFT JOIN  scoring_ia s ON cmp.id_persona = s.id_persona
WHERE
  cmp.tip_documento IN ('DNI','PA','CE','RUC')
