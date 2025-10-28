/********************** CONTROL DE CAMBIOS *****************************
Organizacion: RIMAC
Programa: Portal Salud Siniestro
Creado por: Junior Noel Quintana Flores
Fecha de Creación: 13/03/2025
Propósito: Caso de uso que disponibiliza datos de la tabla analitica
siniestro_detalle_salud y carta_garantia_solicitud para el Portal Salud
Siniestro en la capa rapida a traves de la API en AWS.
Fuentes de datos: Capa analytics anl_siniestro.siniestro_detalle_salud
Destino: Capa rapida Bigtable uni__siniestro_detalle_salud

Historial de Modificaciones
Autor Fecha Detalle
=======================================================================

***********************************************************************/

WITH
procedimiento AS (
  SELECT DISTINCT
    CONCAT(SPLIT(ats.id_pre_liquidacion_siniestro, '-')[SAFE_ORDINAL(4)], '-', SPLIT(ats.id_pre_liquidacion_siniestro, '-')[SAFE_ORDINAL(5)]) AS num_siniestro,
    ROW_NUMBER() OVER(PARTITION BY ats.id_pre_liquidacion_siniestro ORDER BY prs.num_colegio_medico DESC) AS indicador,
    prs.num_procedimiento_atencion_origen,
    prs.num_procedimiento_origen,
    prs.des_procedimiento,
    prs.mnt_deducible_tec_sol,
    prs.mnt_coaseguro_tec_sol,
    prs.mnt_gasto_presentado_tec_sol,
    prs.mnt_beneficio_tec_sol,
    prs.num_colegio_medico,
    prs.nom_medico
  FROM `rs-nprd-dlk-dt-anlyt-meve-2034.anl_salud.siniestro_detalle_salud` AS salud
  CROSS JOIN UNNEST(atencion_salud) ats
  CROSS JOIN UNNEST(procedimiento_salud) prs
  WHERE periodo = DATE_TRUNC(CURRENT_DATE('America/Lima'), MONTH)
),
base_siniestro AS (
  SELECT
    salud.id_siniestro,
    SAFE_CAST(atencion.fec_inicio_tratamiento AS DATE) AS fec_inicio_tratamiento,
    SAFE_CAST(salud.fec_ult_liquidacion AS DATE) AS fec_ult_liquidacion,
    salud.cod_proceso_bpm,
    atencion.razon_social_proveedor,
    atencion.id_pre_liquidacion_siniestro,
    salud.num_obligacion,
    TRIM(salud.nom_completo_contratante) AS nom_completo_contratante,
    salud.num_documento_contratante AS num_documento_contratante,
    salud.tip_reclamo,
    salud.des_tipo_reclamo,
    salud.des_tipo_contrato,
    salud.num_poliza,
    salud.id_plan_origen,
    salud.cod_producto_origen,
    salud.des_producto_agrupado,
    salud.des_producto,
    salud.tip_procedencia,
    atencion.cod_sede_proveedor,
    salud.num_documento_proveedor_siniestro AS ruc_proveedor,
    TRIM(salud.nom_completo_proveedor_siniestro) AS nom_completo_proveedor_siniestro,
    TRIM(salud.nom_sede_proveedor_siniestro) AS nom_sede_proveedor_siniestro,
    salud.num_siniestro,
    salud.id_persona_afiliado,
    salud.id_titular,
    salud.tip_documento_afiliado,
    salud.num_documento_afiliado,
    TRIM(salud.nom_completo_titular) AS nom_completo_titular,
    TRIM(salud.nom_completo_afiliado) AS nom_completo_afiliado,
    salud.des_cod_parentesco,
    salud.des_sexo_afiliado,
    salud.edad_afiliado_ocurrencia,
    salud.fec_nacimiento_afiliado,
    atencion.num_diagnostico_origen,
    atencion.des_cobertura,
    atencion.agrupacion_cobertura,
    atencion.agrupacion_cobertura_negocio,
    atencion.agrupacion_cobertura_subnegocio,
    atencion.des_grupo_diagnostico,
    atencion.cod_grupo_diagnostico,
    atencion.des_agrupacion_diagnostico,
    atencion.des_diagnostico_salud,
    atencion.nro_dias_estancia,
    atencion.mnt_beneficio_sin_impuesto_aprobado_usd,
    atencion.mnt_beneficio_sin_impuesto_aprobado_sol,
    atencion.mnt_impuesto_aprobado_usd,
    atencion.mnt_impuesto_aprobado_sol,
    atencion.mnt_gasto_cubierto_tec_sol,
    salud.periodo
  FROM `rs-nprd-dlk-dt-anlyt-meve-2034.anl_salud.siniestro_detalle_salud` AS salud
  LEFT JOIN UNNEST(atencion_salud) AS atencion
  WHERE salud.periodo = DATE_TRUNC(CURRENT_DATE('America/Lima'), MONTH)
),
reporte_rar AS (
  SELECT
    bs.*,
    p.indicador,
    p.num_colegio_medico,
    p.nom_medico,
    p.num_procedimiento_atencion_origen,
    p.num_procedimiento_origen,
    p.des_procedimiento,
    p.mnt_deducible_tec_sol,
    p.mnt_coaseguro_tec_sol,
    p.mnt_gasto_presentado_tec_sol,
    p.mnt_beneficio_tec_sol,
  FROM base_siniestro bs
  LEFT JOIN procedimiento p ON bs.num_siniestro = p.num_siniestro
),
without_row_key AS (
  SELECT
    tip_documento_afiliado,
    num_documento_afiliado,
    id_persona_afiliado,
    EXTRACT(YEAR FROM fec_inicio_tratamiento) AS num_anhio_ocurrencia,
    num_siniestro,
    cod_proceso_bpm,
    num_diagnostico_origen,
    des_diagnostico_salud,
    des_procedimiento,
    des_producto,
    COUNT(DISTINCT num_siniestro) AS ctd_siniestro,
    mnt_deducible_tec_sol,
    mnt_coaseguro_tec_sol,
    mnt_beneficio_tec_sol,
    fec_inicio_tratamiento,
    fec_ult_liquidacion,
    num_colegio_medico,
    nom_medico,
    des_tipo_contrato,
    des_tipo_reclamo,
    des_cobertura,
    mnt_gasto_cubierto_tec_sol,
    razon_social_proveedor,
    num_poliza,
    periodo
  FROM reporte_rar
  WHERE 1 = 1
  AND fec_inicio_tratamiento IS NOT NULL
  AND (
      num_diagnostico_origen NOT IN ('Z768', 'Z518')
      -- AND des_procedimiento NOT IN ('COBERTURA CAPITACIÓN', 'DESCRIPCIÓN CÁPITA')
    )
    AND (
      num_diagnostico_origen NOT IN (
        'Z001', 'Z34', 'Z348', 'Z349', 'J00X', 'R509', 'R14X', 'R11X', 'J039',
        'J040', 'J041', 'J042', 'A09X', 'E86X', 'R05X', 'N390', 'Z012'
      )
      AND des_procedimiento NOT IN ('CONSULTA MEDICA', 'FARMACIA')
    )
    AND (
      num_diagnostico_origen NOT IN (
        'Z23', 'Z230', 'Z231', 'Z232', 'Z233', 'Z234', 'Z235', 'Z236', 'Z237', 'Z238',
        'Z24', 'Z240', 'Z241', 'Z242', 'Z243', 'Z244', 'Z245', 'Z246', 'Z25', 'Z250',
        'Z251', 'Z258', 'Z26', 'Z260', 'Z268', 'Z269', 'Z27', 'Z270', 'Z271', 'Z272',
        'Z273', 'Z274', 'Z278', 'Z279', 'Z28', 'Z280', 'Z281', 'Z282', 'Z288', 'Z289',
        'Z29', 'K020', 'K021', 'K022', 'K023', 'K024', 'K025', 'K028', 'K029', 'H612'
      )
    )
  GROUP BY tip_documento_afiliado, num_documento_afiliado, id_persona_afiliado, num_anhio_ocurrencia,
           num_siniestro, cod_proceso_bpm, num_diagnostico_origen, des_diagnostico_salud, des_procedimiento,
           des_producto, mnt_deducible_tec_sol, mnt_coaseguro_tec_sol, mnt_beneficio_tec_sol, fec_inicio_tratamiento, fec_ult_liquidacion, num_colegio_medico, nom_medico,
           des_tipo_contrato, des_tipo_reclamo, des_cobertura, mnt_gasto_cubierto_tec_sol, razon_social_proveedor,
           num_poliza, periodo
  ORDER BY num_documento_afiliado, num_siniestro, des_procedimiento, num_anhio_ocurrencia
)

SELECT DISTINCT
    CONCAT(
        IFNULL(CAST(tip_documento_afiliado AS STRING), 'NULL'), '#',
        IFNULL(CAST(num_documento_afiliado AS STRING), 'NULL'), '#',
        IFNULL(CAST(id_persona_afiliado AS STRING), 'NULL'), '#',
        IFNULL(CAST(num_anhio_ocurrencia AS STRING), 'NULL'), '#',
        IFNULL(CAST(num_siniestro AS STRING), 'NULL'), '#',
        IFNULL(CAST(cod_proceso_bpm AS STRING), 'NULL'), '#',
        IFNULL(CAST(num_diagnostico_origen AS STRING), 'NULL'), '#',
        IFNULL(CAST(des_diagnostico_salud AS STRING), 'NULL'), '#',
        IFNULL(CAST(des_procedimiento AS STRING), 'NULL'), '#',
        IFNULL(CAST(des_producto AS STRING), 'NULL'), '#',
        IFNULL(CAST(ctd_siniestro AS STRING), 'NULL'), '#',
        IFNULL(CAST(mnt_deducible_tec_sol AS STRING), 'NULL'), '#',
        IFNULL(CAST(mnt_coaseguro_tec_sol AS STRING), 'NULL'), '#',
        IFNULL(CAST(mnt_beneficio_tec_sol AS STRING), 'NULL'), '#',
        IFNULL(CAST(fec_inicio_tratamiento AS STRING), 'NULL'), '#',
        IFNULL(CAST(fec_ult_liquidacion AS STRING), 'NULL'), '#',
        IFNULL(CAST(num_colegio_medico AS STRING), 'NULL'), '#',
        IFNULL(CAST(nom_medico AS STRING), 'NULL'), '#',
        IFNULL(CAST(des_tipo_reclamo AS STRING), 'NULL'), '#',
        IFNULL(CAST(des_tipo_contrato AS STRING), 'NULL'), '#',
        IFNULL(CAST(des_cobertura AS STRING), 'NULL'), '#',
        IFNULL(CAST(mnt_gasto_cubierto_tec_sol AS STRING), 'NULL'), '#',
        IFNULL(CAST(razon_social_proveedor AS STRING), 'NULL'), '#',
        IFNULL(CAST(num_poliza AS STRING), 'NULL'), '#',
        IFNULL(CAST(periodo AS STRING), 'NULL')
    ) AS row_key,
    SAFE_CAST(tip_documento_afiliado AS STRING) AS tip_documento_afiliado,
    SAFE_CAST(num_documento_afiliado AS STRING) AS num_documento_afiliado,
    SAFE_CAST(id_persona_afiliado AS STRING) AS id_persona_afiliado,
    SAFE_CAST(num_anhio_ocurrencia AS INT64) AS num_anhio_ocurrencia,
    SAFE_CAST(num_siniestro AS STRING) AS num_siniestro,
    SAFE_CAST(cod_proceso_bpm AS STRING) AS cod_proceso_bpm,
    SAFE_CAST(num_diagnostico_origen AS STRING) AS num_diagnostico_origen,
    SAFE_CAST(des_diagnostico_salud AS STRING) AS des_diagnostico_salud,
    SAFE_CAST(des_procedimiento AS STRING) AS des_procedimiento,
    SAFE_CAST(des_producto AS STRING) AS des_producto,
    SAFE_CAST(ctd_siniestro AS INT64) AS ctd_siniestro,
    SAFE_CAST(mnt_deducible_tec_sol AS FLOAT64) AS mnt_deducible_tec_sol,
    SAFE_CAST(mnt_coaseguro_tec_sol AS FLOAT64) AS mnt_coaseguro_tec_sol,
    SAFE_CAST(mnt_beneficio_tec_sol AS FLOAT64) AS mnt_beneficio_tec_sol,
    SAFE_CAST(fec_inicio_tratamiento AS DATE) AS fec_inicio_tratamiento,
    SAFE_CAST(fec_ult_liquidacion AS DATE) AS fec_ult_liquidacion,
    SAFE_CAST(num_colegio_medico AS STRING) AS num_colegio_medico,
    SAFE_CAST(nom_medico AS STRING) AS nom_medico,
    SAFE_CAST(des_tipo_reclamo AS STRING) AS des_tipo_reclamo,
    SAFE_CAST(des_tipo_contrato AS STRING) AS des_tipo_contrato,
    SAFE_CAST(des_cobertura AS STRING) AS des_cobertura,
    SAFE_CAST(mnt_gasto_cubierto_tec_sol AS FLOAT64) AS mnt_gasto_cubierto_tec_sol,
    SAFE_CAST(razon_social_proveedor AS STRING) AS razon_social_proveedor,
    SAFE_CAST(num_poliza AS STRING) AS num_poliza,
    SAFE_CAST(periodo AS DATE) AS periodo
FROM without_row_key
ORDER BY
    num_documento_afiliado,
    num_siniestro,
    des_procedimiento,
    num_anhio_ocurrencia;
