/********************** CONTROL DE CAMBIOS *****************************
Organizacion: RIMAC
Programa: Portal Salud Siniestro
Creado por: Junior Noel Quintana Flores
Fecha de Creación: 19/11/2024
Propósito: Caso de uso que disponibiliza datos de la tabla analitica
siniestro_detalle_salud y carta_garantia_solicitud para el Portal Salud
Siniestro en la capa rapida a traves de la API en AWS.
Fuentes de datos: Capa analytics anl_siniestro.siniestro_detalle_salud
Destino: Capa rapida Bigtable uni__siniestro_grupo_diagnostico_salud

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
  WHERE periodo = DATE(CONCAT(SUBSTRING(CAST(CURRENT_TIMESTAMP() AS STRING), 1, 7), '-01'))
),

base_siniestro AS (
  SELECT
    salud.id_siniestro,
    FORMAT_DATE('%Y-%m-%d', atencion.fec_inicio_tratamiento) AS fec_inicio_tratamiento,
    FORMAT_DATE('%Y-%m-%d', salud.fec_ult_liquidacion) AS fec_ult_liquidacion,
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
  WHERE salud.periodo = DATE(CONCAT(SUBSTRING(CAST(CURRENT_TIMESTAMP() AS STRING), 1, 7), '-01'))
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
    SUBSTRING(num_diagnostico_origen, 3) AS cod_diagnostico_origen,
    des_grupo_diagnostico,
    mnt_gasto_cubierto_tec_sol,
    mnt_gasto_cubierto_tec_sol * COUNT(DISTINCT num_siniestro) AS mnt_gasto_cubierto_total_sol,
    COUNT(DISTINCT num_siniestro) AS num_siniestro,
    tip_procedencia,
    nom_completo_afiliado,
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
  GROUP BY
    tip_documento_afiliado,
    num_documento_afiliado,
    id_persona_afiliado,
    cod_diagnostico_origen,
    des_grupo_diagnostico,
    mnt_gasto_cubierto_tec_sol,
    tip_procedencia,
    nom_completo_afiliado,
    periodo
),

without_row_key_v2 AS (
  SELECT
    tip_documento_afiliado,
    num_documento_afiliado,
    id_persona_afiliado,
    cod_diagnostico_origen,
    des_grupo_diagnostico,
    ROUND( SUM(mnt_gasto_cubierto_total_sol), 2 ) AS mnt_gasto_cubierto_sol,
    SUM(num_siniestro) AS ctd_siniestro,
    tip_procedencia,
    nom_completo_afiliado,
    periodo
  FROM without_row_key
  GROUP BY
    tip_documento_afiliado,
    num_documento_afiliado,
    id_persona_afiliado,
    cod_diagnostico_origen,
    des_grupo_diagnostico,
    tip_procedencia,
    nom_completo_afiliado,
    periodo
  ORDER BY
    num_documento_afiliado,
    des_grupo_diagnostico
)

SELECT DISTINCT
    CONCAT(
        IFNULL(CAST(tip_documento_afiliado AS STRING), 'NULL'), '#',
        IFNULL(CAST(num_documento_afiliado AS STRING), 'NULL'), '#',
        IFNULL(CAST(id_persona_afiliado AS STRING), 'NULL'), '#',
        IFNULL(CAST(cod_diagnostico_origen AS STRING), 'NULL'), '#',
        IFNULL(CAST(des_grupo_diagnostico AS STRING), 'NULL'), '#',
        IFNULL(CAST(mnt_gasto_cubierto_sol AS STRING), 'NULL'), '#',
        IFNULL(CAST(ctd_siniestro AS STRING), 'NULL'), '#',
        IFNULL(CAST(tip_procedencia AS STRING), 'NULL'), '#',
        IFNULL(CAST(nom_completo_afiliado AS STRING), 'NULL'), '#',
        IFNULL(CAST(periodo AS STRING), 'NULL')
    ) AS row_key,
    tip_documento_afiliado,
    num_documento_afiliado,
    id_persona_afiliado,
    cod_diagnostico_origen,
    des_grupo_diagnostico,
    mnt_gasto_cubierto_sol,
    ctd_siniestro,
    tip_procedencia,
    nom_completo_afiliado,
    periodo
FROM without_row_key_v2
ORDER BY
    num_documento_afiliado,
    des_grupo_diagnostico
