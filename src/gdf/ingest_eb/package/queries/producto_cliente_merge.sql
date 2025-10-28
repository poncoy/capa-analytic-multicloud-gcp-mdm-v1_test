WITH cliente_pers_ult_periodo AS (
  SELECT
    MAX(periodo) AS periodo
  FROM
    `{PROJECT_ID}.lan_mdm_ingesta.PER__cliente_persona`
)
SELECT
  CONCAT(
    cp.tip_documento,
    '-',
    cp.num_documento,
    '#',
    cpd.cod_producto_origen,
    '-',
    cpd.pol_num_poliza,
    '#',
    cpd.cer_id_certificado
  ) C005_row_key,
  cp.id_cliente_persona AS C005_id_producto_cliente,
  cp.tip_documento AS C005_tip_documento,
  cp.num_documento AS C005_num_documento,
  cpd.pol_id_poliza AS C005_id_poliza,
  cpd.cuc_contratante AS C005_cod_clave_unico_cliente_contratante,
  cpd.pol_des_contratante AS C005_nom_contratante,
  cpd.des_agrupacion_n1 AS C005_des_agrupacion_n1,
  cpd.des_agrupacion_n2 AS C005_des_agrupacion_n2,
  cpd.des_agrupacion_n3 AS C005_des_agrupacion_n3,
  cpd.id_producto AS C005_id_producto,
  cpd.cod_producto_origen AS C005_cod_producto_origen,
  cpd.des_producto AS C005_des_producto,
  cpd.pol_id_estado_poliza_origen AS C005_id_estado_poliza_origen,
  cpd.pol_num_poliza AS C005_pol_num_poliza,
  cpd.pol_fec_inicio_vigencia AS C005_fec_inicio_vigencia_poliza,
  ifnull(cpd.pol_fec_fin_vigencia, cpd.pol_fec_anulacion) as C005_fec_fin_vigencia_poliza,
  cpd.pol_fec_anulacion as C005_fec_anulacion_poliza,
  cpd.cer_id_certificado AS C005_cer_id_certificado,
  cpd.cer_id_estado_origen as C005_id_estado_origen_certificado,
  cpd.cer_fec_inicio_vigencia AS C005_fec_inicio_vigencia_certificado,
  ifnull(cpd.cer_fec_fin_vigencia, cpd.cer_fec_anulacion) AS C005_fec_fin_vigencia_certificado,
  cpd.cer_fec_anulacion AS C005_fec_anulacion_certificado,
  cp.periodo AS C005_periodo
FROM
  `{PROJECT_ID}.lan_mdm_ingesta.PER__cliente_persona_merge` cp
  INNER JOIN `{PROJECT_ID}.lan_mdm_ingesta.PER__cliente_persona_detalle_merge` cpd ON cp.id_cliente_persona = cpd.id_cliente_persona
  AND cp.periodo = cpd.periodo
WHERE
  cp.periodo IN (
    SELECT
      periodo
    FROM
      cliente_pers_ult_periodo
  )