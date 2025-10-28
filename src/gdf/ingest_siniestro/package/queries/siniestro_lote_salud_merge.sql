/********************** CONTROL DE CAMBIOS *****************************
Organizacion: RIMAC
Programa: Portal Salud Siniestro
Creado por:
Fecha de Creación: 03/10/2023
Propósito: Caso de uso que disponibiliza datos de la tabla analitica
siniestro_lote para el Portal Salud Siniestro en la capa rapida
a traves de la API en AWS.
Fuentes de datos: Capa analytics anl_siniestro.siniestro_detalle_salud
Destino: Capa rapida Bigtable uni__siniestro_lote_salud  

Historial de Modificaciones
Autor Fecha Detalle
=======================================================================
Jose Zamalloa 13/12/2023 Se incluye el campo cod_sede_proveedor_sinistro
a solicitud del usuario para que se realice el filtro en la consulta de
la API
Jose Zamalloa 20/03/2024 Se modifica el valor en caso el campo 
cod_estado_siniestro_lote venga nulo para que sea reconocido 
correctamente por la API
***********************************************************************/

WITH  siniestro_lote_ult_periodo as (
    select max(periodo) as periodo
    from 
    `rs-nprd-dlk-dt-anlyt-meve-2034.anl_salud.siniestro_lote`   
    WHERE
    periodo > DATE_ADD(CURRENT_DATE(), INTERVAL -2 MONTH)
),
base AS ( SELECT
  fec_envio_siniestro_lote AS C007_fec_envio_siniestro_lote
  ,num_documento_proveedor_lote AS C007_num_documento_proveedor_lote
  ,IFNULL(cod_estado_siniestro_lote,'999') AS C007_cod_estado_siniestro_lote
  ,(CASE 
    WHEN cod_estado_siniestro_lote = '0' THEN 'ACEPTADA'
    WHEN cod_estado_siniestro_lote = '2' THEN 'RECHAZADA'
    WHEN cod_estado_siniestro_lote = '4' THEN 'NOTIFICADA'
    ELSE 'VALIDADA'
  END) 
  AS C007_des_cod_estado_comprobante_pago_lote
  ,periodo AS C007_periodo
  ,IFNULL(cod_sede_proveedor,'0') AS C007_cod_sede_proveedor_siniestro
FROM
  `rs-nprd-dlk-dt-anlyt-meve-2034.anl_salud.siniestro_lote`
WHERE
  periodo > DATE_ADD(CURRENT_DATE(), INTERVAL -2 MONTH) 
)
SELECT
CONCAT(b.C007_fec_envio_siniestro_lote,'#',b.C007_num_documento_proveedor_lote,'#',b.C007_cod_estado_siniestro_lote,'#',b.C007_des_cod_estado_comprobante_pago_lote,'#',b.C007_cod_sede_proveedor_siniestro) as C007_row_key
,b.C007_fec_envio_siniestro_lote
,b.C007_num_documento_proveedor_lote
,b.C007_cod_estado_siniestro_lote
,b.C007_des_cod_estado_comprobante_pago_lote
,count(1) AS C007_cnt_documento_siniestro_lote
,b.C007_periodo
,b.C007_cod_sede_proveedor_siniestro FROM base b
GROUP BY 1,2,3,4,5,7,8