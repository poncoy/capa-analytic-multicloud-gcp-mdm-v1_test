/********************** CONTROL DE CAMBIOS *****************************
Organizacion: RIMAC
Programa: Portal Gestion de Tareas
Creado por:
Fecha de Creación: 28/05/2024
Propósito: Se requiere disponibilizar datos que serán consumidos mediante
las APIs de Capa Rápida para el módulo de Gestión de Tareas que tendrá
datos de fuentes de Rimac Salud y Camunda.
Fuentes de datos: Capa analytics anl_siniestro.siniestro_gestion_tareas_salud
Destino: Capa rapida Bigtable uni__siniestro_gestion_tareas  

Historial de Modificaciones
Autor Fecha Detalle
=======================================================================

***********************************************************************/

WITH  siniestro_gestion_ult_periodo as (
    select max(periodo) as periodo
    from 
    `rs-nprd-dlk-dt-anlyt-meve-2034.anl_salud.siniestro_gestion_tarea_salud`  
    WHERE
    periodo > DATE_ADD(CURRENT_DATE(), INTERVAL -4 MONTH)
),
base AS (
SELECT 
  IFNULL(DATE(sgts.fec_inicio_actividad),CURRENT_DATE()) AS fec_inicio_actividad
  ,IFNULL(DATE(sgts.fec_fin_actividad),CURRENT_DATE()) AS fec_fin_actividad
  ,sgts.des_actividad_siniestro AS des_actividad_siniestro
  ,IFNULL(sgts.des_tipo_proceso_auditoria,"NULL") AS des_tipo_proceso_auditoria
  ,IF(sgts.val_proceso_auditoria_administrativa = "","ND", IFNULL(sgts.val_proceso_auditoria_administrativa,"NULL")) AS val_proceso_auditoria_admin
  ,SPLIT(sgts.id_persona_proveedor_siniestro,'-')[offset(1)] AS cod_proveedor
  ,SPLIT(sgts.id_sede_proveedor_siniestro,'-')[offset(2)] AS cod_sede_proveedor
  ,IFNULL(sgts.des_correo_corporativo,"NULL") AS des_correo_corporativo
  ,sgts.periodo
  ,CASE 
    WHEN  sgts.des_proceso_actividad = 'continuarProcess' THEN "Completadas"
    WHEN  sgts.des_proceso_actividad = 'rechazoProcess' THEN "Rechazadas"
    WHEN  sgts.des_proceso_actividad = 'anulacionProcess' THEN "Anuladas"  
    WHEN  sgts.des_proceso_actividad = 'forzadoProcess' THEN "Forzadas"  
    ELSE "ND"
  END AS des_detalle_tarea
  ,SUM(sgts.mnt_auditado_sol) AS mnt_detalle_tarea
  ,COUNT(sgts.des_proceso_actividad) AS cnt_detalle_tarea 
  ,sgts.des_asignacion_tarea
  ,COUNT(sgts.des_asignacion_tarea) AS cnt_asignacion_tarea
  ,sgts.des_vencimiento_tarea
  ,COUNT(sgts.des_vencimiento_tarea) AS cnt_vencimiento_tarea
  ,sgts.nom_completo_proveedor_siniestro AS nom_completo_proveedor_siniestro
  ,COUNT(sgts.num_documento_proveedor_siniestro) AS cnt_tareas_proveedor
  ,IFNULL(sgts.des_tipo_liquidacion,"NULL") AS des_tipo_liquidacion
  ,COUNT(sgts.des_tipo_liquidacion) AS cnt_liquidacion
  ,IFNULL(sgts.des_tipo_devolucion,"NULL") AS des_tipo_devolucion
  ,COUNT(sgts.des_tipo_devolucion) AS cnt_devolucion
FROM `rs-nprd-dlk-dt-anlyt-meve-2034.anl_salud.siniestro_gestion_tarea_salud` sgts
WHERE 
  sgts.periodo in (select periodo from siniestro_gestion_ult_periodo)
  AND SPLIT(sgts.id_siniestro,'-')[offset(1)] = '1' 
  AND SPLIT(sgts.id_siniestro,'-')[offset(2)] = '4' 
  AND sgts.tip_reclamo = 'C' 
  AND sgts.id_estado_siniestro_origen NOT IN ('5','A')
GROUP BY 
  fec_inicio_actividad
  ,fec_fin_actividad
  ,des_actividad_siniestro
  ,des_tipo_proceso_auditoria
  ,val_proceso_auditoria_administrativa
  ,id_persona_proveedor_siniestro
  ,id_sede_proveedor_siniestro
  ,des_correo_corporativo
  ,periodo
  ,des_proceso_actividad
  ,des_asignacion_tarea
  ,des_vencimiento_tarea
  ,nom_completo_proveedor_siniestro
  ,des_tipo_liquidacion
  ,des_tipo_devolucion
)
SELECT 
CONCAT(
  b.fec_inicio_actividad,'#',b.fec_fin_actividad,'#',b.des_actividad_siniestro,'#',b.des_tipo_proceso_auditoria,'#',b.val_proceso_auditoria_admin,'#',b.cod_proveedor,'#',b.cod_sede_proveedor,'#',b.des_correo_corporativo,'#',b.des_detalle_tarea,'#',b.des_asignacion_tarea,'#',b.des_vencimiento_tarea,'#',b.des_tipo_liquidacion,'#',b.des_tipo_devolucion) as C009_row_key
  ,b.fec_inicio_actividad AS C009_fec_inicio_actividad
  ,b.fec_fin_actividad AS C009_fec_fin_actividad
  ,b.des_actividad_siniestro AS C009_des_actividad_siniestro
  ,b.des_tipo_proceso_auditoria AS C009_des_tipo_proceso_auditoria
  ,b.val_proceso_auditoria_admin AS C009_val_proceso_auditoria_admin
  ,b.cod_proveedor AS C009_cod_proveedor
  ,b.cod_sede_proveedor AS C009_cod_sede_proveedor
  ,b.des_correo_corporativo AS C009_des_correo_corporativo
  ,b.periodo AS C009_periodo
  ,b.des_detalle_tarea AS C009_des_detalle_tarea
  ,b.mnt_detalle_tarea AS C009_mnt_detalle_tarea
  ,b.cnt_detalle_tarea AS C009_cnt_detalle_tarea
  ,b.des_asignacion_tarea AS C009_des_asignacion_tarea
  ,b.cnt_asignacion_tarea AS C009_cnt_asignacion_tarea
  ,b.des_vencimiento_tarea AS C009_des_vencimiento_tarea
  ,b.cnt_vencimiento_tarea AS C009_cnt_vencimiento_tarea
  ,b.nom_completo_proveedor_siniestro AS C009_nom_completo_proveedor_siniestro
  ,b.cnt_tareas_proveedor AS C009_cnt_tareas_proveedor
  ,b.des_tipo_liquidacion AS C009_des_tipo_liquidacion
  ,b.cnt_liquidacion AS C009_cnt_liquidacion
  ,b.des_tipo_devolucion AS C009_des_tipo_devolucion
  ,b.cnt_devolucion AS C009_cnt_devolucion
FROM base b