/********************** CONTROL DE CAMBIOS *****************************
Organizacion: RIMAC
Programa: Portal Salud Siniestro
Creado por:
Fecha de Creación: 03/10/2023
Propósito: Caso de uso que disponibiliza datos de la tabla analitica
siniestro_detalle_salud para el Portal Salud Siniestro en la capa rapida
a traves de la API en AWS.
Fuentes de datos: Capa analytics anl_siniestro.siniestro_detalle_salud
Destino: Capa rapida Bigtable uni__siniestro_proveedor_salud 

Historial de Modificaciones
Autor Fecha Detalle
=======================================================================
13/12/2023 Jose Zamalloa Se incluye el campo cod_sede_proveedor_sinistro
a solicitud del usuario para que se realice el filtro en la consulta de
la API
19/09/2024 Jose Zamalloa Se agrega el campo fec_devolucion para filtro de 
devoluciones
***********************************************************************/
DECLARE ult_periodo DATE;   

SET ult_periodo = (SELECT MAX(periodo) FROM `rs-nprd-dlk-dt-anlyt-mlk-0c8a.anl_siniestro.siniestro_detalle_salud`);

with 
base AS(
  SELECT  
    FORMAT_DATE('%Y-%m-%d', fec_notificacion) as C006_fec_notificacion,
    IFNULL(CAST(DATE(MAX(fec_devolucion_siniestro)) AS STRING) ,"NULL") as C006_fec_devolucion,
    num_documento_proveedor_siniestro AS C006_num_documento_proveedor,
    id_estado_siniestro_origen AS C006_id_estado_siniestro_origen,
    des_estado_siniestro_origen AS C006_des_estado_siniestro_origen,
    des_grupo_siniestro as C006_des_grupo_siniestro,
    ind_tedef_salud as C006_ind_registrado_trama,
    sum(mto_auditado_sol) as C006_mnt_documento_registrado_sol,
    case when ind_trama_tedef = 'SI' then 'TEDEF' else 'Manual' end as C006_des_origen_factura,
    count(distinct id_siniestro) as C006_cnt_siniestro,
    case when id_estado_siniestro_origen = '5' then
      case when sum(peso_devolucion) >= 3 then 'AMBOS' 
           when sum(peso_devolucion) = 1 then 'ADMINISTRATIVO'
           when sum(peso_devolucion) = 2 then 'MEDICO'
    end
    else 'SIN DEVOLUCION' end as C006_des_grupo_motivo_devolucion,
    periodo AS C006_periodo,
    IFNULL(cod_sede_proveedor_siniestro,'0')  AS C006_cod_sede_proveedor_siniestro
from (
    select  
            sds.id_siniestro,
            sds.num_siniestro,
            sds.fec_notificacion,
            sds.id_estado_siniestro_origen,
            sds.des_estado_siniestro_origen,
            sds.num_documento_proveedor_siniestro,
            cod_sede_proveedor_siniestro,
            sds.num_documento_contratante,
            sds.ind_tedef_salud,
            sds.mto_auditado_sol,
            sds.ind_trama_tedef,
            CASE
              WHEN sds.id_estado_siniestro_origen in ('3','4') THEN 'EN PROCESO DE PAGO'
              WHEN sds.id_estado_siniestro_origen in ('2','1','9') THEN 'PROCESADO'
              WHEN sds.id_estado_siniestro_origen in ('0') THEN 'RECIBIDO' 
              WHEN sds.id_estado_siniestro_origen in ('8') THEN 'CANCELADO'  
              WHEN sds.id_estado_siniestro_origen in ('5') THEN 'DEVUELTO CON CARTA'    
            ELSE 'PENDIENTE'
          END AS des_grupo_siniestro,
          case when sds.ind_tedef_salud = 'N' then 'FORMULARIO' else 'TEDEF' end as tip_factura_recibida, -- Se cambia al campo correcto una vez llevado al modelo staging
          sds.des_producto,
          case when gd.cod_grupo_motivo_devolucion = 'B' or gd.cod_grupo_motivo_devolucion = 'C' then 2 -- MEDICO 
              else 1 end -- ADMINISTRATIVO 
              as peso_devolucion,
          gd.fec_devolucion_siniestro,
          periodo 
       from `rs-nprd-dlk-dt-anlyt-mlk-0c8a.anl_siniestro.siniestro_detalle_salud` sds
       left join unnest (atencion_salud) ats
       left join unnest (grupo_devolucion) gd
    where
    1=1 
    and sds.periodo = ult_periodo
    and FORMAT_DATE('%Y%m', sds.fec_hora_ocurrencia) >= '202201'
    and sds.tip_reclamo = 'C'
    --and des_estado_siniestro_origen = 'RECHAZADO'
    --and id_persona_proveedor is null
)
group by 1,3,4,5,6,7,9,12,13)
SELECT CONCAT(b.C006_fec_notificacion,'#',b.C006_fec_devolucion,'#',b.C006_num_documento_proveedor,'#',b.C006_id_estado_siniestro_origen,'#',b.C006_ind_registrado_trama,'#',b.C006_des_origen_factura,'#', b.C006_des_grupo_motivo_devolucion,'#',b.C006_cod_sede_proveedor_siniestro) as C006_row_key, b.* FROM base b;
