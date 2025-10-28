/********************** CONTROL DE CAMBIOS *****************************
Organizacion: RIMAC
Programa: Portal Salud Siniestro
Creado por: Junior Noel Quintana Flores
Fecha de Creación: 12/02/2025
Propósito: Caso de uso que disponibiliza datos de la tabla analitica
preexistencia para el Portal Salud en la capa rapida a traves del API en AWS.
Fuentes de datos: Capa analytics anl_persona.preexistencia
Destino: Capa rapida Bigtable uni__preexistencia

Historial de Modificaciones
Autor Fecha Detalle
=======================================================================

***********************************************************************/

SELECT DISTINCT
    CONCAT(
        IFNULL(CAST(p.tip_documento AS STRING), 'NULL'), '#',
        IFNULL(CAST(p.num_documento AS STRING), 'NULL'), '#',
        IFNULL(CAST(p.id_persona AS STRING), 'NULL'), '#',
        IFNULL(CAST(pc.cod_preexistencia_origen AS STRING), 'NULL'), '#',
        IFNULL(CAST(p.des_preexistencia AS STRING), 'NULL'), '#',
        IFNULL(CAST(pc.fec_inicio_vigencia_preexistencia AS STRING), 'NULL'), '#',
        IFNULL(CAST(pc.num_tramite_preexistencia_inclusion AS STRING), 'NULL'), '#',
        IFNULL(CAST(pc.nom_producto AS STRING), 'NULL'), '#',
        IFNULL(CAST(p.est_preexistencia_origen AS STRING), 'NULL'), '#',
        IFNULL(CAST(pc.num_tramite_preexistencia_exclusion AS STRING), 'NULL'), '#',
        IFNULL(CAST(pc.fec_fin_vigencia_preexistencia AS STRING), 'NULL'), '#',
        IFNULL(CAST(pc.ind_fraude AS STRING), 'NULL'), '#',
        IFNULL(CAST(p.periodo AS STRING), 'NULL')
    ) AS row_key,
    p.tip_documento,
    p.num_documento,
    p.id_persona,
    pc.cod_preexistencia_origen,
    p.des_preexistencia,
    pc.fec_inicio_vigencia_preexistencia,
    pc.num_tramite_preexistencia_inclusion,
    pc.nom_producto,
    p.est_preexistencia_origen,
    pc.num_tramite_preexistencia_exclusion,
    pc.fec_fin_vigencia_preexistencia,
    pc.ind_fraude,
    p.periodo
FROM
    `rs-nprd-dlk-dt-anlyt-meve-2034.anl_salud.preexistencia` p,
    UNNEST(p.arr_producto_contratante) pc
WHERE
    p.periodo = DATE_TRUNC(CURRENT_DATE('America/Lima'), MONTH)
ORDER BY
    pc.fec_inicio_vigencia_preexistencia;
