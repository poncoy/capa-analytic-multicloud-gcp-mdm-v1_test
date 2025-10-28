/********************** CONTROL DE CAMBIOS *****************************
Organizacion: RIMAC
Programa: Delivery SMT
Creado por:
Fecha de Creación: 21/08/2024
Propósito: Caso de uso que disponibiliza datos de la tabla analitica
cliente_empresa y proespecto_empresa para el Caso de Uso Leads EPS en 
la capa rapida a traves de la API en AWS.
Fuentes de datos: Capa analytics anl_empresa.cliente_empresa, 
anl_empresa.prospecto_empresa
Destino: Capa rapida Bigtable uni__empresa

Historial de Modificaciones
Autor Fecha Detalle
=======================================================================
Jose Zamalloa 21/08/2024 Se disponibiliza el caso de uso Leads EPS
***********************************************************************/

WITH cliente_emp_ult_periodo AS (
    SELECT max(periodo) AS periodo
    FROM 
    `rs-nprd-dlk-dt-anlyt-mlk-0c8a.anl_empresa.cliente_empresa`    
),
base AS (
    SELECT
        ce.id_cliente_empresa AS id_empresa,        
        ce.tip_documento,
        ce.num_documento,
        ce.cuc AS cod_claveunicocliente,
        ce.razon_social AS des_razon_social,
        ce.nombre_comercial AS nom_comercial,
        CAST(ce.ext_fec_constitucion AS DATE) AS fec_constitucion,
        ce.departamento_gestion_servicio AS nom_departamento,
        ce.provincia_gestion_servicio AS nom_provincia,
        ce.distrito_gestion_servicio AS nom_distrito,
        ce.segmento_cliente AS nom_segmento_empresa,
        CAST(ce.periodo AS DATE) AS periodo
    FROM `rs-nprd-dlk-dt-anlyt-mlk-0c8a.anl_empresa.cliente_empresa`  ce
    WHERE
        ce.periodo in (select periodo from cliente_emp_ult_periodo)
    UNION ALL
    SELECT
        pe.id_prospecto_empresa AS id_empresa,
        pe.tip_documento,
        pe.num_documento,
        pe.cuc AS cod_claveunicocliente,
        pe.des_razon_social,
        pe.nom_comercial,
        CAST(pe.fec_constitucion AS DATE) AS fec_constitucion,
        pe.des_departamento_gestion_servicio AS nom_departamento,
        pe.des_provincia_gestion_servicio AS nom_provincia,
        pe.des_distrito_gestion_servicio AS nom_distrito,
        pe.des_segmento AS nom_segmento_empresa,
        CAST(pe.periodo AS DATE) AS periodo
    FROM `rs-nprd-dlk-dt-anlyt-mlk-0c8a.anl_empresa.prospecto_empresa` pe
    WHERE
        pe.periodo in (select periodo from cliente_emp_ult_periodo)
)
SELECT
    CONCAT(base.tip_documento,'#',REGEXP_REPLACE(base.num_documento, r'[^\w\.@-]' , ''),"#",REGEXP_REPLACE(base.cod_claveunicocliente, r'[^\w\.@-]' , ''),"#",base.id_empresa) as C010_row_key,    
    base.id_empresa as C010_id_empresa,
    base.tip_documento as C010_tip_documento,
    REGEXP_REPLACE(base.num_documento, r'[^\w\.@-]' , '') as C010_num_documento,
    REGEXP_REPLACE(base.cod_claveunicocliente, r'[^\w\.@-]' , '') as C010_cod_claveunicocliente,
    base.des_razon_social as C010_des_razon_social,
    base.nom_comercial as C010_nom_comercial,
    base.fec_constitucion as C010_fec_constitucion,
    base.nom_departamento as C010_nom_departamento,
    base.nom_provincia as C010_nom_provincia,
    base.nom_distrito as C010_nom_distrito,
    base.nom_segmento_empresa as C010_nom_segmento_empresa,
    base.periodo as C010_periodo
FROM base