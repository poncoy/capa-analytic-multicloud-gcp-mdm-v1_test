/*
PROJECT_SOURCE_ID
DATASET_NAME
TABLE_SOURCE_NAME

LIST_FIELDS
LIST_FIELDS_ORIGEN

ID_FIELD_ORIGEN
ID_FIELD

PROJECT_ID
TABLE_NAME
*/

WITH  
  GroupedStagingTransactions AS (  
    SELECT CONCAT({ID_FIELD},{LIST_FIELDS}) registro, CONCAT({ID_FIELD}) identificador, 
      ARRAY_AGG(data) AS trans  
    FROM `{PROJECT_SOURCE_ID}.{DATASET_NAME}.{TABLE_SOURCE_NAME}` data 
    WHERE data.periodo in ({LIST_PERIODOS}) 
    GROUP BY 1 ,2 
  ),GroupedTransactions AS (  
    SELECT  
      d.registro, d.identificador, ARRAY(SELECT AS STRUCT * FROM d.trans) AS trans 
    FROM  
      GroupedStagingTransactions d  
  ),  
  JoinedTransactions AS (  
    SELECT staging.registro,staging.identificador, 
      staging.trans,  
      IF(ori.llave IS NULL, 'si','no') AS change  
    FROM  
      GroupedTransactions AS staging  
      LEFT JOIN (SELECT CONCAT({ID_FIELD_ORIGEN},{LIST_FIELDS_ORIGEN})  llave
                  FROM `{PROJECT_ID}.lan_mdm_ingesta.{TABLE_NAME}` AS origen
                  GROUP BY 1
                  ) ori
      ON staging.registro =  ori.llave
  )select x.*, t.change 
  from JoinedTransactions t, unnest(t.trans) as x ;