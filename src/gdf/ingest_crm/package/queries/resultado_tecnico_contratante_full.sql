SELECT
CONCAT(id_persona,'#',des_riesgo,'#',val_anho) as C008_row_key
,id_persona as C008_id_persona
,des_riesgo as C008_des_riesgo
,val_anho as C008_val_anho
,mnt_prima_total_usd as C008_mnt_prima_total_usd
,mnt_prima_retenida_usd as C008_mnt_prima_retenida_usd
,mnt_siniestros_total_usd as C008_mnt_siniestros_total_usd
,mnt_siniestros_retenidos_usd as C008_mnt_siniestros_retenidos_usd
,mnt_resultado_tecnico_total_usd as C008_mnt_resultado_tecnico_total_usd
,fec_ultima_procesamiento as C008_fec_ultima_procesamiento
,por_resultado_tecnico as C008_por_resultado_tecnico
,por_siniestralidad as C008_por_siniestralidad
,por_siniestralidad_retenida as C008_por_siniestralidad_retenida
,fec_procesamiento as C008_fec_procesamiento
,date_trunc(current_date(), MONTH) AS C008_periodo 
FROM `rs-nprd-dlk-dt-anlyt-mlk-0c8a.delivery_salesforce.resultado_tecnico_contratante`