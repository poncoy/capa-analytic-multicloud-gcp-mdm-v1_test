#importacion de librerias
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from package.transform import get_json_file,bq_read_by_table,bq_read_by_merge
import logging
from datetime import date
from dateutil.relativedelta import relativedelta


def excluye_change(dct):
  return {key:value for key,value in dct.items() if key not in ["change"]}

#funcion de creacion de pipeline
def create_pipeline():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--use_case',
        help='Caso de Uso ID',
        default='replica02')            
    app_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    #Diseño de pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        tablas = get_json_file("package/config/config_use_case.json")
        fday_month = date.today().replace(day=1)
        num_months = 3
        
        for params in tablas[app_args.use_case]:
            
            source_table = params["source_table"]
            target_table = params["target_table"]
            
            if params["exec_type"] =="full" :
                read_full_pipe=[]

                for ind in range(num_months):
                    partition = (fday_month-relativedelta(months=ind)).strftime('%Y%m%d')
                    source_table_part = "{par1}${par2}".format(par1=source_table,par2=partition)
                    read_full_pipe.append(bq_read_by_table(pipeline,source_table_part))
                
                tpl_read_full_pipe = tuple(read_full_pipe)
                flatten = (tpl_read_full_pipe 
                            | f"Union partition full tables: {target_table}" >> beam.Flatten())
                (flatten 
                            | f"Write BigQuery:  {target_table}" >>  beam.io.WriteToBigQuery(
                                target_table,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
                            )
                )

            else :

                origen_lis_fields= params["lis_fields"]
                origen_id_field=params["id_field"]
                target_table_merge="{tabla}_merge".format(tabla=target_table)
                lis_periodo = []

                for ind in range(num_months):
                    lis_periodo.append((fday_month-relativedelta(months=ind)).strftime('%Y-%m-%d'))
                
                str_lis_periodo = ",".join(f"'{str(x)}'" for x in lis_periodo)
                
                read_merge_pipe=bq_read_by_merge(pipeline,target_table_merge,source_table,origen_lis_fields,origen_id_field,target_table, str_lis_periodo)
                
                filter_si = (read_merge_pipe[0] | f"Filtro Nuevos Registros ({target_table}): Merge" >>beam.Filter(lambda data: data['change'] == 'si'))
                filter_no = (read_merge_pipe[0] | f"Filtro Registros Actuales ({target_table}): Replica" >>beam.Filter(lambda data: data['change'] == 'no'))
                wc_filter_si = (filter_si | f"Formato Columnas ({target_table}): Merge" >> beam.Map(excluye_change))
                wc_filter_no = (filter_no | f"Formato Columnas ({target_table}): Replica" >> beam.Map(excluye_change))
                flatten = ((wc_filter_no,wc_filter_si) | f"Union Tables({target_table}): Replica+Merge" >> beam.Flatten())
                (wc_filter_si
                    |f"Write BigQuery: {target_table}_merge" >>  beam.io.WriteToBigQuery(
                        target_table_merge,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
                )
                (flatten
                    |f"Write BigQuery: {target_table}" >>  beam.io.WriteToBigQuery(
                    target_table,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
                    )
                )
          

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    create_pipeline()
