#importacion de librerias
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from package.transform import get_json_file,bq_read_by_query,bq_read_by_table,CreateRowFn
import logging


#funcion de creacion de pipeline
def create_pipeline():
    #options = class_add_bigtable_args(flags=[], **options_pipe)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--use_case',
        help='Caso de Uso ID',
        default='C001')
    
    app_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    #Diseño de pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:

        config_use_case = get_json_file("package/config/config_use_case.json")
        
        for params in config_use_case[app_args.use_case]:
            
            bq_table_entire = "{project}:{dataset}.{table}".format(project=params["project_id"],dataset=params["bq_dataset"],table=params["bq_table"])
            bq_table = params["bq_table"]
            bq_table_type = "{table}_{type}".format(table=bq_table,type=params["exec_type"])
            bt_table = "{prefix}__{table}".format(prefix=params["bt_prefix"],table=params["bt_table"])

            #data_json = get_json_file("package/queries/"+bq_table+".json")      schema=data_json["schema"],
            gbt_json = get_json_file("package/config/config_gbt.json")

            primer_pipe = bq_read_by_query(pipeline,bq_table_type,params["project_id"])
            segundo_pipe = (primer_pipe | f"Write BigQuery:  {bq_table}" 
                >>  beam.io.WriteToBigQuery(bq_table_entire,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))
            tercer_pipe = (primer_pipe | f"Bigtable Row: {bt_table}" 
                >> beam.ParDo(CreateRowFn(gbt_json,params["bt_prefix"],params["bt_table"],params["bt_column_family"])) 
                | f"Write Bigtable:  {bt_table}" >>  WriteToBigTable(
                    project_id=params["project_id"],
                    instance_id=params["bt_instance"],
                    table_id=bt_table)
            )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    create_pipeline()
