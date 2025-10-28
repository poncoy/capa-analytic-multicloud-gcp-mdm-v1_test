# Importacion de librerias
import apache_beam as beam
import json
import logging
from apache_beam.io.gcp.internal.clients.bigquery import DatasetReference
# Classes

# Funcion de creacion de nuevas row a formato Capa Rapida(Segun el servicio), en este caso
#la data es transformada registro por registro a un formato de tipo Bigtable con l estructura especificada para el feature store.
class CreateRowFn(beam.DoFn):

    def __init__(self, gbt_json, bt_prefix, bt_table, bt_column_family):
        
        self.gbt_json = gbt_json
        self.bt_prefix = bt_prefix
        self.bt_table = bt_table
        self.bt_column_family = bt_column_family
    
    def process(self, element):
        
        bt_prefix = self.bt_prefix
        bt_table = self.bt_table
        bt_column_family = self.bt_column_family
        gbt_json = self.gbt_json

        json_schema = get_schema_bigtable(gbt_json,bt_prefix,bt_table)

        json_str = json.dumps(json_schema)
        logging.info("json_schema: " + json_str)
        #La llave unica que indexara el servicio de Capa Rapida
        key_column = json_schema["familyId"]

        from google.cloud.bigtable import row
        direct_row = row.DirectRow(row_key=str(element[key_column]).encode('iso-8859-1',errors='ignore'))
        
        for family in json_schema["column_family"]:
            if family["name"] in bt_column_family:
                for col in family["columns"]:

                    family_id = family["name"]
                    column_id = col["qualifierString"]
                    direct_row.set_cell(
                        family_id,
                        column_id,
                        str(element[column_id]).encode('iso-8859-1',errors='ignore')
                        )

        yield direct_row
        

# clase para agregar propiedades nuevos a la configuracion de bigtable
def get_json_file(path:str)->dict:

    with open(path, "r") as read_file:
        data = json.load(read_file)
    
    return data

def bq_read_by_table(pipeline,table_id):

    print("Table ID: ", table_id)
    rows = pipeline | f"read {table_id}" >> beam.io.Read(beam.io.ReadFromBigQuery(table = table_id, use_standard_sql=True))

    return rows

def bq_read_by_query(pipeline,use_case,project_id):   

    sql_query = read_sql_file(use_case,project_id)

    print("Query: ", sql_query)
    rows = pipeline | f"read {use_case}" >> beam.io.Read(beam.io.ReadFromBigQuery(query = sql_query, use_standard_sql=True,temp_dataset= DatasetReference(projectId=f"{project_id}",datasetId="pre_persona") ))
    return rows

def read_sql_file(use_case:str,project_id:str):

    try:
        fr = open('package/queries/'+use_case+'.sql','r')
        sqlFile = fr.read().format(PROJECT_ID=project_id)
        fr.close()

        return sqlFile

    except FileNotFoundError:
        print("No se encuentra el archivo: package/queries/"+use_case+'.sql')

def get_schema_bigtable(gbt_json,bt_prefix,bt_table)->dict:

    json_schema = {}

    for i in gbt_json[bt_prefix]:
        if i["table_name"] == bt_table:
            json_schema = i

    return json_schema

