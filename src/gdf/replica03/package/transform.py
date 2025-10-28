# Importacion de librerias
import apache_beam as beam
import json

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
        
        #La llave unica que indexara el servicio de Capa Rapida
        key_column = json_schema["familyId"]

        from google.cloud.bigtable import row
        direct_row = row.DirectRow(row_key=element[key_column])
        
        for family in json_schema["column_family"]:
            if family["name"] in bt_column_family:
                for col in family["columns"]:

                    family_id = family["name"]
                    column_id = col["qualifierString"]
                    direct_row.set_cell(
                        family_id,
                        column_id,
                        str(element[column_id]).encode('iso-8859-1') 
                        )

        yield direct_row
        

# Utils Functions: Se reutiliza para las implementaciones de casos de uso
def get_json_file(table_schema_path:str)->dict:

    with open(table_schema_path, "r") as read_file:
        data = json.load(read_file)
    
    return data

def bq_read_by_table(pipeline,table_id):

    print("Table ID: ", table_id)
    rows = pipeline | f"read {table_id}" >> beam.io.Read(beam.io.ReadFromBigQuery(table = table_id, use_standard_sql=True))

    return rows

def bq_read_by_query(pipeline,use_case,project_id):   

    sql_query = read_sql_file(use_case,project_id)

    print("Query: ", sql_query)
    rows = pipeline | f"read {use_case}" >> beam.io.Read(beam.io.ReadFromBigQuery(query = sql_query, use_standard_sql=True))
    return rows

def bq_read_by_merge(pipeline,use_case,source_table,lis_fields,id_field,target_table, lis_periodo):   

    project_source_id=source_table.split(':')[0]
    dataset_source=source_table.split(':')[1].split('.')[0]
    table_source_name=source_table.split(':')[1].split('.')[1]
    project_id=target_table.split(':')[0]
    table_name=target_table.split(':')[1].split('.')[1]
    susti_origen=" AS STRING),''),IfNULL(CAST(origen."
    susti_data=" AS STRING),''),IfNULL(CAST(data."

    lis_fields_format='IfNULL(CAST(data.' + susti_data.join(x for x in lis_fields.split(',')) +" AS STRING),'')"
    lis_fields_origen_format='IfNULL(CAST(origen.' + susti_origen.join(x for x in lis_fields.split(',')) +" AS STRING),'')"    

    id_field_format='IfNULL(CAST(data.' + susti_data.join(x for x in id_field.split(',')) +" AS STRING),'')"
    id_field_origen_format='IfNULL(CAST(origen.' + susti_origen.join(x for x in id_field.split(',')) +" AS STRING),'')"
#    source_table_dec = "{par}$20220601".format(par=source_table)

    fr = open('package/queries/TEMPLATE__merge.sql','r')
    sql_query = fr.read().format(PROJECT_SOURCE_ID=project_source_id,
                                DATASET_NAME=dataset_source,
                                TABLE_SOURCE_NAME=table_source_name,
                                LIST_FIELDS=lis_fields_format,
                                ID_FIELD=id_field_format,
                                LIST_FIELDS_ORIGEN=lis_fields_origen_format,
                                ID_FIELD_ORIGEN=id_field_origen_format,
                                PROJECT_ID=project_id,TABLE_NAME=table_name,
                                LIST_PERIODOS=lis_periodo
                                )
    fr.close()    
    print("Query: ", sql_query)
    rows={}
    rows[0] = pipeline | f"read {use_case}" >> beam.io.Read(beam.io.ReadFromBigQuery(query = sql_query, use_standard_sql=True))
#    rows[1] = bq_read_by_table(pipeline,target_table)
#    rows[2] = bq_read_by_table(pipeline,source_table_dec)
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
