#Ejemplo del JSON
#{
#    "cu": "test",
#    "key": {
#        "tip_documento": "DNI",
#        "num_documento": "26626524"
#    },
#    "table": "universal__bloqueo_compra_previa",
#    "columns": [
#        "id_persona_bloqueo",
#        "tip_documento",
#        "num_documento",
#        "cod_producto_ax",
#        "des_agrupacion_n1",
#        "des_agrupacion_n2",
#        "des_agrupacion_n3"
#    ]
#}

import functions_framework
from lib.utils.rapi_utils import dummy_test, find_c001, find_c002, find_c003, find_c006, find_c006_devolucion, find_c007, find_c006_c007, find_c008, find_c009, find_c010, find_c011, find_c012, find_c013, find_c014, find_c015, find_c016

@functions_framework.http
def handler(request):
    resultado = {
        'code': 200,
        'status': 'success',
        'data': {},
        'error': {}
    }
    
    request_json = request.get_json(silent=True)
    request_args = request.args
    if 'support' in request_json :                
        support = request_json["support"]
        print("captura support")
        print(support)
    else:
        support= False


    if 'cu' in request_json :                
        cu = request_json["cu"]
    else:
        resultado['code'] = 505
        resultado['status'] = 'failed'
        resultado['error'] = {'msg': 'Se requiere el valor del caso de uso (cu)'}
        return resultado, 505

    if 'key' in request_json and 'columns' in request_json:
        key = request_json["key"]
        strcolumns = request_json["columns"]         
    else:
        resultado['code'] = 500
        resultado['status'] = 'failed'
        resultado['error'] = {'msg': 'el valor de key y columns son requeridos'}
        return resultado, 500
    try:
        table_id = request_json["table"]
        print(key)
        print(strcolumns)
        resultado={}
        if cu=="test" :            
            dummy_test(table_id,key,strcolumns,resultado,support)
        elif cu=="C001_block_leads":
            resultado={}
            resultado=find_c001(table_id,key,strcolumns,support)
        elif cu=="C002_hyper_leads":
            resultado={}
            resultado=find_c002(table_id,key,strcolumns,support)
        elif cu=="C003_hyper_crm":
            resultado={}
            resultado=find_c002(table_id,key,strcolumns,support)
        elif cu=="C004_cliente_eb":
            print("001")
            resultado={}
            print("002")
            resultado=find_c002(table_id,key,strcolumns,support)
            print("003")
        elif cu=="C005_productos_eb":
            resultado={}
            resultado=find_c003(table_id,key,strcolumns,support)
        elif cu=="C006_siniestros_ps":
            resultado={}
            resultado=find_c006(table_id,key,strcolumns,support)
        elif cu=="C006_siniestros_ps_devolucion":
            resultado={}
            resultado=find_c006_devolucion(table_id,key,strcolumns,support)
        elif cu=="C007_lote_factura_ps":
            resultado={}
            resultado=find_c007(table_id,key,strcolumns,support)
        elif cu=="C006_C007_monto_total_ps":
            resultado={}
            resultado=find_c006_c007(table_id,key,strcolumns,support)
        elif cu=="C008_rt_contratante":
            resultado={}
            resultado=find_c008(table_id,key,strcolumns,support)
        elif cu=="C009_gestion_tareas":
            resultado={}
            resultado=find_c009(table_id,key,strcolumns,support)
        elif cu=="C010_leads_eps":
            resultado={}
            resultado=find_c010(table_id,key,strcolumns,support)
        elif cu=="siniestro_diagnostico_salud":
            resultado={}
            resultado=find_c011(table_id,key,strcolumns,support)
        elif cu=="siniestro_grupo_diagnostico_salud":
            resultado={}
            resultado=find_c012(table_id,key,strcolumns,support)
        elif cu=="siniestro_procedimiento_salud":
            resultado={}
            resultado=find_c013(table_id,key,strcolumns,support)
        elif cu=="siniestro_carta_garantia_salud":
            resultado={}
            resultado=find_c014(table_id,key,strcolumns,support)
        elif cu=="siniestro_detalle_salud":
            resultado={}
            resultado=find_c015(table_id,key,strcolumns,support)
        elif cu=="preexistencia":
            resultado={}
            resultado=find_c016(table_id,key,strcolumns,support)
        else :
            resultado['code'] = 504
            resultado['status'] = 'failed'
            resultado['error'] = {'msg': 'Caso de uso no reconocido, verifique si el valor de cu es correcto'}
            return resultado, 504
    except Exception as ex:
        resultado['code'] = 404
        resultado['status'] = 'failed'
        resultado['error'] = {
            'msg': 'ocurrio un error al obtener los datos',
            'detalle':ex
            }
        return resultado, 404   
    

    
    return resultado
