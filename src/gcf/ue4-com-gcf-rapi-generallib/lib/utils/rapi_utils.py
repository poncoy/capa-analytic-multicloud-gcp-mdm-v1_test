from queue import Empty
from google.cloud import bigtable
import os
import google.cloud.bigtable.row_filters as row_filters
from google.cloud.bigtable.row_set import RowSet
import json
import datetime
import pandas as pd
import calendar
import json

CONSTANT_REQUEST_OK = {
        'code': 200,
        'status': 'success',
        'data': {},
        'error': {}
    }

def get_env(variable_env:str):
    #print(os.environ)
    if variable_env in os.environ:
       return os.environ[variable_env]

def row_to_dict(row,columnfilter,boo_suport):    
    row_dict ={}
    col_empty=True
    #print("INICIO ROW TO DICT")
    if boo_suport :
        row_dict["key"]=row.row_key.decode('latin-1')
    #print("VARIABLE SUPPORT")
    for cf, cols in sorted(row.cells.items()):
        for col, cells in sorted(cols.items()):
            #print("ANTES DEL CONDICIONAL")
            if col.decode('latin-1') in columnfilter:                
                for cell in cells:
                    #print("RECORRIDO DE CELDAS")
                    col_empty=False
                    if cell.value.decode('latin-1') is None:
                        row_dict[col.decode('latin-1')]=''
                    else:
                        row_dict[col.decode('latin-1')] = cell.value.decode('latin-1')                    
                    #rint("SE CAPTURO VALOR DE CELDA")
                    if boo_suport :
                        time_cell=col.decode('latin-1')+'_timestamp'
                        row_dict[time_cell] = cell.timestamp_micros    
    if col_empty :
        row_dict ={'error':'No existe un campo válido'}
    return row_dict

def row_to_dict2(row_key,table_id,col_family,lis_column,boo_suport,str_decode):    
    resultado_dict={}
    resultado_dict=CONSTANT_REQUEST_OK    
    table2=get_gbt_table(table_id)    
    if table2 is Empty:
        resultado_dict['data']={}
        msje_err=f'la tabla {table_id} no existe'
        msg_err(resultado_dict,500,msje_err)        
        return resultado_dict

    row = table2.read_row(row_key.encode('utf-8'))
    if boo_suport :
        resultado_dict['data']['key']=row.row_key.decode(str_decode)    
    for x in list(lis_column):
        column_id = x.encode('utf-8')
        print(row.cells[col_family])
        if column_id in list(row.cells[col_family]):
            value = row.cells[col_family][column_id][0].value.decode('utf-8',errors='ignore')
            resultado_dict['data'][x]=value            
            if boo_suport :
                resultado_dict['data']['timestamp'] = row.cells[col_family][column_id][0].timestamp_micros
        else :            
            msje_err='campos invalidos'
            msg_err(resultado_dict,500,msje_err)        
            return resultado_dict                            
    print("vemos el contenido del dict")
    resultado_dict['error'] = {}
    resultado_dict['status'] = 'success'
    resultado_dict['code'] = 200 
    print(resultado_dict)   
    return resultado_dict

def get_gbt_table(table_name:str):
    client = bigtable.Client(project=get_env('PROJECT_GBT'), admin=True)
    instance = client.instance(get_env('GBT_INSTANCE'))   
    table_result = instance.table(table_name)
    if not table_result.exists():
        return Empty
    return table_result

def msg_err(result,num_err:int,msg:str):
    result['data']={}
    result['code'] = 500
    result['status'] = 'failed'
    result['error'] = {'msg': msg}
    return result

def find_c001(table_find, key_find ,strcolumns_find,boo_suport): 

    hash_key = '#'.join([key_find[x] for x in key_find])
    field_key = '#'.join([x for x in key_find])   
    obj_request={}
    obj_request=CONSTANT_REQUEST_OK
    if field_key not in "tip_documento#num_documento#cod_prod":
        msg_err(obj_request,404,'los campos de búsqueda no son correctos ')
        return obj_request
    
    table=get_gbt_table(table_find)
    prefix = hash_key
    end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
    rows = table.read_rows(row_set=row_set,filter_=row_filters.CellsColumnLimitFilter(1))

    i=0
    obj_request['data'] = {}
    for row in rows:
        i+=1
        obj_request['data'][i]=row_to_dict(row,strcolumns_find,boo_suport)

    obj_request['error'] = {}
    obj_request['status'] = 'success'
    obj_request['code'] = 200 
    
    return obj_request

def find_c002(table_find, key_find ,strcolumns_find,boo_suport): 

    hash_key = '#'.join([key_find[x] for x in key_find])    
    field_key = '#'.join([x for x in key_find])
    obj_request2={}
    obj_request2=CONSTANT_REQUEST_OK
    if field_key not in "tip_documento#num_documento":
        msg_err(obj_request2,404,'los campos de búsqueda no son correctos ')
        return obj_request2
    print("lib01")
    table=get_gbt_table(table_find)
    prefix = hash_key
    end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
    rows = table.read_rows(row_set=row_set,filter_=row_filters.CellsColumnLimitFilter(1))
    print("lib02")
    i=0
    obj_request2['data'] = {}
    for row in rows:
        i+=1
        obj_request2['data'][i]=row_to_dict(row,strcolumns_find,boo_suport)

    obj_request2['error'] = {}
    obj_request2['status'] = 'success'
    obj_request2['code'] = 200 
    print("lib03")
    return obj_request2

def find_c003(table_find, key_find ,strcolumns_find,boo_suport): 

    hash_key = '#'.join([key_find[x] for x in key_find])
    field_key = '#'.join([x for x in key_find])   
    obj_request={}
    obj_request=CONSTANT_REQUEST_OK
    if field_key not in "tipo-numdoc#codprod-numpol":
        msg_err(obj_request,404,'los campos de búsqueda no son correctos ')
        return obj_request
    
    table=get_gbt_table(table_find)
    prefix = hash_key
    end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
    rows = table.read_rows(row_set=row_set,filter_=row_filters.CellsColumnLimitFilter(1))

    i=0
    obj_request['data'] = {}
    for row in rows:
        i+=1
        obj_request['data'][i]=row_to_dict(row,strcolumns_find,boo_suport)

    obj_request['error'] = {}
    obj_request['status'] = 'success'
    obj_request['code'] = 200 
    
    return obj_request

def find_c006(table_find, key_find ,strcolumns_find,boo_suport): 

    #Modificacion de consulta por rango de fechas:

    key_find_2 = {}
    key_master = {"num_documento_proveedor": "","id_estado_siniestro_origen": "","ind_registrado_trama": "", "des_origen_factura":"","des_grupo_motivo_devolucion":"","cod_sede_proveedor_siniestro":""}
    for key,value in key_find.items():
        if key not in ("fec_notificacion_inicio","fec_notificacion_fin","num_documento_proveedor","id_estado_siniestro_origen","ind_registrado_trama","des_origen_factura","des_grupo_motivo_devolucion","cod_sede_proveedor_siniestro"):
            msg_err(obj_request,404,'los campos de búsqueda no son correctos ')
            return obj_request
        if key not in ("fec_notificacion_inicio","fec_notificacion_fin"):
            key_find_2[key] = value

    regex = ""

    for key in key_master:
        if key_find_2.get(key,"null") != "null":
            regex = regex + "#" + str(key_find_2[key])        
        else:
            regex = regex + "#[^#]+"
    
    obj_request={}
    obj_request=CONSTANT_REQUEST_OK

    try:
        start = datetime.datetime.strptime(key_find['fec_notificacion_inicio'], "%Y-%m-%d")
        end = datetime.datetime.strptime(key_find['fec_notificacion_fin'], "%Y-%m-%d")
        if start > end:
            msg_err(obj_request,404,'la fecha final no puede ser menor que la fecha inicial')
            return obj_request

        date_generated = pd.date_range(start, end)
        fechas = date_generated.strftime("%Y-%m-%d")

        obj_request['data'] = []
        for fecha in fechas:
            regex_filter = fecha + "#[^#]+" + regex            
            table=get_gbt_table(table_find)            
            rows = table.read_rows(
                filter_=row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(regex_filter.encode("utf-8")),
                    row_filters.CellsColumnLimitFilter(1),
                ]
                )
            )            
            for row in rows:
                obj_request['data'].append(row_to_dict(row,strcolumns_find,boo_suport))           

        obj_request['error'] = {}
        obj_request['status'] = 'success'
        obj_request['code'] = 200 
            
        return obj_request           
            
    except ValueError:
        msg_err(obj_request,404,'La fecha ingresada es invalida.')
        return obj_request    
    
def find_c006_devolucion(table_find, key_find ,strcolumns_find,boo_suport): 

    #Modificacion de consulta por rango de fechas:

    key_find_2 = {}
    key_master = {"num_documento_proveedor": "","id_estado_siniestro_origen": "","ind_registrado_trama": "", "des_origen_factura":"","des_grupo_motivo_devolucion":"","cod_sede_proveedor_siniestro":""}
    for key,value in key_find.items():
        if key not in ("fec_devolucion_inicio","fec_devolucion_fin","num_documento_proveedor","id_estado_siniestro_origen","ind_registrado_trama","des_origen_factura","des_grupo_motivo_devolucion","cod_sede_proveedor_siniestro"):
            msg_err(obj_request,404,'los campos de búsqueda no son correctos ')
            return obj_request
        if key not in ("fec_devolucion_inicio","fec_devolucion_fin"):
            key_find_2[key] = value

    regex = ""

    for key in key_master:
        if key_find_2.get(key,"null") != "null":
            regex = regex + "#" + str(key_find_2[key])        
        else:
            regex = regex + "#[^#]+"
    
    obj_request={}
    obj_request=CONSTANT_REQUEST_OK

    try:
        start = datetime.datetime.strptime(key_find['fec_devolucion_inicio'], "%Y-%m-%d")
        end = datetime.datetime.strptime(key_find['fec_devolucion_fin'], "%Y-%m-%d")
        if start > end:
            msg_err(obj_request,404,'la fecha final no puede ser menor que la fecha inicial')
            return obj_request

        date_generated = pd.date_range(start, end)
        fechas = date_generated.strftime("%Y-%m-%d")

        obj_request['data'] = []
        for fecha in fechas:
            regex_filter = "[^#]+#" + fecha + regex  
            print(regex_filter)          
            table=get_gbt_table(table_find)            
            rows = table.read_rows(
                filter_=row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(regex_filter.encode("utf-8")),
                    row_filters.CellsColumnLimitFilter(1),
                ]
                )
            )            
            for row in rows:
                obj_request['data'].append(row_to_dict(row,strcolumns_find,boo_suport))           

        obj_request['error'] = {}
        obj_request['status'] = 'success'
        obj_request['code'] = 200 
            
        return obj_request           
            
    except ValueError:
        msg_err(obj_request,404,'La fecha ingresada es invalida.')
        return obj_request  

def find_c007(table_find, key_find ,strcolumns_find,boo_suport): 
    
    #Modificacion de consulta por rango de fechas:

    key_find_2 = {}
    key_master = {"num_documento_proveedor_lote": "","cod_estado_siniestro_lote": "","des_cod_estado_comprobante_pago_lote": "", "cod_sede_proveedor_siniestro":""}

    for key,value in key_find.items():
        if key not in ("fec_envio_siniestro_lote_ini","fec_envio_siniestro_lote_fin","num_documento_proveedor_lote","cod_estado_siniestro_lote","des_cod_estado_comprobante_pago_lote","cod_sede_proveedor_siniestro"):
            msg_err(obj_request,404,'los campos de búsqueda no son correctos ')
            return obj_request
        if key not in ("fec_envio_siniestro_lote_ini","fec_envio_siniestro_lote_fin"):
            key_find_2[key] = value      
    
    regex = ""

    for key in key_master:
        if key_find_2.get(key,"null") != "null":
            regex = regex + "#" + str(key_find_2[key])
        else:
            regex = regex + "#[^#]+"  

    obj_request={}
    obj_request=CONSTANT_REQUEST_OK

    try:
        start = datetime.datetime.strptime(key_find['fec_envio_siniestro_lote_ini'], "%Y-%m-%d")
        end = datetime.datetime.strptime(key_find['fec_envio_siniestro_lote_fin'], "%Y-%m-%d")
        if start > end:
            msg_err(obj_request,404,'la fecha final no puede ser menor que la fecha inicial')
            return obj_request

        date_generated = pd.date_range(start, end)
        fechas = date_generated.strftime("%Y-%m-%d")
       
        obj_request['data'] = []
        for fecha in fechas:           
            regex_filter = fecha + regex           
            table=get_gbt_table(table_find)
            rows = table.read_rows(
                filter_=row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(regex_filter.encode("utf-8")),
                    row_filters.CellsColumnLimitFilter(1),
                ]
                )
            )

            for row in rows:                
                obj_request['data'].append(row_to_dict(row,strcolumns_find,boo_suport))

        obj_request['error'] = {}
        obj_request['status'] = 'success'
        obj_request['code'] = 200 
            
        return obj_request           
            
    except ValueError:
        msg_err(obj_request,404,'La fecha ingresada es invalida.')
        return obj_request

def find_c006_c007(table_find, key_find ,strcolumns_find,boo_suport):        

    obj_request={}
    obj_request=CONSTANT_REQUEST_OK

    try:
        obj_request['data'] = []
        start = datetime.datetime.strptime(key_find['fecha'], "%Y-%m-%d")
        dateMesInicio = "%s-%s-01" % (start.year, start.month)
        dateMesInicio = datetime.datetime.strptime(dateMesInicio, "%Y-%m-%d")
        dateMesFin = "%s-%s-%s" % (start.year, start.month, calendar.monthrange(start.year,start.month)[1])
        dateMesFin = datetime.datetime.strptime(dateMesFin, "%Y-%m-%d")       
                       
        date_generated = pd.date_range(dateMesInicio, start)
        fechas_mes = date_generated.strftime("%Y-%m-%d")

        date_generated = pd.date_range(start, start)
        fechas_al_dia = date_generated.strftime("%Y-%m-%d")
        
        if key_find.get('cod_sede_proveedor',"null") == "null":
            key_find['cod_sede_proveedor'] = "NULL"

        conteo_total = consulta_rango_fechas(fechas_mes, key_find['num_documento_proveedor'],key_find['cod_sede_proveedor'],"uni__siniestro_lote_salud",["C007_cnt_documento_siniestro_lote"],boo_suport)
        obj_request['data'].append({"conteo_total":conteo_total})
        monto_total = consulta_rango_fechas(fechas_mes, key_find['num_documento_proveedor'],key_find['cod_sede_proveedor'],"uni__siniestro_proveedor_salud",["C006_mnt_documento_registrado_sol"],boo_suport)
        obj_request['data'].append({"monto_total":monto_total})
        #obj_request['data'] = consulta_rango_fechas(fechas_mes, key_find['num_documento_proveedor'],key_find['cod_sede_proveedor'],"uni__siniestro_proveedor_salud",["C006_mnt_documento_registrado_sol"],boo_suport)

        conteo_actual = consulta_rango_fechas(fechas_al_dia, key_find['num_documento_proveedor'],key_find['cod_sede_proveedor'],"uni__siniestro_lote_salud",["C007_cnt_documento_siniestro_lote"],boo_suport)
        obj_request['data'].append({"conteo_actual":conteo_actual})
        monto_actual = consulta_rango_fechas(fechas_al_dia, key_find['num_documento_proveedor'],key_find['cod_sede_proveedor'],"uni__siniestro_proveedor_salud",["C006_mnt_documento_registrado_sol"],boo_suport)        
        obj_request['data'].append({"monto_actual":monto_actual})
        #print(f"Monto total: {monto_total}, Monto Actual: {monto_actual}, Conteo Total: {conteo_total}, Conteo Actual: {conteo_actual}")
        #print(f"Monto total: {monto_total}, Conteo Total: {conteo_total}")        
        #obj_request['data'] = {{"monto_total":monto_total},{"conteo_total":conteo_total}}
        obj_request['error'] = {}
        obj_request['status'] = 'success'
        obj_request['code'] = 200 
            
        return obj_request           
            
    except ValueError:
        msg_err(obj_request,404,'La fecha ingresada es invalida.')
        return obj_request

def consulta_rango_fechas(fechas, num_documento_proveedor,cod_sede_proveedor,table_find,strcolumns_find,boo_suport):
    lista = []
    if table_find == "uni__siniestro_proveedor_salud":
        if cod_sede_proveedor != "NULL":
            regex = '#[^#]+#' + num_documento_proveedor  + '#[^#]+#[^#]+#[^#]+#[^#]+#' + cod_sede_proveedor
        else:
            regex = '#[^#]+#' + num_documento_proveedor  + '#[^#]+#[^#]+#[^#]+#[^#]+#[^#]+'
    if table_find == "uni__siniestro_lote_salud":
        if cod_sede_proveedor != "NULL":
            regex = '#' + num_documento_proveedor  + '#[^#]+#[^#]+#' + cod_sede_proveedor
        else:
            regex = '#' + num_documento_proveedor  + '#[^#]+#[^#]+#[^#]+'
    print(regex)
    for fecha in fechas:
            #hash_key = fecha + '#' + num_documento_proveedor          
            #table=get_gbt_table(table_find)
            regex_filter = fecha + regex           
            table=get_gbt_table(table_find)
            rows = table.read_rows(
                filter_=row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(regex_filter.encode("utf-8")),
                    row_filters.CellsColumnLimitFilter(1),
                ]
                )
            )
            # prefix = hash_key
            # end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
            # row_set = RowSet()
            # row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
            # rows = table.read_rows(row_set=row_set,filter_=row_filters.CellsColumnLimitFilter(1))
            for row in rows:                
                lista.append(row_to_dict(row,strcolumns_find,boo_suport))
    suma = 0
    for item in lista:
        for key,value in item.items():
            suma += float(value)
    return round(suma,2)

def find_c008(table_find, key_find ,strcolumns_find,boo_suport): 

    hash_key = '#'.join([key_find[x] for x in key_find])    
    field_key = '#'.join([x for x in key_find])
    obj_request8={}
    obj_request8=CONSTANT_REQUEST_OK
    if field_key not in "id_persona":
        msg_err(obj_request8,404,'los campos de búsqueda no son correctos ')
        return obj_request8
    print("lib01")
    table=get_gbt_table(table_find)
    prefix = hash_key + "#0#0"
    print(prefix)
    end_key = hash_key + "#z#z"
    print(end_key)
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
    rows = table.read_rows(row_set=row_set,filter_=row_filters.CellsColumnLimitFilter(1))
    print("lib02")
    i=0
    obj_request8['data'] = {}
    for row in rows:
        i+=1
        obj_request8['data'][i]=row_to_dict(row,strcolumns_find,boo_suport)
        print("RESULTADO: ")

    obj_request8['error'] = {}
    obj_request8['status'] = 'success'
    obj_request8['code'] = 200 
    print("lib03")
    return obj_request8

def find_c009(table_find, key_find ,strcolumns_find,boo_suport): 
    
    #Modificacion de consulta por rango de fechas:

    key_find_2 = {}
    key_master = {"des_actividad_siniestro": "","des_tipo_proceso_auditoria": "","val_proceso_auditoria_admin": "", "cod_proveedor":"", "cod_sede_proveedor":"", "des_correo_corporativo":""}

    for key,value in key_find.items():
        if key not in ("fec_inicio_actividad","fec_fin_actividad","des_actividad_siniestro","des_tipo_proceso_auditoria","val_proceso_auditoria_admin","cod_proveedor","cod_sede_proveedor","des_correo_corporativo"):
            msg_err(obj_request,404,'los campos de búsqueda no son correctos ')
            return obj_request
        if key not in ("fec_inicio_actividad","fec_fin_actividad"):
            key_find_2[key] = value      

    regex = ""

    for key in key_master:
        if key_find_2.get(key,"null") != "null":
            regex = regex + "#" + str(key_find_2[key])
        else:
            regex = regex + "#[^#]+"  

    obj_request={}
    obj_request=CONSTANT_REQUEST_OK

    try:
        start = datetime.datetime.strptime(key_find['fec_inicio_actividad'], "%Y-%m-%d")
        end = datetime.datetime.strptime(key_find['fec_fin_actividad'], "%Y-%m-%d")
        #today = datetime.date(today).strftime("%Y-%m-%d")
        #print(today)
        #if key_find['fec_fin_actividad'] == "9999-12-31" :
            #end = today
        #else:
            #end = datetime.datetime.strptime(key_find['fec_fin_actividad'], "%Y-%m-%d")
        if start > end:
            msg_err(obj_request,404,'la fecha final no puede ser menor que la fecha inicial')
            return obj_request

        date_generated = pd.date_range(start, end)
        fechas = date_generated.strftime("%Y-%m-%d")
       
        obj_request['data'] = []

        #PARA EL RANGO DE FECHAS QUE ESTEN ENTRE fec_inicio_actividad y fec_fin_actividad
        #C009_fec_inicio_actividad between "2024-05-02" and "2024-05-31" and C009_fec_fin_actividad between "2024-05-02" and "2024-05-31"
        # for fecha in fechas:
        #     for fecha2 in fechas:           
        #         regex_filter = fecha + "#" + fecha2 + regex + "#[^#]+#[^#]+#[^#]+#[^#]+#[^#]+"
        #         print(regex_filter)
        #         table=get_gbt_table(table_find)
        #         rows = table.read_rows(
        #             filter_=row_filters.RowFilterChain(filters=[
        #                 row_filters.RowKeyRegexFilter(regex_filter.encode("utf-8")),
        #                 row_filters.CellsColumnLimitFilter(1),
        #             ]
        #             )
        #         )

        #         for row in rows:                
        #             obj_request['data'].append(row_to_dict(row,strcolumns_find,boo_suport))

        
        # #PARA EL RANGO DE FECHAS QUE ESTEN ENTRE fec_inicio_actividad O fec_fin_actividad
        # #C009_fec_inicio_actividad between "2024-05-02" and "2024-05-31" or C009_fec_fin_actividad between "2024-05-02" and "2024-05-31"
        for fecha in fechas:         
            regex_filter = fecha + "#[^#]+" + regex + "#[^#]+#[^#]+#[^#]+#[^#]+#[^#]+"
            print(regex_filter)
            table=get_gbt_table(table_find)
            rows = table.read_rows(
                filter_=row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(regex_filter.encode("utf-8")),
                    row_filters.CellsColumnLimitFilter(1),
                ]
                )
            )

            for row in rows:                
                data = row_to_dict(row,strcolumns_find,boo_suport)
                #obj_request['data'].append(row_to_dict(row,strcolumns_find,boo_suport)) 
                obj_request['data'].append(data)  if data not in obj_request['data'] else None

        for fecha in fechas:         
            regex_filter = "[^#]+#" + fecha + regex + "#[^#]+#[^#]+#[^#]+#[^#]+#[^#]+"
            print(regex_filter)
            table=get_gbt_table(table_find)
            rows = table.read_rows(
                filter_=row_filters.RowFilterChain(filters=[
                    row_filters.RowKeyRegexFilter(regex_filter.encode("utf-8")),
                    row_filters.CellsColumnLimitFilter(1),
                ]
                )
            )

            for row in rows:                
                data = row_to_dict(row,strcolumns_find,boo_suport)
                #obj_request['data'].append(row_to_dict(row,strcolumns_find,boo_suport)) 
                obj_request['data'].append(data)  if data not in obj_request['data'] else None


        obj_request['error'] = {}
        obj_request['status'] = 'success'
        obj_request['code'] = 200 
            
        return obj_request           
            
    except ValueError:
        msg_err(obj_request,404,'La fecha ingresada es invalida.')
        return obj_request

def find_c010(table_find, key_find ,strcolumns_find,boo_suport): 

    hash_key = '#'.join([key_find[x] for x in key_find])    
    field_key = '#'.join([x for x in key_find])
    obj_request2={}
    obj_request2=CONSTANT_REQUEST_OK
    if field_key not in "tip_documento#num_documento":
        msg_err(obj_request2,404,'los campos de búsqueda no son correctos ')
        return obj_request2
    table=get_gbt_table(table_find)
    prefix = hash_key
    end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
    rows = table.read_rows(row_set=row_set,filter_=row_filters.CellsColumnLimitFilter(1))
    i=0
    obj_request2['data'] = {}
    for row in rows:
        i+=1
        obj_request2['data'][i]=row_to_dict(row,strcolumns_find,boo_suport)

    obj_request2['error'] = {}
    obj_request2['status'] = 'success'
    obj_request2['code'] = 200 
    return obj_request2

def find_c011(table_find, key_find, strcolumns_find, boo_suport):
    hash_key = '#'.join([key_find[x] for x in key_find])
    field_key = '#'.join([x for x in key_find])
    obj_request = {}
    obj_request = CONSTANT_REQUEST_OK
    if field_key not in "tip_documento_afiliado#num_documento_afiliado":
        msg_err(obj_request, 404, 'los campos de búsqueda no son correctos ')
        return obj_request

    table = get_gbt_table(table_find)
    prefix = hash_key
    end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
    rows = table.read_rows(row_set=row_set, filter_=row_filters.CellsColumnLimitFilter(1))

    i = 0
    obj_request['data'] = {}
    for row in rows:
        i += 1
        obj_request['data'][i] = row_to_dict(row, strcolumns_find, boo_suport)

    obj_request['error'] = {}
    obj_request['status'] = 'success'
    obj_request['code'] = 200

    return obj_request

def find_c012(table_find, key_find, strcolumns_find, boo_suport):
    hash_key = '#'.join([key_find[x] for x in key_find])
    field_key = '#'.join([x for x in key_find])
    obj_request = {}
    obj_request = CONSTANT_REQUEST_OK
    if field_key not in "tip_documento_afiliado#num_documento_afiliado":
        msg_err(obj_request, 404, 'los campos de búsqueda no son correctos ')
        return obj_request

    table = get_gbt_table(table_find)
    prefix = hash_key
    end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
    rows = table.read_rows(row_set=row_set, filter_=row_filters.CellsColumnLimitFilter(1))

    i = 0
    obj_request['data'] = {}
    for row in rows:
        i += 1
        obj_request['data'][i] = row_to_dict(row, strcolumns_find, boo_suport)

    obj_request['error'] = {}
    obj_request['status'] = 'success'
    obj_request['code'] = 200

    return obj_request

def find_c013(table_find, key_find, strcolumns_find, boo_suport):
    hash_key = '#'.join([key_find[x] for x in key_find])
    field_key = '#'.join([x for x in key_find])
    obj_request = {}
    obj_request = CONSTANT_REQUEST_OK
    if field_key not in "tip_documento_afiliado#num_documento_afiliado":
        msg_err(obj_request, 404, 'los campos de búsqueda no son correctos ')
        return obj_request

    table = get_gbt_table(table_find)
    prefix = hash_key
    end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
    rows = table.read_rows(row_set=row_set, filter_=row_filters.CellsColumnLimitFilter(1))

    i = 0
    obj_request['data'] = {}
    for row in rows:
        i += 1
        obj_request['data'][i] = row_to_dict(row, strcolumns_find, boo_suport)

    obj_request['error'] = {}
    obj_request['status'] = 'success'
    obj_request['code'] = 200

    return obj_request

def find_c014(table_find, key_find, strcolumns_find, boo_suport):
    hash_key = '#'.join([key_find[x] for x in key_find])
    field_key = '#'.join([x for x in key_find])
    obj_request = {}
    obj_request = CONSTANT_REQUEST_OK
    if field_key not in "tip_documento_afiliado#num_documento_afiliado":
        msg_err(obj_request, 404, 'los campos de búsqueda no son correctos ')
        return obj_request

    table = get_gbt_table(table_find)
    prefix = hash_key
    end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
    rows = table.read_rows(row_set=row_set, filter_=row_filters.CellsColumnLimitFilter(1))

    i = 0
    obj_request['data'] = {}
    for row in rows:
        i += 1
        obj_request['data'][i] = row_to_dict(row, strcolumns_find, boo_suport)

    obj_request['error'] = {}
    obj_request['status'] = 'success'
    obj_request['code'] = 200

    return obj_request

def find_c015(table_find, key_find, strcolumns_find, boo_suport):
    hash_key = '#'.join([key_find[x] for x in key_find])
    field_key = '#'.join([x for x in key_find])
    obj_request = {}
    obj_request = CONSTANT_REQUEST_OK
    if field_key not in "tip_documento_afiliado#num_documento_afiliado":
        msg_err(obj_request, 404, 'los campos de búsqueda no son correctos ')
        return obj_request

    table = get_gbt_table(table_find)
    prefix = hash_key
    end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
    rows = table.read_rows(row_set=row_set, filter_=row_filters.CellsColumnLimitFilter(1))

    i = 0
    obj_request['data'] = {}
    for row in rows:
        i += 1
        obj_request['data'][i] = row_to_dict(row, strcolumns_find, boo_suport)

    obj_request['error'] = {}
    obj_request['status'] = 'success'
    obj_request['code'] = 200

    return obj_request

def find_c016(table_find, key_find, strcolumns_find, boo_suport):
    hash_key = '#'.join([key_find[x] for x in key_find])
    field_key = '#'.join([x for x in key_find])
    obj_request = {}
    obj_request = CONSTANT_REQUEST_OK
    if field_key not in "tip_documento#num_documento":
        msg_err(obj_request, 404, 'los campos de búsqueda no son correctos ')
        return obj_request

    table = get_gbt_table(table_find)
    prefix = hash_key
    end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"), end_key.encode("utf-8"))
    rows = table.read_rows(row_set=row_set, filter_=row_filters.CellsColumnLimitFilter(1))

    i = 0
    obj_request['data'] = {}
    for row in rows:
        i += 1
        obj_request['data'][i] = row_to_dict(row, strcolumns_find, boo_suport)

    obj_request['error'] = {}
    obj_request['status'] = 'success'
    obj_request['code'] = 200

    return obj_request

def dummy_test(table_find, key_find ,strcolumns_find, lis_resultado,boo_suport): 
    lis_resultado['data']= {
        "1":{            
            "campo1": "valor1",
            "campo2": "valor2",
            "campo3": "valor3"
        },
        "2":{
            "campo1": "valor4",
            "campo2": "valor5",
            "campo3": "valor6"
        },
        "3":{
            "campo1": "valor7",
            "campo2": "valor8",
            "campo3": "valor9"
        }
    }
    
