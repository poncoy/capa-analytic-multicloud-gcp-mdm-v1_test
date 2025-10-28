import os
from .__metadata__ import (
    __URL__, __DESCRIPTION__,__DEV__, __DEV_EMAIL__
)

# Archivo de configuracion Dataflow, para la especificacion de tablas y 
# columans que seran enviadas a la capa rapida.
# Interes: Poder dinamizarlo a traves de un json

PACKAGE_NAME = 'fast_layer'
PACKAGE_VERSION = '0.1.0'

tablas_replica =  \
{
    "cliente_persona":"PER__cliente_persona",
    "cliente_persona_detalle":"PER__cliente_persona_detalle",
    "prospecto_persona":"PER__prospecto_persona"
}

#Query dinamico para  la obtencion de datos con periodo
QUERY = """
    SELECT 
    {COLUMNS}
    FROM `{TABLE}`
"""
