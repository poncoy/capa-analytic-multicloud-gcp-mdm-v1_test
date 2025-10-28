## Objetivo
El objetivo de este proyecto es de la disponibilizacion de datos del feature store y la capa deliver de la Coe IAAA a una base de datos de baja latencia.

## Estructura
##### folders & files
- [`main.py`] Archivo de orquestacion del Dataflow pipeline
- [`build_dataflow.py`] Archivo de creacion de Dataflow pipeline para el CD
- [`setup.py`] Archivo de setup de Dataflow pipeline para el CD
- [`requirements.txt`] Archivo de dependencias el CD
- [`pipeline`] Package
  - [`__init__.py`] Para reconocimiento como module
  - [`__metadata__.py`] Para metadata y labels
  - [`config.py`] Archivo de configuracion(Tobe: Json Config)
  - [`transform.py`] Archivo con los modulos de transformacion
- [`pipeline/queries`] source files
  - [`__init__.py`] Para reconocimiento como module
  - [`query.py`] Archivo donde guardar los querys usados

##### Run Locally

- Ejecutar notebook [`build_dataflow.py`]
