# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbConfiguracionInicial
# MAGIC
# MAGIC **Proposito**: Configuracion inicial del proyecto DbsFreeLakeflowSparkDeclarativePipelinesBase.
# MAGIC
# MAGIC Este notebook realiza las siguientes acciones en una sola ejecucion idempotente:
# MAGIC - Crea los 4 catalogos de Unity Catalog: control, bronce, plata, oro
# MAGIC - Crea el esquema de la tabla Parametros (parametro de entrada)
# MAGIC - Crea la tabla Delta centralizada de parametros con CREATE OR REPLACE TABLE
# MAGIC - Inserta los 15 registros de configuracion del proyecto
# MAGIC - Lee los esquemas desde la tabla y crea los 4 esquemas medallion
# MAGIC - Crea el Volume gestionado para almacenamiento de archivos parquets
# MAGIC
# MAGIC **Idempotencia**: Re-ejecutar este notebook produce el mismo resultado sin errores.
# MAGIC
# MAGIC **Prerequisitos**:
# MAGIC - Workspace de Databricks Free Edition activo con Unity Catalog habilitado
# MAGIC - Computo Serverless disponible
# MAGIC - Permisos para CREATE CATALOG, CREATE SCHEMA, CREATE TABLE, CREATE VOLUME

# COMMAND ----------

# ==============================================================================
# PASO 1 Y 2: DEFINICION Y CAPTURA DE WIDGETS (PARAMETROS DE ENTRADA)
# ==============================================================================
# Se definen los 3 widgets de entrada con valores por defecto que representan
# la configuracion recomendada del proyecto. Los valores son modificables desde
# la interfaz de Databricks o desde la extension de VS Code antes de ejecutar.
#
# RF-001: dbutils.widgets.text() con valores por defecto
# ==============================================================================

import time

dbutils.widgets.text("catalogoParametro", "control",    "Catalogo de la tabla Parametros")
dbutils.widgets.text("esquemaParametro",  "lab1",   "Esquema de la tabla Parametros")
dbutils.widgets.text("tablaParametros",   "Parametros", "Nombre de la tabla Parametros")

# Captura inmediata de valores de widgets en variables locales
catalogo_parametro = dbutils.widgets.get("catalogoParametro")
esquema_parametro  = dbutils.widgets.get("esquemaParametro")
tabla_parametros   = dbutils.widgets.get("tablaParametros")

# Ruta completa de la tabla de parametros (se usa en multiples pasos)
ruta_tabla_parametros = f"{catalogo_parametro}.{esquema_parametro}.{tabla_parametros}"

# COMMAND ----------

# ==============================================================================
# PASO 3: VALIDACION DE PARAMETROS DE ENTRADA
# ==============================================================================
# Se valida que los 3 parametros obligatorios no esten vacios antes de continuar.
# Si alguno esta vacio, la ejecucion se detiene con un mensaje explicativo.
#
# RF-008: Validar parametros no vacios antes de cualquier operacion
# ==============================================================================

parametros_vacios = []

if not catalogo_parametro or not catalogo_parametro.strip():
    parametros_vacios.append("catalogoParametro")
if not esquema_parametro or not esquema_parametro.strip():
    parametros_vacios.append("esquemaParametro")
if not tabla_parametros or not tabla_parametros.strip():
    parametros_vacios.append("tablaParametros")

if parametros_vacios:
    mensaje_error = (
        "ERROR DE VALIDACION: Los siguientes parametros son obligatorios y no pueden "
        "estar vacios: " + ", ".join(parametros_vacios) + ". "
        "Proporcione un valor para cada parametro y vuelva a ejecutar el notebook."
    )
    print(mensaje_error)
    raise ValueError(mensaje_error)

print("Validacion de parametros de entrada: CORRECTA")

# COMMAND ----------

# ==============================================================================
# PASO 4: BLOQUE DE RESUMEN INICIAL
# ==============================================================================
# Se imprime el resumen de los parametros recibidos y la ruta de la tabla
# destino antes de comenzar las operaciones. Permite verificar la configuracion.
#
# RF-010: Bloque de resumen al inicio con todos los parametros recibidos
# ==============================================================================

tiempo_inicio_total = time.time()

print("=" * 70)
print("INICIO: NbConfiguracionInicial")
print("=" * 70)
print(f"  Catalogo de la tabla Parametros : {catalogo_parametro}")
print(f"  Esquema de la tabla Parametros  : {esquema_parametro}")
print(f"  Nombre de la tabla Parametros   : {tabla_parametros}")
print(f"  Ruta completa de la tabla       : {ruta_tabla_parametros}")
print("=" * 70)

# COMMAND ----------

# ==============================================================================
# PASO 5: CREACION DE CATALOGOS Y ESQUEMA DE LA TABLA PARAMETROS
# ==============================================================================
# Se definen los 15 registros de configuracion. Los nombres de los catalogos
# medallion se derivan de estos registros para evitar valores hardcodeados.
# Luego se crean los 4 catalogos y el esquema de la tabla Parametros.
#
# Se crean PRIMERO los catalogos y el esquema de la tabla Parametros porque
# estos son prerrequisito para el CREATE OR REPLACE TABLE del Paso 6.
# Los esquemas medallion se crean en el Paso 9 (sus nombres estan en la tabla).
#
# RF-013 (parcial): catalogos con IF NOT EXISTS + esquema de la tabla Parametros
# RF-011: nombres de catalogos derivados de registros_parametros (no hardcoded)
# ==============================================================================

# Definicion centralizada de los 15 registros de configuracion del proyecto.
# Esta lista es la unica fuente de verdad para los valores que se insertan
# en la tabla Parametros. Los nombres de catalogos se derivan de aqui.
registros_parametros = [
    ("catalogoBronce",   "bronce"),
    ("esquemaBronce",    "lab1"),
    ("contenedorBronce", "bronce"),
    ("TipoStorage",      "Volume"),
    ("catalogoVolume",   "bronce"),
    ("esquemaVolume",    "lab1"),
    ("nombreVolume",     "datos_bronce"),
    ("bucketS3",         ""),
    ("prefijoS3",        ""),
    ("DirectorioBronce", "archivos"),
    ("catalogoPlata",    "plata"),
    ("esquemaPlata",     "lab1"),
    ("catalogoOro",      "oro"),
    ("esquemaOro",       "lab1"),
    ("esquemaControl",   "lab1"),
]

# Diccionario de configuracion inicial para consultas previas a la tabla
dict_config_inicial = dict(registros_parametros)

# Los nombres de los catalogos medallion se leen del diccionario de configuracion
catalogo_bronce = dict_config_inicial["catalogoBronce"]
catalogo_plata  = dict_config_inicial["catalogoPlata"]
catalogo_oro    = dict_config_inicial["catalogoOro"]
# El catalogo de control es el mismo que aloja la tabla Parametros (widget)

# Lista ordenada de catalogos a crear: control primero (aloja la tabla Parametros)
catalogos_a_crear = [catalogo_parametro, catalogo_bronce, catalogo_plata, catalogo_oro]

tiempos_catalogos = {}

print("Creando catalogos de Unity Catalog...")
for catalogo in catalogos_a_crear:
    t_inicio = time.time()
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalogo}")
    t_fin = time.time()
    tiempos_catalogos[catalogo] = round(t_fin - t_inicio, 3)
    print(f"  Catalogo '{catalogo}': OK ({tiempos_catalogos[catalogo]}s)")

# Crear el esquema que alojara la tabla Parametros
print(f"Creando esquema de la tabla Parametros: {catalogo_parametro}.{esquema_parametro}...")
t_inicio_esquema_param = time.time()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo_parametro}.{esquema_parametro}")
t_fin_esquema_param = time.time()
tiempo_esquema_param = round(t_fin_esquema_param - t_inicio_esquema_param, 3)
print(f"  Esquema '{catalogo_parametro}.{esquema_parametro}': OK ({tiempo_esquema_param}s)")

# COMMAND ----------

# ==============================================================================
# PASO 6: CREACION DE LA TABLA PARAMETROS
# ==============================================================================
# Se crea (o recrea) la tabla Delta con CREATE OR REPLACE TABLE.
# Esta estrategia garantiza idempotencia absoluta: siempre produce exactamente
# la estructura correcta sin importar si la tabla existia previamente o tenia
# una estructura de columnas diferente.
#
# RF-002: Tabla con columnas Clave STRING y Valor STRING
# RF-004: Idempotencia via CREATE OR REPLACE TABLE
# RF-009: Cronometro de la operacion DDL de creacion de tabla
# ==============================================================================

print(f"Creando tabla Parametros: {ruta_tabla_parametros}...")
t_inicio_tabla = time.time()
spark.sql(f"""
    CREATE OR REPLACE TABLE {ruta_tabla_parametros} (
        Clave STRING NOT NULL,
        Valor STRING NOT NULL
    )
""")
t_fin_tabla = time.time()
tiempo_tabla = round(t_fin_tabla - t_inicio_tabla, 3)
print(f"  Tabla '{ruta_tabla_parametros}': OK ({tiempo_tabla}s)")

# COMMAND ----------

# ==============================================================================
# PASO 7: INSERCION DE LOS 15 REGISTROS DE PARAMETROS
# ==============================================================================
# Se construye un unico INSERT con todos los valores para minimizar el numero
# de operaciones Spark (optimizacion para Serverless con recursos limitados).
# Cada registro se imprime para verificacion visual antes de la escritura.
#
# RF-003: Todos los 15 registros requeridos por el proyecto
# RF-005: Impresion de la ubicacion de la tabla y cada registro insertado
# RF-011: Sin valores hardcodeados en DDL; vienen de registros_parametros
# ==============================================================================

print(f"Insertando {len(registros_parametros)} registros en {ruta_tabla_parametros}...")
print(f"  Tabla destino: {ruta_tabla_parametros}")

# Imprimir cada registro para verificacion visual (RF-005)
for clave, valor in registros_parametros:
    valor_impresion = valor if valor else "(vacio)"
    print(f"  -> Clave: '{clave}' | Valor: '{valor_impresion}'")

# Construir e insertar todos los valores en una sola sentencia SQL
valores_sql = ",\n        ".join(
    [f"('{clave}', '{valor.replace(chr(39), chr(39)+chr(39))}')"
     for clave, valor in registros_parametros]
)

t_inicio_inserts = time.time()
spark.sql(f"""
    INSERT INTO {ruta_tabla_parametros}
    VALUES
        {valores_sql}
""")
t_fin_inserts = time.time()
tiempo_inserts = round(t_fin_inserts - t_inicio_inserts, 3)
print(f"  {len(registros_parametros)} registros insertados: OK ({tiempo_inserts}s)")

# COMMAND ----------

# ==============================================================================
# PASO 8: LECTURA DE VALORES DE ESQUEMAS Y DATOS DEL VOLUME DESDE LA TABLA
# ==============================================================================
# Se leen los valores de esquemas medallion y los datos del Volume directamente
# desde la tabla Parametros que se acaba de crear. Esto garantiza que el codigo
# de los pasos 9 y 10 no tenga valores hardcodeados; siempre usa lo que
# realmente esta almacenado en la tabla.
# ==============================================================================

print(f"Leyendo configuracion desde {ruta_tabla_parametros}...")
df_parametros = spark.read.table(ruta_tabla_parametros)
dict_parametros = {fila["Clave"]: fila["Valor"] for fila in df_parametros.collect()}

# Esquemas medallion (uno por catalogo)
esquema_control = dict_parametros["esquemaControl"]
esquema_bronce  = dict_parametros["esquemaBronce"]
esquema_plata   = dict_parametros["esquemaPlata"]
esquema_oro     = dict_parametros["esquemaOro"]

# Datos del Volume gestionado
catalogo_volume = dict_parametros["catalogoVolume"]
esquema_volume  = dict_parametros["esquemaVolume"]
nombre_volume   = dict_parametros["nombreVolume"]

print(f"  esquemaControl  : {esquema_control}")
print(f"  esquemaBronce   : {esquema_bronce}")
print(f"  esquemaPlata    : {esquema_plata}")
print(f"  esquemaOro      : {esquema_oro}")
print(f"  catalogoVolume  : {catalogo_volume}")
print(f"  esquemaVolume   : {esquema_volume}")
print(f"  nombreVolume    : {nombre_volume}")

# COMMAND ----------

# ==============================================================================
# PASO 9: CREACION DE LOS 4 ESQUEMAS MEDALLION
# ==============================================================================
# Con los nombres de esquemas leidos de la tabla, se crean los esquemas en sus
# catalogos correspondientes. IF NOT EXISTS garantiza idempotencia.
#
# Nota: El esquema de la tabla Parametros (catalogo_parametro.esquema_parametro)
# puede coincidir con uno de estos (p.ej. control.lab1). El IF NOT EXISTS
# maneja esto correctamente sin errores ni duplicados.
#
# RF-013 (completa): esquemas medallion con IF NOT EXISTS
# RF-009: Cronometro por cada operacion DDL de esquema
# ==============================================================================

esquemas_medallion = [
    (catalogo_parametro, esquema_control),
    (catalogo_bronce,    esquema_bronce),
    (catalogo_plata,     esquema_plata),
    (catalogo_oro,       esquema_oro),
]

tiempos_esquemas = {}

print("Creando esquemas medallion...")
for catalogo, esquema in esquemas_medallion:
    ruta_esquema = f"{catalogo}.{esquema}"
    t_inicio = time.time()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ruta_esquema}")
    t_fin = time.time()
    tiempos_esquemas[ruta_esquema] = round(t_fin - t_inicio, 3)
    print(f"  Esquema '{ruta_esquema}': OK ({tiempos_esquemas[ruta_esquema]}s)")

# COMMAND ----------

# ==============================================================================
# PASO 10: CREACION DEL VOLUME GESTIONADO
# ==============================================================================
# Se crea el Volume gestionado de Unity Catalog usando los valores leidos de
# la tabla Parametros. IF NOT EXISTS garantiza idempotencia: si el Volume ya
# existia no se produce error y se reporta su ruta de acceso de todas formas.
#
# RF-006: CREATE VOLUME IF NOT EXISTS con valores de la tabla
# RF-007: Idempotencia del Volume
# RF-009: Cronometro de la operacion DDL
# ==============================================================================

ruta_volume          = f"{catalogo_volume}.{esquema_volume}.{nombre_volume}"
ruta_acceso_volume   = f"/Volumes/{catalogo_volume}/{esquema_volume}/{nombre_volume}/"

print(f"Creando Volume gestionado: {ruta_volume}...")
t_inicio_volume = time.time()
spark.sql(f"CREATE VOLUME IF NOT EXISTS {ruta_volume}")
t_fin_volume = time.time()
tiempo_volume = round(t_fin_volume - t_inicio_volume, 3)
print(f"  Volume '{ruta_volume}': OK ({tiempo_volume}s)")
print(f"  Ruta de acceso : {ruta_acceso_volume}")

# COMMAND ----------

# ==============================================================================
# PASO 11: BLOQUE DE RESUMEN FINAL
# ==============================================================================
# Se imprime el resumen completo de la ejecucion: todos los recursos creados
# o verificados, cantidad de registros insertados, tiempos de cada operacion
# DDL y tiempo total de ejecucion del notebook.
#
# RF-009: Tiempos de todas las operaciones DDL
# RF-010: Bloque de resumen final con metricas de ejecucion
# RF-012: Documentacion completa en espanol
# ==============================================================================

tiempo_fin_total = time.time()
tiempo_total     = round(tiempo_fin_total - tiempo_inicio_total, 3)

print("=" * 70)
print("RESUMEN FINAL: NbConfiguracionInicial")
print("=" * 70)
print("Recursos creados / verificados (todos idempotentes):")
print(f"  Catalogos        : {len(catalogos_a_crear)}  {catalogo_parametro}, {catalogo_bronce}, {catalogo_plata}, {catalogo_oro}")
print(f"  Esquema tabla    : {catalogo_parametro}.{esquema_parametro}")
print(f"  Tabla Parametros : {ruta_tabla_parametros}  ({len(registros_parametros)} registros)")
print(f"  Esquemas medallion: {len(esquemas_medallion)}")
for catalogo, esquema in esquemas_medallion:
    print(f"    - {catalogo}.{esquema}")
print(f"  Volume gestionado: {ruta_volume}")
print(f"  Ruta de acceso   : {ruta_acceso_volume}")
print("-" * 70)
print("Tiempos de operaciones DDL:")
for catalogo, t in tiempos_catalogos.items():
    print(f"  CREATE CATALOG {catalogo:<12}: {t}s")
print(f"  CREATE SCHEMA  {catalogo_parametro + '.' + esquema_parametro:<20}: {tiempo_esquema_param}s")
print(f"  CREATE TABLE   {tabla_parametros:<20}: {tiempo_tabla}s")
print(f"  INSERT         {len(registros_parametros)} registros        : {tiempo_inserts}s")
for ruta_esq, t in tiempos_esquemas.items():
    print(f"  CREATE SCHEMA  {ruta_esq:<20}: {t}s")
print(f"  CREATE VOLUME  {nombre_volume:<20}: {tiempo_volume}s")
print("-" * 70)
print(f"  Tiempo total de ejecucion : {tiempo_total}s")
print("=" * 70)
print("NbConfiguracionInicial: COMPLETADO EXITOSAMENTE")
print("=" * 70)
