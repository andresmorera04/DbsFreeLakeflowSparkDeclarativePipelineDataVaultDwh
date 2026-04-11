# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbGenerarTransaccionalCliente
# MAGIC
# MAGIC **Proposito**: Genera un archivo parquet simulando las transacciones bancarias
# MAGIC del sistema AS400 (TRXPFL). El parquet contiene 60 columnas con distribucion de
# MAGIC tipos: 7 StringType, 19 DateType, 2 TimestampType, 2 LongType, 30 DoubleType.
# MAGIC
# MAGIC **Dependencia**: Requiere que el Maestro de Clientes (CMSTFL) haya sido generado
# MAGIC previamente. Lee los CUSTID validos del parquet CMSTFL para garantizar integridad
# MAGIC referencial.
# MAGIC
# MAGIC **Distribucion de tipos de transaccion**:
# MAGIC - Alta (~60%): CATM, DATM, CMPR, TINT, DPST
# MAGIC - Media (~30%): PGSL, TEXT, RTRO, PGSV, NMNA, INTR
# MAGIC - Baja (~10%): ADSL, IMPT, DMCL, CMSN
# MAGIC
# MAGIC **Prerequisitos**:
# MAGIC - Unity Catalog activo con tabla Parametros y parquet CMSTFL generado

# COMMAND ----------

# ==============================================================================
# PASO 1: IMPORTS Y CONFIGURACION INICIAL
# ==============================================================================

import time
import re
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, LongType, DoubleType, DateType, TimestampType
)

# COMMAND ----------

# ==============================================================================
# PASO 2: DEFINICION DE WIDGETS (PARAMETROS DE ENTRADA)
# ==============================================================================
# 11 widgets segun contrato. fechaTransaccion es obligatorio sin valor por defecto.
# ==============================================================================

dbutils.widgets.text("catalogoParametro",          "control",                          "Catalogo de la tabla Parametros")
dbutils.widgets.text("esquemaParametro",           "lab1",                         "Esquema de la tabla Parametros")
dbutils.widgets.text("tablaParametros",            "Parametros",                       "Nombre de la tabla Parametros")
dbutils.widgets.text("cantidadTransacciones",      "150000",                           "Cantidad de transacciones a generar")
dbutils.widgets.text("fechaTransaccion",           "",                                 "Fecha de transacciones formato YYYY-MM-DD")
dbutils.widgets.text("rutaRelativaTransaccional",  "LSDP_Base/As400/Transaccional/",   "Ruta relativa destino del parquet transaccional")
dbutils.widgets.text("rutaRelativaMaestroCliente", "LSDP_Base/As400/MaestroCliente/",  "Ruta relativa del parquet CMSTFL existente")
dbutils.widgets.text("montoMinimo",                "10",                               "Monto minimo para columnas DoubleType")
dbutils.widgets.text("montoMaximo",                "100000",                           "Monto maximo para columnas DoubleType")
dbutils.widgets.text("numeroParticiones",          "8",                                "Numero de particiones para coalesce")
dbutils.widgets.text("shufflePartitions",          "8",                                "Valor de spark.sql.shuffle.partitions")

# Captura de valores
catalogo_parametro         = dbutils.widgets.get("catalogoParametro")
esquema_parametro          = dbutils.widgets.get("esquemaParametro")
tabla_parametros           = dbutils.widgets.get("tablaParametros")
cantidad_transacciones_str = dbutils.widgets.get("cantidadTransacciones")
fecha_transaccion_str      = dbutils.widgets.get("fechaTransaccion")
ruta_relativa_transac      = dbutils.widgets.get("rutaRelativaTransaccional")
ruta_relativa_maestro      = dbutils.widgets.get("rutaRelativaMaestroCliente")
monto_minimo_str           = dbutils.widgets.get("montoMinimo")
monto_maximo_str           = dbutils.widgets.get("montoMaximo")
numero_particiones_str     = dbutils.widgets.get("numeroParticiones")
shuffle_partitions_str     = dbutils.widgets.get("shufflePartitions")

# COMMAND ----------

# ==============================================================================
# PASO 3: CONFIGURACION DE SPARK
# ==============================================================================

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_str)

# COMMAND ----------

# ==============================================================================
# PASO 4: VALIDACION DE PARAMETROS WIDGETS
# ==============================================================================

widgets_obligatorios = {
    "catalogoParametro":         catalogo_parametro,
    "esquemaParametro":          esquema_parametro,
    "tablaParametros":           tabla_parametros,
    "cantidadTransacciones":     cantidad_transacciones_str,
    "fechaTransaccion":          fecha_transaccion_str,
    "rutaRelativaTransaccional": ruta_relativa_transac,
    "rutaRelativaMaestroCliente":ruta_relativa_maestro,
    "montoMinimo":               monto_minimo_str,
    "montoMaximo":               monto_maximo_str,
    "numeroParticiones":         numero_particiones_str,
    "shufflePartitions":         shuffle_partitions_str,
}

vacios = [k for k, v in widgets_obligatorios.items() if not v or not v.strip()]
if vacios:
    raise ValueError(
        "ERROR DE VALIDACION: Los siguientes parametros son obligatorios y no pueden "
        "estar vacios: " + ", ".join(vacios) + ". Proporcione un valor y vuelva a ejecutar."
    )

# Validar cantidadTransacciones es entero positivo
try:
    cantidad_transacciones = int(cantidad_transacciones_str)
    if cantidad_transacciones <= 0:
        raise ValueError()
except ValueError:
    raise ValueError(
        f"ERROR: 'cantidadTransacciones' debe ser un entero positivo. Valor recibido: '{cantidad_transacciones_str}'"
    )

# Validar formato YYYY-MM-DD de fechaTransaccion
patron_fecha = re.compile(r"^\d{4}-\d{2}-\d{2}$")
if not patron_fecha.match(fecha_transaccion_str.strip()):
    raise ValueError(
        f"ERROR: 'fechaTransaccion' debe tener formato YYYY-MM-DD. Valor recibido: '{fecha_transaccion_str}'"
    )

# Validar montoMinimo < montoMaximo
try:
    monto_minimo = float(monto_minimo_str)
    monto_maximo = float(monto_maximo_str)
    if monto_minimo <= 0 or monto_maximo <= 0:
        raise ValueError()
    if monto_minimo >= monto_maximo:
        raise ValueError()
except ValueError:
    raise ValueError(
        f"ERROR: 'montoMinimo' y 'montoMaximo' deben ser numericos positivos con montoMinimo < montoMaximo. "
        f"Valores recibidos: montoMinimo='{monto_minimo_str}', montoMaximo='{monto_maximo_str}'"
    )

numero_particiones = int(numero_particiones_str)
fecha_transaccion  = fecha_transaccion_str.strip()

print("Validacion de parametros widgets: CORRECTA")

# COMMAND ----------

# ==============================================================================
# PASO 5: LECTURA DE TABLA PARAMETROS
# ==============================================================================

ruta_tabla_parametros = f"{catalogo_parametro}.{esquema_parametro}.{tabla_parametros}"

filas_parametros = spark.read.table(ruta_tabla_parametros).collect()
dict_parametros  = {fila["Clave"]: fila["Valor"] for fila in filas_parametros}

# Validar TipoStorage
tipo_storage = dict_parametros.get("TipoStorage", "")
if tipo_storage not in ("Volume", "AmazonS3"):
    raise ValueError(
        f"ERROR: TipoStorage debe ser 'Volume' o 'AmazonS3'. Valor en tabla: '{tipo_storage}'"
    )

# COMMAND ----------

# ==============================================================================
# PASO 6: CONSTRUCCION DINAMICA DE RUTAS
# ==============================================================================

if tipo_storage == "Volume":
    catalogo_volume      = dict_parametros.get("catalogoVolume", "")
    esquema_volume       = dict_parametros.get("esquemaVolume", "")
    nombre_volume        = dict_parametros.get("nombreVolume", "")
    ruta_completa_transac = f"/Volumes/{catalogo_volume}/{esquema_volume}/{nombre_volume}/{ruta_relativa_transac}"
    ruta_completa_maestro = f"/Volumes/{catalogo_volume}/{esquema_volume}/{nombre_volume}/{ruta_relativa_maestro}"
elif tipo_storage == "AmazonS3":
    bucket_s3             = dict_parametros.get("bucketS3", "")
    ruta_completa_transac = f"s3://{bucket_s3}/{ruta_relativa_transac}"
    ruta_completa_maestro = f"s3://{bucket_s3}/{ruta_relativa_maestro}"
else:
    raise ValueError(
        f"ERROR: TipoStorage '{tipo_storage}' no es valido. Valores aceptados: 'Volume', 'AmazonS3'"
    )

# COMMAND ----------

# ==============================================================================
# PASO 7: BLOQUE DE OBSERVABILIDAD INICIAL
# ==============================================================================

tiempo_inicio_total = time.time()

print("=" * 70)
print("INICIO: NbGenerarTransaccionalCliente (TRXPFL)")
print("=" * 70)
print("--- Parametros Widgets ---")
print(f"  catalogoParametro          : {catalogo_parametro}")
print(f"  esquemaParametro           : {esquema_parametro}")
print(f"  tablaParametros            : {tabla_parametros}")
print(f"  cantidadTransacciones      : {cantidad_transacciones}")
print(f"  fechaTransaccion           : {fecha_transaccion}")
print(f"  rutaRelativaTransaccional  : {ruta_relativa_transac}")
print(f"  rutaRelativaMaestroCliente : {ruta_relativa_maestro}")
print(f"  montoMinimo                : {monto_minimo}")
print(f"  montoMaximo                : {monto_maximo}")
print(f"  numeroParticiones          : {numero_particiones}")
print(f"  shufflePartitions          : {shuffle_partitions_str}")
print("--- Parametros Tabla Parametros ---")
for clave, valor in dict_parametros.items():
    print(f"  {clave:<30} : {valor}")
print("--- Storage ---")
print(f"  TipoStorage           : {tipo_storage}")
print(f"  Ruta destino transac  : {ruta_completa_transac}")
print(f"  Ruta maestro CMSTFL   : {ruta_completa_maestro}")
print("=" * 70)

# COMMAND ----------

# ==============================================================================
# PASO 8: VERIFICACION Y LECTURA DEL PARQUET CMSTFL
# ==============================================================================

try:
    df_maestro = spark.read.parquet(ruta_completa_maestro)
except Exception as e:
    raise ValueError(
        f"ERROR: No se pudo leer el Maestro de Clientes (CMSTFL) en '{ruta_completa_maestro}'. "
        f"Asegurese de ejecutar NbGenerarMaestroCliente primero. Detalle: {e}"
    )

# Obtener CUSTIDs unicos para integridad referencial (distinct evita duplicados
# cuando la ruta apunta a una carpeta padre con multiples particiones por dia)
df_custids_validos = df_maestro.select("CUSTID").distinct()
total_clientes = df_custids_validos.count()
print(f"Maestro de Clientes cargado: {total_clientes} CUSTIDs unicos")

# COMMAND ----------

# ==============================================================================
# PASO 9: GENERACION DE DATOS TRXPFL (60 COLUMNAS)
# ==============================================================================
# Distribucion de tipos de transaccion (15 tipos en 3 grupos):
#   Alta (~60%): CATM 15%, DATM 14%, CMPR 13%, TINT 10%, DPST 8%
#   Media (~30%): PGSL 7%, TEXT 6%, RTRO 5%, PGSV 5%, NMNA 4%, INTR 3%
#   Baja (~10%): ADSL 3%, IMPT 3%, DMCL 2%, CMSN 2%
#
# La asignacion se realiza via un valor random en [0,1) mapeado a los rangos acumulados.
# ==============================================================================

# Definicion de tipos de transaccion con sus umbrales acumulados (0-100)
# Umbrales: se suma el porcentaje de cada tipo en orden
# CATM: 0-15, DATM: 15-29, CMPR: 29-42, TINT: 42-52, DPST: 52-60
# PGSL: 60-67, TEXT: 67-73, RTRO: 73-78, PGSV: 78-83, NMNA: 83-87, INTR: 87-90
# ADSL: 90-93, IMPT: 93-96, DMCL: 96-98, CMSN: 98-100
tipos_trx         = ["CATM", "DATM", "CMPR", "TINT", "DPST", "PGSL", "TEXT", "RTRO", "PGSV", "NMNA", "INTR", "ADSL", "IMPT", "DMCL", "CMSN"]
umbrales_acum     = [15, 29, 42, 52, 60, 67, 73, 78, 83, 87, 90, 93, 96, 98, 100]

valores_trxcur    = ["USD", "EUR", "ILS", "EGP", "GBP"]
valores_trxst     = ["APPR", "DECL", "PEND", "REVS"]
valores_trxch     = ["ATM", "BRN", "ONL", "MOB", "POS"]
descripciones_trx = ["Retiro en cajero", "Deposito bancario", "Compra en comercio",
                     "Transferencia externa", "Pago de servicios", "Abono de nomina",
                     "Pago de prestamo", "Cargo por comision", "Reembolso", "Interes acreditado",
                     "Adelanto de efectivo", "Pago de impuesto", "Domiciliacion mensual",
                     "Operacion internacional", "Ajuste de cuenta"]

# Funcion para mapear random 0-100 a tipo de transaccion
# Se construye con CASE WHEN via expresion SQL
condicion_tipo_trx = (
    F.when(F.col("_rnd_tipo") < F.lit(15), F.lit("CATM"))
     .when(F.col("_rnd_tipo") < F.lit(29), F.lit("DATM"))
     .when(F.col("_rnd_tipo") < F.lit(42), F.lit("CMPR"))
     .when(F.col("_rnd_tipo") < F.lit(52), F.lit("TINT"))
     .when(F.col("_rnd_tipo") < F.lit(60), F.lit("DPST"))
     .when(F.col("_rnd_tipo") < F.lit(67), F.lit("PGSL"))
     .when(F.col("_rnd_tipo") < F.lit(73), F.lit("TEXT"))
     .when(F.col("_rnd_tipo") < F.lit(78), F.lit("RTRO"))
     .when(F.col("_rnd_tipo") < F.lit(83), F.lit("PGSV"))
     .when(F.col("_rnd_tipo") < F.lit(87), F.lit("NMNA"))
     .when(F.col("_rnd_tipo") < F.lit(90), F.lit("INTR"))
     .when(F.col("_rnd_tipo") < F.lit(93), F.lit("ADSL"))
     .when(F.col("_rnd_tipo") < F.lit(96), F.lit("IMPT"))
     .when(F.col("_rnd_tipo") < F.lit(98), F.lit("DMCL"))
     .otherwise(F.lit("CMSN"))
)

# Funcion para generar fecha aleatoria en rango via epoch
def dias_a_fecha_trx(anio_inicio, anio_fin, semilla_str):
    dias_inicio = int((date(anio_inicio, 1, 1) - date(1970, 1, 1)).days)
    dias_fin    = int((date(anio_fin, 12, 31)  - date(1970, 1, 1)).days)
    dias_rango  = dias_fin - dias_inicio
    epoch_col   = (F.lit(dias_inicio) + (F.abs(F.hash(F.col("id").cast("string"), F.lit(semilla_str)).cast("long")) % F.lit(dias_rango))).cast("int")
    return F.date_add(F.lit("1970-01-01").cast(DateType()), epoch_col)

# Generar DataFrame base
df_base_trx = spark.range(1, cantidad_transacciones + 1)

# Agregar valor random 0-100 para asignacion de tipo de transaccion
df_base_trx = df_base_trx.withColumn(
    "_rnd_tipo",
    (F.abs(F.hash(F.col("id").cast("string"), F.lit("tipo_rnd")).cast("long")) % 100).cast("integer")
)

# Asignar tipo de transaccion
df_base_trx = df_base_trx.withColumn("TRXTYP", condicion_tipo_trx)

# Asignar CUSTID valido del Maestro via modulo sobre total_clientes
# Se usa el hash del id para una distribucion uniforme de CUSTIDs
df_base_trx = df_base_trx.withColumn(
    "_custid_idx",
    (F.abs(F.hash(F.col("id").cast("string"), F.lit("custid_rnd")).cast("long")) % F.lit(total_clientes)).cast("long")
)

# Join con CUSTIDs validos: se agrega un row_number al df_custids para usar como indice
from pyspark.sql import Window as W
df_custids_con_idx = df_custids_validos.withColumn(
    "_idx",
    (F.row_number().over(W.orderBy("CUSTID")) - 1).cast("long")
)

df_base_trx = df_base_trx.join(
    df_custids_con_idx,
    df_base_trx["_custid_idx"] == df_custids_con_idx["_idx"],
    how="left"
).drop("_custid_idx", "_idx")

# Construir TRXID: prefijo tipo (4 chars) + secuencial (8 digitos zero-padded)
df_base_trx = df_base_trx.withColumn(
    "TRXID",
    F.concat(F.col("TRXTYP"), F.lpad(F.col("id").cast("string"), 8, "0"))
)

# Numero secuencial de transaccion
df_base_trx = df_base_trx.withColumn("TRXSQ", F.col("id").cast(LongType()))

# Descripcion de transaccion
arr_descs = F.array([F.lit(d) for d in descripciones_trx])
df_base_trx = df_base_trx.withColumn(
    "TRXDSC",
    F.element_at(arr_descs, (F.abs(F.hash(F.col("id").cast("string"), F.lit("dsc")).cast("long")) % len(descripciones_trx) + 1).cast("integer"))
)

# Construir las 60 columnas TRXPFL
arr_trxcur = F.array([F.lit(v) for v in valores_trxcur])
arr_trxst  = F.array([F.lit(v) for v in valores_trxst])
arr_trxch  = F.array([F.lit(v) for v in valores_trxch])

def arr_elem_trx(arr_python, arr_spark, semilla):
    return F.element_at(arr_spark, (F.abs(F.hash(F.col("id").cast("string"), F.lit(semilla)).cast("long")) % len(arr_python) + 1).cast("integer"))

df_trxpfl = (
    df_base_trx
    # Columna 1: TRXID (ya construido)
    # Columna 2: CUSTID (ya asignado del maestro)
    # Columna 3: TRXTYP (ya asignado)
    # Columna 4: TRXAMT — monto entre montoMinimo y montoMaximo
    .withColumn("TRXAMT", (F.lit(monto_minimo) + F.rand(seed=33) * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    # Columna 5: TRXCUR
    .withColumn("TRXCUR", arr_elem_trx(valores_trxcur, arr_trxcur, "cur"))
    # Columna 6: TRXST
    .withColumn("TRXST",  arr_elem_trx(valores_trxst,  arr_trxst,  "st"))
    # Columna 7: TRXCH
    .withColumn("TRXCH",  arr_elem_trx(valores_trxch,  arr_trxch,  "ch"))
    # Columna 8: TRXDSC (ya asignado)
    # Columna 9: TRXREF — referencia externa
    .withColumn("TRXREF", F.concat(F.lit("EXT"), F.lpad(F.col("id").cast("string"), 10, "0")))
    # Columna 10: TRXSQ (ya asignado)
    # Columnas 11-29: 19 fechas DateType
    # TRXDT: fecha de transaccion = parametro de entrada
    .withColumn("TRXDT",  F.lit(fecha_transaccion).cast(DateType()))
    # Demas fechas en rango 2005-2025
    .withColumn("TRXVD",  dias_a_fecha_trx(2005, 2025, "vd"))
    .withColumn("TRXPD",  dias_a_fecha_trx(2005, 2025, "pd"))
    .withColumn("TRXSD",  dias_a_fecha_trx(2005, 2025, "sd"))
    .withColumn("TRXCD",  dias_a_fecha_trx(2005, 2025, "cd"))
    .withColumn("TRXED",  dias_a_fecha_trx(2005, 2025, "ed"))
    .withColumn("TRXRD",  dias_a_fecha_trx(2005, 2025, "rd"))
    .withColumn("TRXAD",  dias_a_fecha_trx(2005, 2025, "ad"))
    .withColumn("TRXND",  dias_a_fecha_trx(2005, 2025, "nd"))
    .withColumn("TRXXD",  dias_a_fecha_trx(2005, 2025, "xd"))
    .withColumn("TRXFD",  dias_a_fecha_trx(2005, 2025, "fd"))
    .withColumn("TRXGD",  dias_a_fecha_trx(2005, 2025, "gd"))
    .withColumn("TRXHD",  dias_a_fecha_trx(2005, 2025, "hd"))
    .withColumn("TRXBD",  dias_a_fecha_trx(2005, 2025, "bdt"))
    .withColumn("TRXMD",  dias_a_fecha_trx(2005, 2025, "md"))
    .withColumn("TRXLD",  dias_a_fecha_trx(2005, 2025, "ld"))
    .withColumn("TRXUD",  dias_a_fecha_trx(2005, 2025, "ud"))
    .withColumn("TRXOD",  dias_a_fecha_trx(2005, 2025, "od"))
    .withColumn("TRXKD",  dias_a_fecha_trx(2005, 2025, "kd"))
    # Columnas 30-31: 2 TimestampType
    .withColumn("TRXTS",  F.to_timestamp(F.concat(dias_a_fecha_trx(2005, 2025, "ts").cast("string"), F.lit(" 00:00:00"))))
    .withColumn("TRXUS",  F.to_timestamp(F.concat(dias_a_fecha_trx(2005, 2025, "us").cast("string"), F.lit(" 12:00:00"))))
    # Columnas 32-60: 30 DoubleType — montos entre montoMinimo y montoMaximo
    .withColumn("TRXBA", (F.lit(monto_minimo) + F.rand(seed=51)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXBP", (F.lit(monto_minimo) + F.rand(seed=52)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXCM", (F.lit(monto_minimo) + F.rand(seed=53)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXIM", (F.lit(monto_minimo) + F.rand(seed=54)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXNT", (F.col("TRXAMT") - F.col("TRXCM") - F.col("TRXIM")).cast(DoubleType()))
    .withColumn("TRXTC", (F.lit(0.5) + F.rand(seed=55) * F.lit(4.5)).cast(DoubleType()))
    .withColumn("TRXAO", (F.lit(monto_minimo) + F.rand(seed=56)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXAL", (F.lit(monto_minimo) + F.rand(seed=57)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXIN", (F.lit(monto_minimo) + F.rand(seed=58)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXPN", (F.lit(monto_minimo) + F.rand(seed=59)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXDS", (F.lit(monto_minimo) + F.rand(seed=60)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXBF", (F.lit(monto_minimo) + F.rand(seed=61)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXPT", (F.lit(monto_minimo) + F.rand(seed=62)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXRL", (F.lit(0.0)          + F.rand(seed=63)  * F.lit(0.25)).cast(DoubleType()))
    .withColumn("TRXMX", (F.lit(monto_maximo) * F.lit(1.1)).cast(DoubleType()))
    .withColumn("TRXMN", F.lit(monto_minimo).cast(DoubleType()))
    .withColumn("TRXAV", (F.lit(monto_minimo) + F.rand(seed=66)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXDV", (F.col("TRXAMT") - F.col("TRXAV")).cast(DoubleType()))
    .withColumn("TRXRK", (F.rand(seed=68) * F.lit(100.0)).cast(DoubleType()))
    .withColumn("TRXFR", (F.rand(seed=69) * F.lit(100.0)).cast(DoubleType()))
    .withColumn("TRXLM", (F.lit(monto_minimo) + F.rand(seed=70)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("TRXLP", (F.col("TRXLM") - F.col("TRXAMT")).cast(DoubleType()))
    .withColumn("TRXCP", (F.lit(monto_minimo) + F.rand(seed=72)  * F.lit(monto_maximo * 0.01)).cast(DoubleType()))
    .withColumn("TRXCI", (F.lit(monto_minimo) + F.rand(seed=73)  * F.lit(monto_maximo * 0.01)).cast(DoubleType()))
    .withColumn("TRXCF", (F.lit(monto_minimo) + F.rand(seed=74)  * F.lit(monto_maximo * 0.005)).cast(DoubleType()))
    .withColumn("TRXCV", (F.lit(monto_minimo) + F.rand(seed=75)  * F.lit(monto_maximo * 0.02)).cast(DoubleType()))
    .withColumn("TRXSB", (F.col("TRXAMT") - F.col("TRXCM")).cast(DoubleType()))
    .withColumn("TRXTL", (F.col("TRXAMT") + F.col("TRXCP") + F.col("TRXCI")).cast(DoubleType()))
    .withColumn("TRXRS", (F.lit(0.0) + F.rand(seed=78) * F.lit(monto_maximo * 0.1)).cast(DoubleType()))
    # Eliminar columnas auxiliares
    .drop("id", "_rnd_tipo")
    # Seleccionar columnas en orden del contrato
    .select(
        "TRXID", "CUSTID", "TRXTYP", "TRXAMT", "TRXCUR", "TRXST", "TRXCH", "TRXDSC", "TRXREF", "TRXSQ",
        "TRXDT", "TRXVD", "TRXPD", "TRXSD", "TRXCD", "TRXED", "TRXRD", "TRXAD", "TRXND", "TRXXD",
        "TRXFD", "TRXGD", "TRXHD", "TRXBD", "TRXMD", "TRXLD", "TRXUD", "TRXOD", "TRXKD",
        "TRXTS", "TRXUS",
        "TRXBA", "TRXBP", "TRXCM", "TRXIM", "TRXNT", "TRXTC", "TRXAO", "TRXAL", "TRXIN", "TRXPN",
        "TRXDS", "TRXBF", "TRXPT", "TRXRL", "TRXMX", "TRXMN", "TRXAV", "TRXDV", "TRXRK", "TRXFR",
        "TRXLM", "TRXLP", "TRXCP", "TRXCI", "TRXCF", "TRXCV", "TRXSB", "TRXTL", "TRXRS"
    )
)

print(f"DataFrame TRXPFL generado con {df_trxpfl.count()} registros y {len(df_trxpfl.columns)} columnas")

# Mostrar distribucion de tipos de transaccion
print("Distribucion de tipos de transaccion generada:")
df_trxpfl.groupBy("TRXTYP").count().orderBy(F.desc("count")).show(20, truncate=False)

# COMMAND ----------

# ==============================================================================
# PASO 10: ESCRITURA DEL PARQUET
# ==============================================================================

tiempo_inicio_escritura = time.time()
print(f"Escribiendo parquet en: {ruta_completa_transac}")

df_trxpfl.coalesce(numero_particiones).write.mode("overwrite").parquet(ruta_completa_transac)

tiempo_fin_escritura = time.time()
tiempo_escritura     = round(tiempo_fin_escritura - tiempo_inicio_escritura, 3)

# COMMAND ----------

# ==============================================================================
# PASO 11: BLOQUE DE OBSERVABILIDAD FINAL
# ==============================================================================

tiempo_fin_total   = time.time()
tiempo_total       = round(tiempo_fin_total - tiempo_inicio_total, 3)
registros_escritos = spark.read.parquet(ruta_completa_transac).count()

print("=" * 70)
print("FIN: NbGenerarTransaccionalCliente (TRXPFL)")
print("=" * 70)
print(f"  Registros escritos    : {registros_escritos}")
print(f"  Columnas              : {len(df_trxpfl.columns)}")
print(f"  Ruta destino          : {ruta_completa_transac}")
print(f"  Tiempo escritura      : {tiempo_escritura}s")
print(f"  Tiempo total          : {tiempo_total}s")
print("=" * 70)
