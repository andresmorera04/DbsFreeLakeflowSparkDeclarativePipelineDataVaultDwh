# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbGenerarSaldosCliente
# MAGIC
# MAGIC **Proposito**: Genera un archivo parquet simulando los saldos bancarios del
# MAGIC sistema AS400 (BLNCFL). El parquet contiene 100 columnas con distribucion de
# MAGIC tipos: 2 LongType, 29 StringType, 34 DoubleType, 35 DateType.
# MAGIC
# MAGIC **Relacion 1:1 con CMSTFL**: Genera exactamente 1 registro por cada CUSTID del
# MAGIC Maestro de Clientes. La cantidad de registros se determina automaticamente.
# MAGIC
# MAGIC **Distribucion de tipos de cuenta**:
# MAGIC - AHRO (Ahorro): 40%
# MAGIC - CRTE (Corriente): 30%
# MAGIC - PRES (Prestamo): 20%
# MAGIC - INVR (Inversion): 10%
# MAGIC
# MAGIC **Prerequisitos**:
# MAGIC - Unity Catalog activo con tabla Parametros y parquet CMSTFL generado

# COMMAND ----------

# ==============================================================================
# PASO 1: IMPORTS Y CONFIGURACION INICIAL
# ==============================================================================

import time
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, LongType, DoubleType, DateType
)

# COMMAND ----------

# ==============================================================================
# PASO 2: DEFINICION DE WIDGETS (PARAMETROS DE ENTRADA)
# ==============================================================================
# 9 widgets segun contrato.
# ==============================================================================

dbutils.widgets.text("catalogoParametro",          "control",                         "Catalogo de la tabla Parametros")
dbutils.widgets.text("esquemaParametro",           "lab1",                        "Esquema de la tabla Parametros")
dbutils.widgets.text("tablaParametros",            "Parametros",                      "Nombre de la tabla Parametros")
dbutils.widgets.text("rutaRelativaSaldoCliente",   "LSDP_Base/As400/SaldoCliente/",   "Ruta relativa destino del parquet de saldos")
dbutils.widgets.text("rutaRelativaMaestroCliente", "LSDP_Base/As400/MaestroCliente/", "Ruta relativa del parquet CMSTFL existente")
dbutils.widgets.text("montoMinimo",                "10",                              "Monto minimo para columnas DoubleType")
dbutils.widgets.text("montoMaximo",                "100000",                          "Monto maximo para columnas DoubleType")
dbutils.widgets.text("numeroParticiones",          "8",                               "Numero de particiones para coalesce")
dbutils.widgets.text("shufflePartitions",          "8",                               "Valor de spark.sql.shuffle.partitions")

# Captura de valores
catalogo_parametro     = dbutils.widgets.get("catalogoParametro")
esquema_parametro      = dbutils.widgets.get("esquemaParametro")
tabla_parametros       = dbutils.widgets.get("tablaParametros")
ruta_relativa_saldo    = dbutils.widgets.get("rutaRelativaSaldoCliente")
ruta_relativa_maestro  = dbutils.widgets.get("rutaRelativaMaestroCliente")
monto_minimo_str       = dbutils.widgets.get("montoMinimo")
monto_maximo_str       = dbutils.widgets.get("montoMaximo")
numero_particiones_str = dbutils.widgets.get("numeroParticiones")
shuffle_partitions_str = dbutils.widgets.get("shufflePartitions")

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
    "rutaRelativaSaldoCliente":  ruta_relativa_saldo,
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
    catalogo_volume       = dict_parametros.get("catalogoVolume", "")
    esquema_volume        = dict_parametros.get("esquemaVolume", "")
    nombre_volume         = dict_parametros.get("nombreVolume", "")
    ruta_completa_saldo   = f"/Volumes/{catalogo_volume}/{esquema_volume}/{nombre_volume}/{ruta_relativa_saldo}"
    ruta_completa_maestro = f"/Volumes/{catalogo_volume}/{esquema_volume}/{nombre_volume}/{ruta_relativa_maestro}"
elif tipo_storage == "AmazonS3":
    bucket_s3             = dict_parametros.get("bucketS3", "")
    ruta_completa_saldo   = f"s3://{bucket_s3}/{ruta_relativa_saldo}"
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
print("INICIO: NbGenerarSaldosCliente (BLNCFL)")
print("=" * 70)
print("--- Parametros Widgets ---")
print(f"  catalogoParametro          : {catalogo_parametro}")
print(f"  esquemaParametro           : {esquema_parametro}")
print(f"  tablaParametros            : {tabla_parametros}")
print(f"  rutaRelativaSaldoCliente   : {ruta_relativa_saldo}")
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
print(f"  Ruta destino saldos   : {ruta_completa_saldo}")
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

# Obtener CUSTIDs unicos en orden para asignacion 1:1 (distinct evita duplicados
# cuando la ruta apunta a una carpeta padre con multiples particiones por dia)
df_custids = df_maestro.select("CUSTID").distinct().orderBy("CUSTID")
total_clientes = df_custids.count()
print(f"Maestro de Clientes cargado: {total_clientes} CUSTIDs unicos")
print(f"Se generaran {total_clientes} saldos (relacion 1:1 con CMSTFL)")

# COMMAND ----------

# ==============================================================================
# PASO 9: GENERACION DE DATOS BLNCFL (100 COLUMNAS)
# ==============================================================================
# Distribucion de tipos de cuenta:
#   AHRO 40%, CRTE 30%, PRES 20%, INVR 10%
# Relacion 1:1 con CMSTFL: un registro por CUSTID.
# ==============================================================================

# Funcion para generar fecha aleatoria en rango via epoch
def dias_a_fecha_bl(anio_inicio, anio_fin, semilla_str):
    dias_inicio = int((date(anio_inicio, 1, 1) - date(1970, 1, 1)).days)
    dias_fin    = int((date(anio_fin, 12, 31)  - date(1970, 1, 1)).days)
    dias_rango  = dias_fin - dias_inicio
    epoch_col   = (F.lit(dias_inicio) + (F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit(semilla_str)).cast("long")) % F.lit(dias_rango))).cast("int")
    return F.date_add(F.lit("1970-01-01").cast(DateType()), epoch_col)

# Tipo de cuenta segun distribucion acumulada
# AHRO: 0-40, CRTE: 40-70, PRES: 70-90, INVR: 90-100
condicion_tipo_cuenta = (
    F.when(F.col("_rnd_cuenta") < F.lit(40), F.lit("AHRO"))
     .when(F.col("_rnd_cuenta") < F.lit(70), F.lit("CRTE"))
     .when(F.col("_rnd_cuenta") < F.lit(90), F.lit("PRES"))
     .otherwise(F.lit("INVR"))
)

# Listas de valores categoricos para BLNCFL
valores_blcur = ["USD", "EUR", "ILS", "EGP", "GBP"]
valores_blst  = ["ACTV", "INAC", "SUSP", "CERR"]
valores_blrk  = ["LOW", "MED", "HIG"]
valores_bltp  = ["PRI", "SEC", "AUT"]
valores_bllc  = ["ATM", "BRN", "ONL", "MOB"]
valores_blky  = ["COMP", "PEND"]
valores_yn    = ["Y", "N"]
valores_blfc  = ["MEN", "TRI", "SEM", "ANU"]

sucursales_bl = ["BRN001", "BRN002", "BRN003", "BRN004", "BRN005"]
productos_bl  = ["PROD-AHORRO-STD", "PROD-CRED-PREM", "PROD-PREST-PEQ",
                 "PROD-INV-CONS", "PROD-CTA-EMP"]
subproductos_bl = ["SUB-01", "SUB-02", "SUB-03", "SUB-04"]
clasificac_bl   = ["CLF01", "CLF02", "CLF03", "CLF04"]
grupos_bl       = ["GRP-AHORRO", "GRP-CREDITO", "GRP-INVERSION", "GRP-NOMINA"]
planes_bl       = ["PLAN-A", "PLAN-B", "PLAN-C", "PLAN-D"]
regiones_bl     = ["Norte", "Sur", "Centro", "Este", "Oeste"]
gerentes_bl     = ["MGR001", "MGR002", "MGR003", "MGR004", "MGR005"]
nombres_cuenta  = ["Mi Cuenta Principal", "Cuenta Empresarial", "Cuenta Nomina",
                   "Cuenta Inversion", "Caja de Ahorro", "Prestamo Personal",
                   "Cuenta Corriente", "Cuenta Premium"]
centros_costo   = ["CC001", "CC002", "CC003", "CC004"]
referencias_bl  = ["REF-BL-001", "REF-BL-002", "REF-BL-003", "REF-BL-004"]
codigos_freq    = ["MEN", "TRI", "SEM", "ANU"]

def arr_elem_bl(arr_python, semilla):
    arr_spark = F.array([F.lit(v) for v in arr_python])
    return F.element_at(arr_spark, (F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit(semilla)).cast("long")) % len(arr_python) + 1).cast("integer"))

# Agregar row_number a df_custids para usar como BLSQ
from pyspark.sql import Window as W
df_base_bl = df_custids.withColumn("BLSQ", F.monotonically_increasing_id() + 1)

# Asignar tipo de cuenta
df_base_bl = df_base_bl.withColumn(
    "_rnd_cuenta",
    (F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit("cuenta_rnd")).cast("long")) % 100).cast("integer")
)
df_base_bl = df_base_bl.withColumn("BLACT", condicion_tipo_cuenta)

# Construir las 100 columnas BLNCFL
df_blncfl = (
    df_base_bl
    # Columna 1: CUSTID (ya presente)
    # Columna 2: BLSQ (ya presente)
    # Columna 3: BLACT (ya presente)
    # Columna 4: numero de cuenta
    .withColumn("BLACN", F.concat(F.lit("ACC"), F.lpad(F.col("CUSTID").cast("string"), 10, "0")))
    # Columna 5: moneda
    .withColumn("BLCUR", arr_elem_bl(valores_blcur, "cur"))
    # Columna 6: estado
    .withColumn("BLST",  arr_elem_bl(valores_blst,  "st"))
    # Columna 7: sucursal
    .withColumn("BLBR",  arr_elem_bl(sucursales_bl, "br"))
    # Columna 8: producto
    .withColumn("BLPR",  arr_elem_bl(productos_bl,  "pr"))
    # Columna 9: subproducto
    .withColumn("BLSP",  arr_elem_bl(subproductos_bl, "sp"))
    # Columna 10: nombre de la cuenta
    .withColumn("BLNM",  arr_elem_bl(nombres_cuenta, "nm"))
    # Columna 11: clasificacion
    .withColumn("BLCL",  arr_elem_bl(clasificac_bl,  "cl"))
    # Columna 12: nivel de riesgo
    .withColumn("BLRK",  arr_elem_bl(valores_blrk,   "rk"))
    # Columna 13: tipo de titular
    .withColumn("BLTP",  arr_elem_bl(valores_bltp,   "tp"))
    # Columna 14: gerente asignado
    .withColumn("BLMG",  arr_elem_bl(gerentes_bl,    "mg"))
    # Columna 15: referencia
    .withColumn("BLRF",  arr_elem_bl(referencias_bl, "rf"))
    # Columna 16: centro de costo
    .withColumn("BLCC",  arr_elem_bl(centros_costo,  "cc"))
    # Columna 17: grupo de afinidad
    .withColumn("BLAG",  arr_elem_bl(grupos_bl,      "ag"))
    # Columna 18: plan
    .withColumn("BLPL",  arr_elem_bl(planes_bl,      "pl"))
    # Columna 19: region
    .withColumn("BLRG",  arr_elem_bl(regiones_bl,    "rg"))
    # Columna 20: sufijo
    .withColumn("BLSF",  F.lpad((F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit("sf")).cast("long")) % 99 + 1).cast("string"), 2, "0"))
    # Columna 21: notas
    .withColumn("BLNT",  F.concat(F.lit("NOTA-BL-"), F.col("CUSTID").cast("string")))
    # Columna 22: ultimo canal
    .withColumn("BLLC",  arr_elem_bl(valores_bllc,   "lc"))
    # Columnas 23-30: indicadores Y/N y estados
    .withColumn("BLPF",  arr_elem_bl(valores_yn,     "pf"))
    .withColumn("BLAU",  arr_elem_bl(valores_yn,     "au"))
    .withColumn("BLTX",  arr_elem_bl(valores_yn,     "tx"))
    .withColumn("BLGR",  arr_elem_bl(valores_yn,     "gr"))
    .withColumn("BLEM",  arr_elem_bl(valores_yn,     "em"))
    .withColumn("BLFR",  arr_elem_bl(valores_yn,     "fr"))
    .withColumn("BLKY",  arr_elem_bl(valores_blky,   "ky"))
    .withColumn("BLVP",  arr_elem_bl(valores_yn,     "vp"))
    # Columna 31: frecuencia estado de cuenta
    .withColumn("BLFC",  arr_elem_bl(codigos_freq,   "fc"))
    # Columnas 32-65: 34 DoubleType — saldos y montos entre montoMinimo y montoMaximo
    .withColumn("BLAV",  (F.lit(monto_minimo) + F.rand(seed=201) * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("BLTB",  (F.lit(monto_minimo) + F.rand(seed=202) * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("BLRV",  (F.lit(monto_minimo) + F.rand(seed=203) * F.lit(monto_maximo * 0.2)).cast(DoubleType()))
    .withColumn("BLBK",  (F.lit(monto_minimo) + F.rand(seed=204) * F.lit(monto_maximo * 0.1)).cast(DoubleType()))
    .withColumn("BLMN",  F.lit(monto_minimo).cast(DoubleType()))
    .withColumn("BLMX",  F.lit(monto_maximo).cast(DoubleType()))
    .withColumn("BLIR",  (F.rand(seed=207) * F.lit(0.25)).cast(DoubleType()))
    .withColumn("BLPM",  (F.rand(seed=208) * F.lit(0.15)).cast(DoubleType()))
    .withColumn("BLCR",  (F.lit(monto_minimo) + F.rand(seed=209) * F.lit(monto_maximo * 2)).cast(DoubleType()))
    .withColumn("BLCU",  (F.lit(monto_minimo) + F.rand(seed=210) * F.lit(monto_maximo)).cast(DoubleType()))
    .withColumn("BLCD",  (F.col("BLCR") - F.col("BLCU")).cast(DoubleType()))
    .withColumn("BLOV",  (F.rand(seed=212) * F.lit(monto_maximo * 0.05)).cast(DoubleType()))
    .withColumn("BLOL",  (F.lit(monto_minimo) + F.rand(seed=213) * F.lit(monto_maximo * 0.1)).cast(DoubleType()))
    .withColumn("BLPD",  (F.lit(monto_minimo) + F.rand(seed=214) * F.lit(monto_maximo * 0.05)).cast(DoubleType()))
    .withColumn("BLPC",  (F.lit(monto_minimo) + F.rand(seed=215) * F.lit(monto_maximo * 0.05)).cast(DoubleType()))
    .withColumn("BLPA",  (F.lit(monto_minimo) + F.rand(seed=216) * F.lit(monto_maximo * 0.5)).cast(DoubleType()))
    .withColumn("BLDI",  (F.lit(monto_minimo) + F.rand(seed=217) * F.lit(monto_maximo)).cast(DoubleType()))
    .withColumn("BLWI",  (F.lit(monto_minimo) + F.rand(seed=218) * F.lit(monto_maximo)).cast(DoubleType()))
    .withColumn("BLTI",  (F.lit(monto_minimo) + F.rand(seed=219) * F.lit(monto_maximo * 0.1)).cast(DoubleType()))
    .withColumn("BLTC",  (F.lit(monto_minimo) + F.rand(seed=220) * F.lit(monto_maximo * 0.02)).cast(DoubleType()))
    .withColumn("BLCA",  (F.lit(monto_minimo) + F.rand(seed=221) * F.lit(monto_maximo * 0.01)).cast(DoubleType()))
    .withColumn("BLIM",  (F.lit(monto_minimo) + F.rand(seed=222) * F.lit(monto_maximo * 0.03)).cast(DoubleType()))
    .withColumn("BLRF2", (F.lit(monto_minimo) + F.rand(seed=223) * F.lit(monto_maximo)).cast(DoubleType()))
    .withColumn("BLPN",  (F.rand(seed=224) * F.lit(monto_maximo * 0.02)).cast(DoubleType()))
    .withColumn("BLBN",  (F.rand(seed=225) * F.lit(monto_maximo * 0.01)).cast(DoubleType()))
    .withColumn("BLAP",  (F.lit(monto_minimo) + F.rand(seed=226) * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("BLAM",  (F.lit(monto_minimo) + F.rand(seed=227) * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("BLAY",  (F.lit(monto_minimo) + F.rand(seed=228) * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("BLHI",  (F.lit(monto_minimo) + F.rand(seed=229) * F.lit(monto_maximo)).cast(DoubleType()))
    .withColumn("BLLO",  F.lit(monto_minimo).cast(DoubleType()))
    .withColumn("BLVR",  (F.rand(seed=231) * F.lit(monto_maximo * 0.1) - F.lit(monto_maximo * 0.05)).cast(DoubleType()))
    .withColumn("BLRT",  (F.rand(seed=232) * F.lit(0.20)).cast(DoubleType()))
    .withColumn("BLCP",  (F.lit(monto_minimo) + F.rand(seed=233) * F.lit(monto_maximo)).cast(DoubleType()))
    .withColumn("BLCI",  (F.rand(seed=234) * F.lit(monto_maximo * 0.1)).cast(DoubleType()))
    # Columnas 66-100: 35 DateType — fechas en rango 2005-2025
    .withColumn("BLOD",  dias_a_fecha_bl(2005, 2025, "od"))
    .withColumn("BLXD",  dias_a_fecha_bl(2005, 2025, "xd"))
    .withColumn("BLUD",  dias_a_fecha_bl(2005, 2025, "ud"))
    .withColumn("BLLD",  dias_a_fecha_bl(2005, 2025, "ld"))
    .withColumn("BLSD",  dias_a_fecha_bl(2005, 2025, "sd"))
    .withColumn("BLPD2", dias_a_fecha_bl(2005, 2025, "pd2"))
    .withColumn("BLRD",  dias_a_fecha_bl(2005, 2025, "rd"))
    .withColumn("BLMD",  dias_a_fecha_bl(2005, 2025, "md"))
    .withColumn("BLCD2", dias_a_fecha_bl(2005, 2025, "cd2"))
    .withColumn("BLBD",  dias_a_fecha_bl(2005, 2025, "bdt"))
    .withColumn("BLFD",  dias_a_fecha_bl(2005, 2025, "fd"))
    .withColumn("BLGD",  dias_a_fecha_bl(2005, 2025, "gd"))
    .withColumn("BLHD",  dias_a_fecha_bl(2005, 2025, "hd"))
    .withColumn("BLID",  dias_a_fecha_bl(2005, 2025, "id"))
    .withColumn("BLJD",  dias_a_fecha_bl(2005, 2025, "jd"))
    .withColumn("BLKD",  dias_a_fecha_bl(2005, 2025, "kd"))
    .withColumn("BLND",  dias_a_fecha_bl(2005, 2025, "nd"))
    .withColumn("BLTD",  dias_a_fecha_bl(2005, 2025, "td"))
    .withColumn("BLVD",  dias_a_fecha_bl(2005, 2025, "vd"))
    .withColumn("BLWD",  dias_a_fecha_bl(2005, 2025, "wd"))
    .withColumn("BLYD",  dias_a_fecha_bl(2005, 2025, "yd"))
    .withColumn("BLZD",  dias_a_fecha_bl(2005, 2025, "zd"))
    .withColumn("BLED",  dias_a_fecha_bl(2005, 2025, "eld"))
    .withColumn("BLAD2", dias_a_fecha_bl(2005, 2025, "ad2"))
    .withColumn("BLDD",  dias_a_fecha_bl(2005, 2025, "dd"))
    .withColumn("BLFP",  dias_a_fecha_bl(2005, 2025, "fp"))
    .withColumn("BLLP",  dias_a_fecha_bl(2005, 2025, "lp"))
    .withColumn("BLMP",  dias_a_fecha_bl(2005, 2025, "mp"))
    .withColumn("BLNP",  dias_a_fecha_bl(2005, 2025, "np"))
    .withColumn("BLOP",  dias_a_fecha_bl(2005, 2025, "op"))
    .withColumn("BLPP",  dias_a_fecha_bl(2005, 2025, "pp"))
    .withColumn("BLQP",  dias_a_fecha_bl(2005, 2025, "qp"))
    .withColumn("BLRP",  dias_a_fecha_bl(2005, 2025, "rp"))
    .withColumn("BLSP2", dias_a_fecha_bl(2005, 2025, "sp2"))
    .withColumn("BLTP2", dias_a_fecha_bl(2005, 2025, "tp2"))
    # Eliminar columnas auxiliares
    .drop("_rnd_cuenta")
    # Seleccionar columnas en orden del contrato
    .select(
        "CUSTID", "BLSQ",
        "BLACT", "BLACN", "BLCUR", "BLST", "BLBR", "BLPR", "BLSP", "BLNM", "BLCL",
        "BLRK", "BLTP", "BLMG", "BLRF", "BLCC", "BLAG", "BLPL", "BLRG", "BLSF", "BLNT",
        "BLLC", "BLPF", "BLAU", "BLTX", "BLGR", "BLEM", "BLFR", "BLKY", "BLVP", "BLFC",
        "BLAV", "BLTB", "BLRV", "BLBK", "BLMN", "BLMX", "BLIR", "BLPM", "BLCR", "BLCU",
        "BLCD", "BLOV", "BLOL", "BLPD", "BLPC", "BLPA", "BLDI", "BLWI", "BLTI", "BLTC",
        "BLCA", "BLIM", "BLRF2", "BLPN", "BLBN", "BLAP", "BLAM", "BLAY", "BLHI", "BLLO",
        "BLVR", "BLRT", "BLCP", "BLCI",
        "BLOD", "BLXD", "BLUD", "BLLD", "BLSD", "BLPD2", "BLRD", "BLMD", "BLCD2", "BLBD",
        "BLFD", "BLGD", "BLHD", "BLID", "BLJD", "BLKD", "BLND", "BLTD", "BLVD", "BLWD",
        "BLYD", "BLZD", "BLED", "BLAD2", "BLDD", "BLFP", "BLLP", "BLMP", "BLNP", "BLOP",
        "BLPP", "BLQP", "BLRP", "BLSP2", "BLTP2"
    )
)

print(f"DataFrame BLNCFL generado con {df_blncfl.count()} registros y {len(df_blncfl.columns)} columnas")

# Mostrar distribucion de tipos de cuenta
print("Distribucion de tipos de cuenta generada:")
df_blncfl.groupBy("BLACT").count().orderBy(F.desc("count")).show(truncate=False)

# COMMAND ----------

# ==============================================================================
# PASO 10: ESCRITURA DEL PARQUET
# ==============================================================================

tiempo_inicio_escritura = time.time()
print(f"Escribiendo parquet en: {ruta_completa_saldo}")

df_blncfl.coalesce(numero_particiones).write.mode("overwrite").parquet(ruta_completa_saldo)

tiempo_fin_escritura = time.time()
tiempo_escritura     = round(tiempo_fin_escritura - tiempo_inicio_escritura, 3)

# COMMAND ----------

# ==============================================================================
# PASO 11: BLOQUE DE OBSERVABILIDAD FINAL
# ==============================================================================

tiempo_fin_total   = time.time()
tiempo_total       = round(tiempo_fin_total - tiempo_inicio_total, 3)
registros_escritos = spark.read.parquet(ruta_completa_saldo).count()

print("=" * 70)
print("FIN: NbGenerarSaldosCliente (BLNCFL)")
print("=" * 70)
print(f"  Registros escritos    : {registros_escritos}")
print(f"  Columnas              : {len(df_blncfl.columns)}")
print(f"  Ruta destino          : {ruta_completa_saldo}")
print(f"  Tiempo escritura      : {tiempo_escritura}s")
print(f"  Tiempo total          : {tiempo_total}s")
print("=" * 70)
