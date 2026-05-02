# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbGenerarMaestroCliente
# MAGIC
# MAGIC **Proposito**: Genera un archivo parquet simulando la tabla maestra de clientes
# MAGIC del sistema AS400 bancario (CMSTFL). El parquet contiene 70 columnas con
# MAGIC distribucion de tipos: 41 StringType, 18 DateType, 9 LongType, 2 DoubleType.
# MAGIC
# MAGIC **Primera ejecucion** (`rutaMaestroClienteExistente` vacio): Genera `cantidadClientes`
# MAGIC registros con CUSTID secuencial (1..N) y nombres exclusivamente hebreos, egipcios
# MAGIC e ingleses.
# MAGIC
# MAGIC **Re-ejecucion** (`rutaMaestroClienteExistente` con ruta): Lee el parquet existente,
# MAGIC muta `porcentajeMutacion` de registros en los campos demograficos listados en
# MAGIC `camposMutacion`, agrega `porcentajeNuevos` de registros nuevos y escribe el
# MAGIC resultado consolidado con modo overwrite.
# MAGIC
# MAGIC **Prerequisitos**:
# MAGIC - Unity Catalog activo con tabla Parametros creada (NbConfiguracionInicial ejecutado)
# MAGIC - Databricks Free Edition con Serverless Compute

# COMMAND ----------

# ==============================================================================
# PASO 1: IMPORTS Y CONFIGURACION INICIAL
# ==============================================================================

import time
import re
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StringType, LongType, DoubleType, DateType
)

# COMMAND ----------

# ==============================================================================
# PASO 2: DEFINICION DE WIDGETS (PARAMETROS DE ENTRADA)
# ==============================================================================
# 13 widgets segun contrato. rutaMaestroClienteExistente puede estar vacio.
# ==============================================================================

dbutils.widgets.text("catalogoParametro",           "control",        "Catalogo de la tabla Parametros")
dbutils.widgets.text("esquemaParametro",            "lab1",       "Esquema de la tabla Parametros")
dbutils.widgets.text("tablaParametros",             "Parametros",     "Nombre de la tabla Parametros")
dbutils.widgets.text("cantidadClientes",            "50000",          "Cantidad de clientes a generar")
dbutils.widgets.text("rutaRelativaMaestroCliente",  "LSDP_Base/As400/MaestroCliente/", "Ruta relativa destino del parquet")
dbutils.widgets.text("rutaMaestroClienteExistente", "",               "Ruta del maestro existente (vacio = primera ejecucion)")
dbutils.widgets.text("porcentajeMutacion",          "0.20",           "Porcentaje de registros a mutar (0-1)")
dbutils.widgets.text("porcentajeNuevos",            "0.006",          "Porcentaje de registros nuevos a agregar (0-1)")
dbutils.widgets.text("camposMutacion",              "CUSNM,CUSLN,CUSMD,CUSFN,CUSAD,CUSA2,CUSCT,CUSST,CUSZP,CUSPH,CUSMB,CUSEM,CUSMS,CUSOC,CUSED", "Campos a mutar separados por coma")
dbutils.widgets.text("montoMinimo",                 "10",             "Monto minimo para columnas DoubleType")
dbutils.widgets.text("montoMaximo",                 "100000",         "Monto maximo para columnas DoubleType")
dbutils.widgets.text("numeroParticiones",           "8",              "Numero de particiones para coalesce")
dbutils.widgets.text("shufflePartitions",           "8",              "Valor de spark.sql.shuffle.partitions")

# Captura de valores
catalogo_parametro            = dbutils.widgets.get("catalogoParametro")
esquema_parametro             = dbutils.widgets.get("esquemaParametro")
tabla_parametros              = dbutils.widgets.get("tablaParametros")
cantidad_clientes_str         = dbutils.widgets.get("cantidadClientes")
ruta_relativa_maestro         = dbutils.widgets.get("rutaRelativaMaestroCliente")
ruta_maestro_existente        = dbutils.widgets.get("rutaMaestroClienteExistente")
porcentaje_mutacion_str       = dbutils.widgets.get("porcentajeMutacion")
porcentaje_nuevos_str         = dbutils.widgets.get("porcentajeNuevos")
campos_mutacion_str           = dbutils.widgets.get("camposMutacion")
monto_minimo_str              = dbutils.widgets.get("montoMinimo")
monto_maximo_str              = dbutils.widgets.get("montoMaximo")
numero_particiones_str        = dbutils.widgets.get("numeroParticiones")
shuffle_partitions_str        = dbutils.widgets.get("shufflePartitions")

# COMMAND ----------

# ==============================================================================
# PASO 3: CONFIGURACION DE SPARK
# ==============================================================================

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_str)

# COMMAND ----------

# ==============================================================================
# PASO 4: VALIDACION DE PARAMETROS WIDGETS
# ==============================================================================
# rutaMaestroClienteExistente es opcional (puede estar vacio).
# Todos los demas parametros son obligatorios.
# ==============================================================================

widgets_obligatorios = {
    "catalogoParametro":          catalogo_parametro,
    "esquemaParametro":           esquema_parametro,
    "tablaParametros":            tabla_parametros,
    "cantidadClientes":           cantidad_clientes_str,
    "rutaRelativaMaestroCliente": ruta_relativa_maestro,
    "porcentajeMutacion":         porcentaje_mutacion_str,
    "porcentajeNuevos":           porcentaje_nuevos_str,
    "camposMutacion":             campos_mutacion_str,
    "montoMinimo":                monto_minimo_str,
    "montoMaximo":                monto_maximo_str,
    "numeroParticiones":          numero_particiones_str,
    "shufflePartitions":          shuffle_partitions_str,
}

vacios = [k for k, v in widgets_obligatorios.items() if not v or not v.strip()]
if vacios:
    raise ValueError(
        "ERROR DE VALIDACION: Los siguientes parametros son obligatorios y no pueden "
        "estar vacios: " + ", ".join(vacios) + ". Proporcione un valor y vuelva a ejecutar."
    )

# Validar cantidadClientes es entero positivo
try:
    cantidad_clientes = int(cantidad_clientes_str)
    if cantidad_clientes <= 0:
        raise ValueError()
except ValueError:
    raise ValueError(
        f"ERROR: 'cantidadClientes' debe ser un entero positivo. Valor recibido: '{cantidad_clientes_str}'"
    )

# Validar porcentajeMutacion y porcentajeNuevos son numericos en (0, 1.0]
try:
    porcentaje_mutacion = float(porcentaje_mutacion_str)
    if not (0 < porcentaje_mutacion <= 1.0):
        raise ValueError()
except ValueError:
    raise ValueError(
        f"ERROR: 'porcentajeMutacion' debe ser un numero en rango (0, 1.0]. Valor recibido: '{porcentaje_mutacion_str}'"
    )

try:
    porcentaje_nuevos = float(porcentaje_nuevos_str)
    if not (0 < porcentaje_nuevos <= 1.0):
        raise ValueError()
except ValueError:
    raise ValueError(
        f"ERROR: 'porcentajeNuevos' debe ser un numero en rango (0, 1.0]. Valor recibido: '{porcentaje_nuevos_str}'"
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
campos_mutacion    = [c.strip() for c in campos_mutacion_str.split(",") if c.strip()]

print("Validacion de parametros widgets: CORRECTA")

# COMMAND ----------

# ==============================================================================
# PASO 5: LECTURA DE TABLA PARAMETROS
# ==============================================================================
# Lectura via spark.read.table() + collect() a diccionario Python.
# RF: lectura de tabla Parametros para obtener TipoStorage, catalogoVolume, etc.
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
# PASO 6: CONSTRUCCION DINAMICA DE RUTA
# ==============================================================================

if tipo_storage == "Volume":
    catalogo_volume = dict_parametros.get("catalogoVolume", "")
    esquema_volume  = dict_parametros.get("esquemaVolume", "")
    nombre_volume   = dict_parametros.get("nombreVolume", "")
    ruta_completa   = f"/Volumes/{catalogo_volume}/{esquema_volume}/{nombre_volume}/{ruta_relativa_maestro}"
elif tipo_storage == "AmazonS3":
    bucket_s3     = dict_parametros.get("bucketS3", "")
    ruta_completa = f"s3://{bucket_s3}/{ruta_relativa_maestro}"
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
print("INICIO: NbGenerarMaestroCliente (CMSTFL)")
print("=" * 70)
print("--- Parametros Widgets ---")
print(f"  catalogoParametro           : {catalogo_parametro}")
print(f"  esquemaParametro            : {esquema_parametro}")
print(f"  tablaParametros             : {tabla_parametros}")
print(f"  cantidadClientes            : {cantidad_clientes}")
print(f"  rutaRelativaMaestroCliente  : {ruta_relativa_maestro}")
print(f"  rutaMaestroClienteExistente : '{ruta_maestro_existente}' (vacio = primera ejecucion)")
print(f"  porcentajeMutacion          : {porcentaje_mutacion}")
print(f"  porcentajeNuevos            : {porcentaje_nuevos}")
print(f"  camposMutacion              : {campos_mutacion}")
print(f"  montoMinimo                 : {monto_minimo}")
print(f"  montoMaximo                 : {monto_maximo}")
print(f"  numeroParticiones           : {numero_particiones}")
print(f"  shufflePartitions           : {shuffle_partitions_str}")
print("--- Parametros Tabla Parametros ---")
for clave, valor in dict_parametros.items():
    print(f"  {clave:<30} : {valor}")
print("--- Storage ---")
print(f"  TipoStorage  : {tipo_storage}")
print(f"  Ruta destino : {ruta_completa}")
print("=" * 70)

# COMMAND ----------

# ==============================================================================
# PASO 8: MECANISMO FAKER CON FALLBACK A LISTAS ESTATICAS
# ==============================================================================
# Se intenta importar Faker con locales hebreo, arabe e ingles.
# Si no esta disponible, se usan listas estaticas embebidas (~100 nombres y
# ~80 apellidos por etnia). Se prohiben nombres y apellidos latinos.
# ==============================================================================

try:
    from faker import Faker
    faker_he = Faker("he_IL")
    faker_ar = Faker("ar_EG")
    faker_en = Faker(["en_US", "en_GB"])
    faker_disponible = True
    print("Faker disponible: usando locales he_IL, ar_EG, en_US/en_GB")
except ImportError:
    faker_disponible = False
    print("Faker no disponible: usando listas estaticas embebidas")

# Listas estaticas de respaldo — nombres exclusivamente hebreos, egipcios e ingleses
nombres_hebreos   = ["Avraham", "Yitzhak", "Yaakov", "Moshe", "Aharon", "David", "Shlomo", "Eliyahu",
                     "Yosef", "Binyamin", "Shimon", "Levi", "Yehuda", "Dan", "Naftali", "Gad", "Asher",
                     "Issachar", "Zevulun", "Reuven", "Miriam", "Devorah", "Rachel", "Leah", "Sarah",
                     "Rivkah", "Tamar", "Chana", "Naomi", "Ruth", "Esther", "Yael", "Abigail", "Batya",
                     "Tziporah", "Adina", "Ora", "Zahava", "Nehama", "Penina", "Tova", "Shira", "Noa",
                     "Atara", "Bracha", "Chaya", "Dina", "Einat", "Fayge", "Geula", "Hadas", "Ilana",
                     "Yoav", "Boaz", "Calev", "Hillel", "Ilan", "Nimrod", "Omri", "Pinchas", "Ronen",
                     "Sasson", "Tal", "Uri", "Vered", "Ziv", "Avi", "Barak", "Chen", "Doron", "Eyal",
                     "Fivel", "Gil", "Harel", "Ido", "Jonathan", "Kobi", "Lior", "Michael", "Noam",
                     "Ohad", "Peled", "Ran", "Sagi", "Tomer", "Uzi", "Yaniv", "Zohar", "Arie", "Bezalel",
                     "Carmel", "Dor", "Efrat", "Gal", "Haim", "Itzik", "Jameel", "Keren", "Limor", "Miri"]

apellidos_hebreos = ["Cohen", "Levi", "Mizrahi", "Peretz", "Katz", "Friedman", "Shapiro", "Goldstein",
                     "Dayan", "Ben-David", "Kaplan", "Schwartz", "Rosenberg", "Klein", "Weiss", "Stern",
                     "Barak", "Amir", "Golan", "Sharon", "Peres", "Rabin", "Meir", "Begin", "Natan",
                     "Levy", "Oren", "Har-Lev", "Ben-Ami", "Yitzhaki", "Avni", "Baruch", "Carmeli",
                     "Dagan", "Eldar", "Feldman", "Gross", "Halevi", "Israeli", "Jerbi", "Kaplinsky",
                     "Landau", "Mordechai", "Nahmias", "Ovadia", "Pinkus", "Regev", "Sagi", "Tzur",
                     "Uziel", "Varsano", "Weizman", "Yadin", "Zadok", "Abramowitz", "Blum", "Cnaani",
                     "Dror", "Eitan", "Furman", "Greenberg", "Haviv", "Inbar", "Jacobson", "Kedem",
                     "Lotan", "Manor", "Nimni", "Ofir", "Pisker", "Ronen", "Sandler", "Turjeman",
                     "Ulman", "Vilnai", "Weil", "Xanadu", "Yechiel", "Zussman", "Aloni", "Bitan"]

nombres_egipcios  = ["Ahmed", "Mohamed", "Mahmoud", "Ali", "Hassan", "Hussein", "Ibrahim", "Khalid",
                     "Omar", "Yusuf", "Amr", "Hossam", "Kareem", "Sherif", "Walid", "Tamer", "Samir",
                     "Nasser", "Adel", "Amir", "Mustafa", "Tariq", "Samy", "Ramadan", "Hamid",
                     "Fatima", "Nour", "Mona", "Hana", "Amira", "Aya", "Nadia", "Rania", "Doha",
                     "Mariam", "Salma", "Layla", "Heba", "Dina", "Eman", "Ghada", "Inas", "Basma",
                     "Samira", "Nermeen", "Lobna", "Rehab", "Sherine", "Yasmeen", "Zeinab", "Abeer",
                     "Asmaa", "Bothaina", "Carine", "Daliah", "Esraa", "Fadia", "Hadeer", "Iman",
                     "Joumana", "Kholoud", "Lina", "Maya", "Nesma", "Ola", "Passant", "Radwa",
                     "Safiya", "Taghreed", "Usama", "Victoire", "Wafaa", "Xenia", "Yasmine", "Zizi",
                     "Abdo", "Belal", "Chadi", "Diaa", "Emad", "Fouad", "Gamal", "Hazem", "Ihab",
                     "Jaber", "Kamal", "Lotfy", "Maher", "Nagi", "Osama", "Ragab", "Sobhi", "Tarek"]

apellidos_egipcios = ["Hassan", "Mohamed", "Ahmed", "Ali", "Ibrahim", "Khalil", "Salem", "Osman",
                      "Gaber", "Farid", "Mansour", "Naser", "Barakat", "Ramzy", "Yousef", "Zaki",
                      "Abdel-Fattah", "Abu-Bakr", "Badawi", "Darwish", "El-Sayed", "Fawzy", "Ghali",
                      "Hatem", "Ismail", "Jomaa", "Karim", "Lotfy", "Morsi", "Nagib", "Okasha",
                      "Pharaoh", "Qassem", "Ragab", "Saber", "Tantawi", "Ugur", "Wahby", "Yaseen",
                      "Zahran", "Abdallah", "Bahgat", "Chiha", "Diab", "Elsherif", "Farouk", "Gomaa",
                      "Hamdan", "Idris", "Jarrah", "Kamal", "Lasheen", "Moustafa", "Naguib", "Othman",
                      "Pasha", "Quasim", "Rashed", "Soliman", "Tawfik", "Wafi", "Amin", "Badr",
                      "Chalabi", "Dandarawi", "Eshaq", "Fouad", "Habib", "Qutb"]

nombres_ingleses  = ["James", "William", "Oliver", "Jack", "Harry", "George", "Noah", "Charlie",
                     "Jacob", "Alfie", "Freddie", "Archie", "Oscar", "Henry", "Leo", "Arthur",
                     "Thomas", "Edward", "Joshua", "Sebastian", "Alexander", "Benjamin", "Elijah",
                     "Lucas", "Mason", "Ethan", "Logan", "Liam", "Aiden", "Carter", "Wyatt",
                     "Amelia", "Olivia", "Isla", "Emily", "Ava", "Lily", "Sophia", "Isabella",
                     "Mia", "Poppy", "Grace", "Ella", "Charlotte", "Evie", "Abigail", "Elizabeth",
                     "Eleanor", "Chloe", "Emma", "Sophie", "Hannah", "Lucy", "Alice", "Imogen",
                     "Jessica", "Katie", "Laura", "Natalie", "Phoebe", "Rebecca", "Samantha",
                     "Victoria", "Zoe", "Amber", "Beth", "Clara", "Diana", "Elise", "Frances",
                     "Georgina", "Helena", "Iris", "Julia", "Katherine", "Lydia", "Margaret",
                     "Naomi", "Penelope", "Anna", "Harriet", "Beatrice", "Caroline", "Daisy",
                     "Eve", "Felicity", "Gwendolyn", "Hope", "Ivy", "Joan", "Kathleen", "Leah"]

apellidos_ingleses = ["Smith", "Jones", "Williams", "Taylor", "Brown", "Davies", "Evans", "Wilson",
                      "Thomas", "Roberts", "Johnson", "Lewis", "Walker", "Robinson", "Wood", "Thompson",
                      "White", "Watson", "Jackson", "Wright", "Green", "Harris", "Cooper", "King",
                      "Lee", "Martin", "Clarke", "James", "Morgan", "Hughes", "Edwards", "Hill",
                      "Moore", "Clark", "Harrison", "Scott", "Young", "Morris", "Hall", "Ward",
                      "Turner", "Campbell", "Mitchell", "Cook", "Carter", "Anderson", "Phillips",
                      "Nelson", "Parker", "Collins", "Stewart", "Rogers", "Murray", "Bennett",
                      "Gray", "Bailey", "Fisher", "Richardson", "Webb", "Marshall", "Dixon",
                      "Fox", "Gibson", "Holmes", "Hunter", "Kennedy", "Lawrence", "Matthews",
                      "Mills", "Porter", "Russell", "Shaw", "Simpson", "Spencer", "Price", "Reed"]

# Todos los nombres y apellidos disponibles para generacion aleatoria
todos_nombres   = nombres_hebreos + nombres_egipcios + nombres_ingleses
todos_apellidos = apellidos_hebreos + apellidos_egipcios + apellidos_ingleses

# COMMAND ----------

# ==============================================================================
# PASO 9: GENERACION DE DATOS CMSTFL (70 COLUMNAS)
# ==============================================================================
# Se usa spark.range(cantidadClientes) como base y funciones nativas PySpark.
# Se evita spark.sparkContext para compatibilidad con Databricks Serverless.
# ==============================================================================

# --- Listas de valores categoricos para columnas StringType ---
valores_cussx = ["M", "F"]
valores_custt = ["Mr", "Mrs", "Ms", "Dr"]
valores_custp = ["IND", "COR"]
valores_cussg = ["PREM", "STD", "BAS"]
valores_cusms = ["SNG", "MRD", "DIV", "WDW"]
valores_cused = ["PHD", "MST", "BSC", "HSC", "OTH"]
valores_cusdp = ["PASS", "NAID", "DRVL"]
valores_cuslg = ["HEB", "ARA", "ENG"]
valores_cuspc = ["EML", "SMS", "PHL", "MIL"]
valores_cusrk = ["LOW", "MED", "HIG", "CRT"]
valores_cusvp = ["Y", "N"]
valores_cuspf = ["Y", "N"]
valores_cuskt = ["COMP", "PEND", "EXPD"]
valores_cusfm = ["Y", "N"]
valores_cuslc = ["BRN", "ATM", "ONL", "MOB"]
valores_cusac = ["A", "I", "S"]

ciudades = ["Jerusalem", "Tel Aviv", "Haifa", "Cairo", "Alexandria", "Giza", "London", "Manchester",
            "Birmingham", "Liverpool", "Leeds", "Bristol", "Newcastle", "Edinburgh", "Glasgow", "Oxford"]

paises   = ["Israel", "Egypt", "United Kingdom", "USA", "Canada", "Australia"]
regiones = ["Norte", "Sur", "Centro", "Este", "Oeste", "Nordeste", "Noroeste", "Sureste"]
estados  = ["HA", "TA", "JE", "CA", "AL", "GI", "London", "Manchester", "Yorkshire", "Kent"]

ocupaciones  = ["Engineer", "Doctor", "Teacher", "Lawyer", "Accountant", "Manager", "Analyst",
                "Developer", "Nurse", "Architect", "Consultant", "Trader", "Director", "Officer"]

niveles_educ = ["PHD", "MST", "BSC", "HSC", "OTH"]
sucursales   = ["BRN001", "BRN002", "BRN003", "BRN004", "BRN005", "BRN006", "BRN007", "BRN008"]
gerentes     = ["MGR001", "MGR002", "MGR003", "MGR004", "MGR005"]
fuentes_ref  = ["WEB", "MOBILE", "BRANCH", "ATM", "REFERRAL", "SOCIAL"]
grupos_afin  = ["AHORRO", "INVERSION", "CREDITO", "NOMINA", "SEGURO"]
categorias   = ["CAT_A", "CAT_B", "CAT_C", "CAT_D"]
clasificac   = ["CLF01", "CLF02", "CLF03", "CLF04", "CLF05"]
codigos_cred = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC", "D"]

# Funcion auxiliar para crear expresion de elemento aleatorio de una lista Python
def col_lista(lista, seed_offset=0):
    """Selecciona un elemento aleatorio de la lista usando funciones nativas PySpark."""
    return F.element_at(
        F.array([F.lit(v) for v in lista]),
        (F.abs(F.hash(F.col("id").cast("string"), F.lit(str(seed_offset))).cast("long")) % len(lista) + 1).cast("integer")
    )

# Funcion para generar fecha aleatoria en un rango de anios
def col_fecha(anio_inicio, anio_fin, seed_offset=0):
    """Genera una fecha aleatoria en el rango [anio_inicio, anio_fin]."""
    dias_inicio = (date(anio_inicio, 1, 1) - date(1970, 1, 1)).days
    dias_fin    = (date(anio_fin,    12, 31) - date(1970, 1, 1)).days
    dias_rango  = dias_fin - dias_inicio
    epoch_dias  = dias_inicio + (F.abs(F.hash(F.col("id").cast("string"), F.lit(str(seed_offset))).cast("long")) % dias_rango)
    return F.expr(f"date_add(date '1970-01-01', cast({epoch_dias._jc.toString()} as int))")

# Generacion alternativa de fecha con hash seed variable como columna
def col_fecha_col(anio_inicio, anio_fin, seed_col_expr):
    dias_inicio = (date(anio_inicio, 1, 1) - date(1970, 1, 1)).days
    dias_fin    = (date(anio_fin,    12, 31) - date(1970, 1, 1)).days
    dias_rango  = dias_fin - dias_inicio
    epoch_dias  = F.lit(dias_inicio) + (F.abs(F.hash(seed_col_expr).cast("long")) % dias_rango)
    return F.expr("date_add(date '1970-01-01', 0)").cast(DateType())  # placeholder


# Para las fechas usamos una funcion mas directa con semillas distintas
def fecha_aleatoria(anio_inicio, anio_fin, semilla):
    dias_inicio = int((date(anio_inicio, 1, 1) - date(1970, 1, 1)).days)
    dias_fin    = int((date(anio_fin, 12, 31)  - date(1970, 1, 1)).days)
    dias_rango  = dias_fin - dias_inicio
    return F.to_date(
        (F.lit(dias_inicio) + (F.abs(F.hash(F.col("id").cast("string"), F.lit(semilla)).cast("long")) % F.lit(dias_rango)
        ).cast("long").cast("string").cast("long")
        ).cast("int").cast("string"),
        # Convertir dias desde epoch a fecha
    ).cast(DateType())


def dias_a_fecha(anio_inicio, anio_fin, semilla_str):
    """Genera DateType aleatorio en rango via epoch dias."""
    dias_inicio = int((date(anio_inicio, 1, 1) - date(1970, 1, 1)).days)
    dias_fin    = int((date(anio_fin, 12, 31)  - date(1970, 1, 1)).days)
    dias_rango  = dias_fin - dias_inicio
    epoch_col   = (F.lit(dias_inicio) + (F.abs(F.hash(F.col("id").cast("string"), F.lit(semilla_str)).cast("long")) % F.lit(dias_rango))).cast("int")
    return F.date_add(F.lit("1970-01-01").cast(DateType()), epoch_col)

# Generar el DataFrame base con spark.range
df_base = spark.range(1, cantidad_clientes + 1)

# Asignar etnia a cada registro para nombres coherentes (hebreo/egipcio/ingles)
df_base = df_base.withColumn(
    "_etnia",
    F.element_at(F.array(F.lit("HEB"), F.lit("EGY"), F.lit("ENG")),
                 (F.abs(F.hash(F.col("id").cast("string"), F.lit("etnia")).cast("long")) % 3 + 1).cast("integer"))
)

# Listas estaticas para columnas de nombres — se combina el indice con la etnia
df_base = df_base.withColumn("_idx_nombre",   (F.abs(F.hash(F.col("id").cast("string"), F.lit("nm")).cast("long")) % F.lit(len(todos_nombres))).cast("integer"))
df_base = df_base.withColumn("_idx_apellido", (F.abs(F.hash(F.col("id").cast("string"), F.lit("ln")).cast("long")) % F.lit(len(todos_apellidos))).cast("integer"))
df_base = df_base.withColumn("_idx_nombre2",  (F.abs(F.hash(F.col("id").cast("string"), F.lit("md")).cast("long")) % F.lit(len(todos_nombres))).cast("integer"))

# Arrays Spark para nombres
arr_nombres   = F.array([F.lit(n) for n in todos_nombres])
arr_apellidos = F.array([F.lit(a) for a in todos_apellidos])

df_base = df_base.withColumn("_cusnm_raw", F.element_at(arr_nombres,   F.col("_idx_nombre")   + 1))
df_base = df_base.withColumn("_cusln_raw", F.element_at(arr_apellidos, F.col("_idx_apellido") + 1))
df_base = df_base.withColumn("_cusmd_raw", F.element_at(arr_nombres,   F.col("_idx_nombre2")  + 1))

# Ciudades y paises
arr_ciudades = F.array([F.lit(c) for c in ciudades])
arr_paises   = F.array([F.lit(p) for p in paises])
arr_regiones = F.array([F.lit(r) for r in regiones])
arr_estados  = F.array([F.lit(s) for s in estados])
arr_ocup     = F.array([F.lit(o) for o in ocupaciones])
arr_sucursal = F.array([F.lit(s) for s in sucursales])
arr_gerentes = F.array([F.lit(g) for g in gerentes])
arr_fuentes  = F.array([F.lit(f) for f in fuentes_ref])
arr_afin     = F.array([F.lit(a) for a in grupos_afin])
arr_categ    = F.array([F.lit(c) for c in categorias])
arr_clasif   = F.array([F.lit(c) for c in clasificac])
arr_cred     = F.array([F.lit(c) for c in codigos_cred])

def arr_elem(arr_python, arr_spark, semilla):
    return F.element_at(arr_spark, (F.abs(F.hash(F.col("id").cast("string"), F.lit(semilla)).cast("long")) % len(arr_python) + 1).cast("integer"))

# Construir las 70 columnas
df_cmstfl = (
    df_base
    # Columna 1: CUSTID
    .withColumn("CUSTID", F.col("id").cast(LongType()))
    # Columnas 2-4: nombres
    .withColumn("CUSNM", F.col("_cusnm_raw"))
    .withColumn("CUSLN", F.col("_cusln_raw"))
    .withColumn("CUSMD", F.col("_cusmd_raw"))
    # Columna 5: nombre completo
    .withColumn("CUSFN", F.concat_ws(" ", F.col("CUSNM"), F.col("CUSMD"), F.col("CUSLN")))
    # Columnas 6-7
    .withColumn("CUSSX", arr_elem(valores_cussx, F.array([F.lit(v) for v in valores_cussx]), "sx"))
    .withColumn("CUSTT", arr_elem(valores_custt, F.array([F.lit(v) for v in valores_custt]), "tt"))
    # Columnas 8-12: direccion
    .withColumn("CUSAD", F.concat(F.lit("St. "), (F.abs(F.hash(F.col("id").cast("string"), F.lit("ad")).cast("long")) % 9999 + 1).cast("string")))
    .withColumn("CUSA2", F.concat(F.lit("Apt "),  (F.abs(F.hash(F.col("id").cast("string"), F.lit("a2")).cast("long")) % 999  + 1).cast("string")))
    .withColumn("CUSCT", arr_elem(ciudades,  arr_ciudades, "ct"))
    .withColumn("CUSST", arr_elem(estados,   arr_estados,  "st"))
    .withColumn("CUSZP", F.lpad((F.abs(F.hash(F.col("id").cast("string"), F.lit("zp")).cast("long")) % 99999 + 10000).cast("string"), 5, "0"))
    # Columna 13: pais
    .withColumn("CUSCN", arr_elem(paises, arr_paises, "cn"))
    # Columnas 14-16: contacto
    .withColumn("CUSPH", F.concat(F.lit("+1-"),  F.lpad((F.abs(F.hash(F.col("id").cast("string"), F.lit("ph")).cast("long")) % 9000000000 + 1000000000).cast("string"), 10, "0")))
    .withColumn("CUSMB", F.concat(F.lit("+1-"),  F.lpad((F.abs(F.hash(F.col("id").cast("string"), F.lit("mb")).cast("long")) % 9000000000 + 1000000000).cast("string"), 10, "0")))
    .withColumn("CUSEM", F.concat(F.lower(F.col("CUSNM")), F.lit("."), F.lower(F.col("CUSLN")), F.lit("@mail.com")))
    # Columnas 17-18: tipo y segmento
    .withColumn("CUSTP", arr_elem(valores_custp, F.array([F.lit(v) for v in valores_custp]), "tp"))
    .withColumn("CUSSG", arr_elem(valores_cussg, F.array([F.lit(v) for v in valores_cussg]), "sg"))
    # Columnas 19-21
    .withColumn("CUSMS", arr_elem(valores_cusms, F.array([F.lit(v) for v in valores_cusms]), "ms"))
    .withColumn("CUSOC", arr_elem(ocupaciones,   arr_ocup,                                   "oc"))
    .withColumn("CUSED", arr_elem(valores_cused, F.array([F.lit(v) for v in valores_cused]), "ed"))
    # Columnas 22-25
    .withColumn("CUSNA", arr_elem(paises,   arr_paises,   "na"))
    .withColumn("CUSDL", F.lpad((F.abs(F.hash(F.col("id").cast("string"), F.lit("dl")).cast("long")) % 999999999 + 100000000).cast("string"), 9, "0"))
    .withColumn("CUSDP", arr_elem(valores_cusdp, F.array([F.lit(v) for v in valores_cusdp]), "dp"))
    .withColumn("CUSRG", arr_elem(regiones, arr_regiones, "rg"))
    # Columnas 26-29
    .withColumn("CUSBR", arr_elem(sucursales, arr_sucursal, "br"))
    .withColumn("CUSMG", arr_elem(gerentes,   arr_gerentes, "mg"))
    .withColumn("CUSRF", F.concat(F.lit("REF"), F.lpad(F.col("id").cast("string"), 8, "0")))
    .withColumn("CUSRS", arr_elem(fuentes_ref, arr_fuentes, "rs"))
    # Columna 30: idioma preferido
    .withColumn("CUSLG", arr_elem(valores_cuslg, F.array([F.lit(v) for v in valores_cuslg]), "lg"))
    # Columna 31: notas
    .withColumn("CUSNT", F.concat(F.lit("NOTA-"), F.col("id").cast("string")))
    # Columna 32: grupo afinidad
    .withColumn("CUSAG", arr_elem(grupos_afin, arr_afin, "ag"))
    # Columna 33: preferencia comunicacion
    .withColumn("CUSPC", arr_elem(valores_cuspc, F.array([F.lit(v) for v in valores_cuspc]), "pc"))
    # Columna 34: nivel riesgo
    .withColumn("CUSRK", arr_elem(valores_cusrk, F.array([F.lit(v) for v in valores_cusrk]), "rk"))
    # Columnas 35-38: indicadores
    .withColumn("CUSVP", arr_elem(valores_cusvp, F.array([F.lit(v) for v in valores_cusvp]), "vp"))
    .withColumn("CUSPF", arr_elem(valores_cuspf, F.array([F.lit(v) for v in valores_cuspf]), "pf"))
    .withColumn("CUSKT", arr_elem(valores_cuskt, F.array([F.lit(v) for v in valores_cuskt]), "kt"))
    .withColumn("CUSFM", arr_elem(valores_cusfm, F.array([F.lit(v) for v in valores_cusfm]), "fm"))
    # Columna 39: ultimo canal
    .withColumn("CUSLC", arr_elem(valores_cuslc, F.array([F.lit(v) for v in valores_cuslc]), "lc"))
    # Columna 40: codigo categoria crediticia
    .withColumn("CUSCR", arr_elem(codigos_cred, arr_cred, "cr"))
    # Columna 41: indicador cuenta activa
    .withColumn("CUSAC", arr_elem(valores_cusac, F.array([F.lit(v) for v in valores_cusac]), "ac"))
    # Columna 42: clasificacion interna
    .withColumn("CUSCL", arr_elem(clasificac, arr_clasif, "cl"))
    # Columnas 43-60: 18 fechas DateType
    # CUSDB: nacimiento rango 1970-2007
    .withColumn("CUSDB",  dias_a_fecha(1970, 2007, "db"))
    # Demas fechas rango 2005-2025
    .withColumn("CUSOD",  dias_a_fecha(2005, 2025, "od"))
    .withColumn("CUSCD",  dias_a_fecha(2005, 2025, "cd"))
    .withColumn("CUSLV",  dias_a_fecha(2005, 2025, "lv"))
    .withColumn("CUSUD",  dias_a_fecha(2005, 2025, "ud"))
    .withColumn("CUSKD",  dias_a_fecha(2005, 2025, "kd"))
    .withColumn("CUSRD",  dias_a_fecha(2005, 2025, "rd"))
    .withColumn("CUSXD",  dias_a_fecha(2005, 2025, "xd"))
    .withColumn("CUSFD",  dias_a_fecha(2005, 2025, "fd"))
    .withColumn("CUSLD",  dias_a_fecha(2005, 2025, "ld"))
    .withColumn("CUSMD2", dias_a_fecha(2005, 2025, "md2"))
    .withColumn("CUSAD2", dias_a_fecha(2005, 2025, "ad2"))
    .withColumn("CUSBD",  dias_a_fecha(2005, 2025, "bd"))
    .withColumn("CUSVD",  dias_a_fecha(2005, 2025, "vd"))
    .withColumn("CUSPD",  dias_a_fecha(2005, 2025, "pd"))
    .withColumn("CUSDD",  dias_a_fecha(2005, 2025, "dd"))
    .withColumn("CUSED2", dias_a_fecha(2005, 2025, "ed2"))
    .withColumn("CUSND",  dias_a_fecha(2005, 2025, "nd"))
    # Columnas 61-68: 8 LongType (CUSYR, CUSAG2, CUSDP2, CUSAC2, CUSTX, CUSSC, CUSLR, CUSRC)
    .withColumn("CUSYR",  (F.year(F.col("CUSDB"))).cast(LongType()))
    .withColumn("CUSAG2", (F.lit(2026) - F.year(F.col("CUSDB"))).cast(LongType()))
    .withColumn("CUSDP2", (F.abs(F.hash(F.col("id").cast("string"), F.lit("dp2")).cast("long")) % 6).cast(LongType()))
    .withColumn("CUSAC2", (F.abs(F.hash(F.col("id").cast("string"), F.lit("ac2")).cast("long")) % 5 + 1).cast(LongType()))
    .withColumn("CUSTX",  (F.abs(F.hash(F.col("id").cast("string"), F.lit("tx")).cast("long"))  % 500 + 1).cast(LongType()))
    .withColumn("CUSSC",  (F.abs(F.hash(F.col("id").cast("string"), F.lit("sc")).cast("long"))  % 850 + 300).cast(LongType()))
    .withColumn("CUSLR",  (F.abs(F.hash(F.col("id").cast("string"), F.lit("lr")).cast("long"))  % 10).cast(LongType()))
    .withColumn("CUSRC",  (F.abs(F.hash(F.col("id").cast("string"), F.lit("rc")).cast("long"))  % 50).cast(LongType()))
    # Columnas 69-70: 2 DoubleType (CUSIN, CUSBL)
    .withColumn("CUSIN", (F.lit(monto_minimo) + F.rand(seed=42)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    .withColumn("CUSBL", (F.lit(monto_minimo) + F.rand(seed=101) * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
    # Eliminar columnas auxiliares
    .drop("id", "_etnia", "_idx_nombre", "_idx_apellido", "_idx_nombre2",
          "_cusnm_raw", "_cusln_raw", "_cusmd_raw")
)

print(f"DataFrame CMSTFL generado con {df_cmstfl.count()} registros y {len(df_cmstfl.columns)} columnas")

# COMMAND ----------

# ==============================================================================
# PASO 10: MODELO DE MUTACION DIFERENCIADA (RE-EJECUCION)
# ==============================================================================
# Si rutaMaestroClienteExistente tiene valor, se aplica el modelo de mutacion:
# 1) Leer parquet existente
# 2) Mutar porcentajeMutacion% de registros en camposMutacion
# 3) Agregar porcentajeNuevos% de registros nuevos con CUSTID secuencial continuado
# 4) Union de no-mutados + mutados + nuevos
# ==============================================================================

if ruta_maestro_existente and ruta_maestro_existente.strip():
    print("Modo re-ejecucion: aplicando modelo de mutacion diferenciada...")

    # Construir ruta absoluta del parquet existente con la misma logica del Paso 6
    if tipo_storage == "Volume":
        ruta_absoluta_existente = f"/Volumes/{catalogo_volume}/{esquema_volume}/{nombre_volume}/{ruta_maestro_existente}"
    else:
        ruta_absoluta_existente = f"s3://{bucket_s3}/{ruta_maestro_existente}"
    print(f"  Ruta absoluta existente: {ruta_absoluta_existente}")

    try:
        df_existente = spark.read.parquet(ruta_absoluta_existente)
    except Exception as e:
        raise ValueError(
            f"ERROR: No se pudo leer el parquet existente en '{ruta_absoluta_existente}'. "
            f"Detalle: {e}"
        )

    total_existente = df_existente.count()
    print(f"  Registros en parquet existente: {total_existente}")

    # Obtener CUSTID maximo para continuacion secuencial
    max_custid = df_existente.agg(F.max("CUSTID")).collect()[0][0]
    print(f"  CUSTID maximo existente: {max_custid}")

    # Calcular cantidades
    n_mutar  = int(total_existente * porcentaje_mutacion)
    n_nuevos = int(total_existente * porcentaje_nuevos)
    print(f"  Registros a mutar    : {n_mutar} ({porcentaje_mutacion*100:.1f}%)")
    print(f"  Registros nuevos     : {n_nuevos} ({porcentaje_nuevos*100 :.2f}%)")

    # Agregar numero de fila aleatorio para seleccion de registros a mutar
    ventana_random = Window.orderBy(F.rand(seed=7))
    df_con_rn = df_existente.withColumn("_rn", F.row_number().over(ventana_random))

    df_a_mutar    = df_con_rn.filter(F.col("_rn") <= n_mutar)
    df_no_mutar   = df_con_rn.filter(F.col("_rn") >  n_mutar).drop("_rn")

    # Aplicar mutacion en camposMutacion usando rand() como semilla de variacion
    df_mutados = df_a_mutar
    for campo in campos_mutacion:
        if campo in df_mutados.columns:
            # Re-generar el campo de forma aleatoria reutilizando las listas
            if campo in ("CUSNM", "CUSMD"):
                df_mutados = df_mutados.withColumn(
                    campo,
                    F.element_at(F.array([F.lit(n) for n in todos_nombres]),
                                 (F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit(campo), F.lit("mut")).cast("long")) % len(todos_nombres) + 1).cast("integer"))
                )
            elif campo == "CUSLN":
                df_mutados = df_mutados.withColumn(
                    campo,
                    F.element_at(F.array([F.lit(a) for a in todos_apellidos]),
                                 (F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit(campo), F.lit("mut")).cast("long")) % len(todos_apellidos) + 1).cast("integer"))
                )
            elif campo == "CUSFN":
                df_mutados = df_mutados.withColumn("CUSFN", F.concat_ws(" ", F.col("CUSNM"), F.col("CUSMD"), F.col("CUSLN")))
            elif campo in ("CUSAD", "CUSA2"):
                df_mutados = df_mutados.withColumn(campo, F.concat(F.lit("St. "), (F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit(campo), F.lit("mut")).cast("long")) % 9999 + 1).cast("string")))
            elif campo == "CUSCT":
                df_mutados = df_mutados.withColumn(campo, F.element_at(F.array([F.lit(c) for c in ciudades]), (F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit("ct_mut")).cast("long")) % len(ciudades) + 1).cast("integer")))
            elif campo == "CUSST":
                df_mutados = df_mutados.withColumn(campo, F.element_at(F.array([F.lit(s) for s in estados]),  (F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit("st_mut")).cast("long")) % len(estados) + 1).cast("integer")))
            elif campo == "CUSZP":
                df_mutados = df_mutados.withColumn(campo, F.lpad((F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit("zp_mut")).cast("long")) % 99999 + 10000).cast("string"), 5, "0"))
            elif campo == "CUSPH":
                df_mutados = df_mutados.withColumn(campo, F.concat(F.lit("+1-"), F.lpad((F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit("ph_mut")).cast("long")) % 9000000000 + 1000000000).cast("string"), 10, "0")))
            elif campo == "CUSMB":
                df_mutados = df_mutados.withColumn(campo, F.concat(F.lit("+1-"), F.lpad((F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit("mb_mut")).cast("long")) % 9000000000 + 1000000000).cast("string"), 10, "0")))
            elif campo == "CUSEM":
                df_mutados = df_mutados.withColumn(campo, F.concat(F.lower(F.col("CUSNM")), F.lit("."), F.lower(F.col("CUSLN")), F.lit("@mail.com")))
            elif campo == "CUSMS":
                df_mutados = df_mutados.withColumn(campo, F.element_at(F.array([F.lit(v) for v in valores_cusms]), (F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit("ms_mut")).cast("long")) % len(valores_cusms) + 1).cast("integer")))
            elif campo == "CUSOC":
                df_mutados = df_mutados.withColumn(campo, F.element_at(F.array([F.lit(o) for o in ocupaciones]),   (F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit("oc_mut")).cast("long")) % len(ocupaciones) + 1).cast("integer")))
            elif campo == "CUSED":
                df_mutados = df_mutados.withColumn(campo, F.element_at(F.array([F.lit(v) for v in valores_cused]), (F.abs(F.hash(F.col("CUSTID").cast("string"), F.lit("ed_mut")).cast("long")) % len(valores_cused) + 1).cast("integer")))

    df_mutados = df_mutados.drop("_rn")

    # Generar registros nuevos con CUSTID secuencial continuado
    df_nuevos_base = spark.range(max_custid + 1, max_custid + n_nuevos + 1)
    df_nuevos_base = df_nuevos_base.withColumn("_etnia", F.lit("ENG"))
    df_nuevos_base = df_nuevos_base.withColumn("_idx_nombre",   (F.abs(F.hash(F.col("id").cast("string"), F.lit("nm_new")).cast("long"))  % len(todos_nombres)).cast("integer"))
    df_nuevos_base = df_nuevos_base.withColumn("_idx_apellido", (F.abs(F.hash(F.col("id").cast("string"), F.lit("ln_new")).cast("long"))  % len(todos_apellidos)).cast("integer"))
    df_nuevos_base = df_nuevos_base.withColumn("_idx_nombre2",  (F.abs(F.hash(F.col("id").cast("string"), F.lit("md_new")).cast("long"))  % len(todos_nombres)).cast("integer"))
    df_nuevos_base = df_nuevos_base.withColumn("_cusnm_raw", F.element_at(arr_nombres,   F.col("_idx_nombre")   + 1))
    df_nuevos_base = df_nuevos_base.withColumn("_cusln_raw", F.element_at(arr_apellidos, F.col("_idx_apellido") + 1))
    df_nuevos_base = df_nuevos_base.withColumn("_cusmd_raw", F.element_at(arr_nombres,   F.col("_idx_nombre2")  + 1))

    df_nuevos = (
        df_nuevos_base
        .withColumn("CUSTID", F.col("id").cast(LongType()))
        .withColumn("CUSNM",  F.col("_cusnm_raw"))
        .withColumn("CUSLN",  F.col("_cusln_raw"))
        .withColumn("CUSMD",  F.col("_cusmd_raw"))
        .withColumn("CUSFN",  F.concat_ws(" ", F.col("CUSNM"), F.col("CUSMD"), F.col("CUSLN")))
        .withColumn("CUSSX",  arr_elem(valores_cussx, F.array([F.lit(v) for v in valores_cussx]), "sx_new"))
        .withColumn("CUSTT",  arr_elem(valores_custt, F.array([F.lit(v) for v in valores_custt]), "tt_new"))
        .withColumn("CUSAD",  F.concat(F.lit("St. "), (F.abs(F.hash(F.col("id").cast("string"), F.lit("ad_new")).cast("long")) % 9999 + 1).cast("string")))
        .withColumn("CUSA2",  F.concat(F.lit("Apt "), (F.abs(F.hash(F.col("id").cast("string"), F.lit("a2_new")).cast("long")) % 999  + 1).cast("string")))
        .withColumn("CUSCT",  arr_elem(ciudades,  arr_ciudades, "ct_new"))
        .withColumn("CUSST",  arr_elem(estados,   arr_estados,  "st_new"))
        .withColumn("CUSZP",  F.lpad((F.abs(F.hash(F.col("id").cast("string"), F.lit("zp_new")).cast("long")) % 99999 + 10000).cast("string"), 5, "0"))
        .withColumn("CUSCN",  arr_elem(paises, arr_paises, "cn_new"))
        .withColumn("CUSPH",  F.concat(F.lit("+1-"), F.lpad((F.abs(F.hash(F.col("id").cast("string"), F.lit("ph_new")).cast("long")) % 9000000000 + 1000000000).cast("string"), 10, "0")))
        .withColumn("CUSMB",  F.concat(F.lit("+1-"), F.lpad((F.abs(F.hash(F.col("id").cast("string"), F.lit("mb_new")).cast("long")) % 9000000000 + 1000000000).cast("string"), 10, "0")))
        .withColumn("CUSEM",  F.concat(F.lower(F.col("CUSNM")), F.lit("."), F.lower(F.col("CUSLN")), F.lit("@mail.com")))
        .withColumn("CUSTP",  arr_elem(valores_custp, F.array([F.lit(v) for v in valores_custp]), "tp_new"))
        .withColumn("CUSSG",  arr_elem(valores_cussg, F.array([F.lit(v) for v in valores_cussg]), "sg_new"))
        .withColumn("CUSMS",  arr_elem(valores_cusms, F.array([F.lit(v) for v in valores_cusms]), "ms_new"))
        .withColumn("CUSOC",  arr_elem(ocupaciones,   arr_ocup,                                   "oc_new"))
        .withColumn("CUSED",  arr_elem(valores_cused, F.array([F.lit(v) for v in valores_cused]), "ed_new"))
        .withColumn("CUSNA",  arr_elem(paises,   arr_paises,   "na_new"))
        .withColumn("CUSDL",  F.lpad((F.abs(F.hash(F.col("id").cast("string"), F.lit("dl_new")).cast("long")) % 999999999 + 100000000).cast("string"), 9, "0"))
        .withColumn("CUSDP",  arr_elem(valores_cusdp, F.array([F.lit(v) for v in valores_cusdp]), "dp_new"))
        .withColumn("CUSRG",  arr_elem(regiones, arr_regiones, "rg_new"))
        .withColumn("CUSBR",  arr_elem(sucursales, arr_sucursal, "br_new"))
        .withColumn("CUSMG",  arr_elem(gerentes,   arr_gerentes, "mg_new"))
        .withColumn("CUSRF",  F.concat(F.lit("REF"), F.lpad(F.col("id").cast("string"), 8, "0")))
        .withColumn("CUSRS",  arr_elem(fuentes_ref, arr_fuentes, "rs_new"))
        .withColumn("CUSLG",  arr_elem(valores_cuslg, F.array([F.lit(v) for v in valores_cuslg]), "lg_new"))
        .withColumn("CUSNT",  F.concat(F.lit("NOTA-"), F.col("id").cast("string")))
        .withColumn("CUSAG",  arr_elem(grupos_afin, arr_afin, "ag_new"))
        .withColumn("CUSPC",  arr_elem(valores_cuspc, F.array([F.lit(v) for v in valores_cuspc]), "pc_new"))
        .withColumn("CUSRK",  arr_elem(valores_cusrk, F.array([F.lit(v) for v in valores_cusrk]), "rk_new"))
        .withColumn("CUSVP",  arr_elem(valores_cusvp, F.array([F.lit(v) for v in valores_cusvp]), "vp_new"))
        .withColumn("CUSPF",  arr_elem(valores_cuspf, F.array([F.lit(v) for v in valores_cuspf]), "pf_new"))
        .withColumn("CUSKT",  arr_elem(valores_cuskt, F.array([F.lit(v) for v in valores_cuskt]), "kt_new"))
        .withColumn("CUSFM",  arr_elem(valores_cusfm, F.array([F.lit(v) for v in valores_cusfm]), "fm_new"))
        .withColumn("CUSLC",  arr_elem(valores_cuslc, F.array([F.lit(v) for v in valores_cuslc]), "lc_new"))
        .withColumn("CUSCR",  arr_elem(codigos_cred, arr_cred, "cr_new"))
        .withColumn("CUSAC",  arr_elem(valores_cusac, F.array([F.lit(v) for v in valores_cusac]), "ac_new"))
        .withColumn("CUSCL",  arr_elem(clasificac, arr_clasif, "cl_new"))
        .withColumn("CUSDB",  dias_a_fecha(1970, 2007, "db_new"))
        .withColumn("CUSOD",  dias_a_fecha(2005, 2025, "od_new"))
        .withColumn("CUSCD",  dias_a_fecha(2005, 2025, "cd_new"))
        .withColumn("CUSLV",  dias_a_fecha(2005, 2025, "lv_new"))
        .withColumn("CUSUD",  dias_a_fecha(2005, 2025, "ud_new"))
        .withColumn("CUSKD",  dias_a_fecha(2005, 2025, "kd_new"))
        .withColumn("CUSRD",  dias_a_fecha(2005, 2025, "rd_new"))
        .withColumn("CUSXD",  dias_a_fecha(2005, 2025, "xd_new"))
        .withColumn("CUSFD",  dias_a_fecha(2005, 2025, "fd_new"))
        .withColumn("CUSLD",  dias_a_fecha(2005, 2025, "ld_new"))
        .withColumn("CUSMD2", dias_a_fecha(2005, 2025, "md2_new"))
        .withColumn("CUSAD2", dias_a_fecha(2005, 2025, "ad2_new"))
        .withColumn("CUSBD",  dias_a_fecha(2005, 2025, "bd_new"))
        .withColumn("CUSVD",  dias_a_fecha(2005, 2025, "vd_new"))
        .withColumn("CUSPD",  dias_a_fecha(2005, 2025, "pd_new"))
        .withColumn("CUSDD",  dias_a_fecha(2005, 2025, "dd_new"))
        .withColumn("CUSED2", dias_a_fecha(2005, 2025, "ed2_new"))
        .withColumn("CUSND",  dias_a_fecha(2005, 2025, "nd_new"))
        .withColumn("CUSYR",  (F.year(F.col("CUSDB"))).cast(LongType()))
        .withColumn("CUSAG2", (F.lit(2026) - F.year(F.col("CUSDB"))).cast(LongType()))
        .withColumn("CUSDP2", (F.abs(F.hash(F.col("id").cast("string"), F.lit("dp2_new")).cast("long")) % 6).cast(LongType()))
        .withColumn("CUSAC2", (F.abs(F.hash(F.col("id").cast("string"), F.lit("ac2_new")).cast("long")) % 5 + 1).cast(LongType()))
        .withColumn("CUSTX",  (F.abs(F.hash(F.col("id").cast("string"), F.lit("tx_new")).cast("long"))  % 500 + 1).cast(LongType()))
        .withColumn("CUSSC",  (F.abs(F.hash(F.col("id").cast("string"), F.lit("sc_new")).cast("long"))  % 850 + 300).cast(LongType()))
        .withColumn("CUSLR",  (F.abs(F.hash(F.col("id").cast("string"), F.lit("lr_new")).cast("long"))  % 10).cast(LongType()))
        .withColumn("CUSRC",  (F.abs(F.hash(F.col("id").cast("string"), F.lit("rc_new")).cast("long"))  % 50).cast(LongType()))
        .withColumn("CUSIN",  (F.lit(monto_minimo) + F.rand(seed=77)  * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
        .withColumn("CUSBL",  (F.lit(monto_minimo) + F.rand(seed=123) * F.lit(monto_maximo - monto_minimo)).cast(DoubleType()))
        .drop("id", "_etnia", "_idx_nombre", "_idx_apellido", "_idx_nombre2",
              "_cusnm_raw", "_cusln_raw", "_cusmd_raw")
    )

    # Union de registros: no-mutados + mutados + nuevos
    columnas_orden = df_no_mutar.columns
    df_cmstfl = df_no_mutar.select(columnas_orden).union(
        df_mutados.select(columnas_orden)
    ).union(
        df_nuevos.select(columnas_orden)
    )
    print(f"Union completada. Total registros: {df_cmstfl.count()}")

else:
    print("Modo primera ejecucion: generando registros base sin mutacion")

# COMMAND ----------

# ==============================================================================
# PASO 11: ESCRITURA DEL PARQUET
# ==============================================================================

tiempo_inicio_escritura = time.time()
print(f"Escribiendo parquet en: {ruta_completa}")

df_cmstfl.coalesce(numero_particiones).write.mode("overwrite").parquet(ruta_completa)

tiempo_fin_escritura = time.time()
tiempo_escritura     = round(tiempo_fin_escritura - tiempo_inicio_escritura, 3)

# COMMAND ----------

# ==============================================================================
# PASO 12: BLOQUE DE OBSERVABILIDAD FINAL
# ==============================================================================

tiempo_fin_total   = time.time()
tiempo_total       = round(tiempo_fin_total - tiempo_inicio_total, 3)
registros_escritos = spark.read.parquet(ruta_completa).count()

print("=" * 70)
print("FIN: NbGenerarMaestroCliente (CMSTFL)")
print("=" * 70)
print(f"  Registros escritos    : {registros_escritos}")
print(f"  Columnas              : {len(df_cmstfl.columns)}")
print(f"  Ruta destino          : {ruta_completa}")
print(f"  Tiempo escritura      : {tiempo_escritura}s")
print(f"  Tiempo total          : {tiempo_total}s")
print("=" * 70)
