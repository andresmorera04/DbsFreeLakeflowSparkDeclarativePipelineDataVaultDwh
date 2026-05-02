# Databricks notebook source
# MAGIC %md
# MAGIC # NbComentariosTablas — Metadatos Unity Catalog
# MAGIC
# MAGIC **Proyecto**: LSDP Lab DataVault DWH  
# MAGIC **Fecha**: 2026-05-01  
# MAGIC **Propósito**: Aplicar comentarios de tablas y columnas en Unity Catalog para las
# MAGIC 21 entidades del modelo de datos (Bronce × 3, Plata × 14, Oro × 4).
# MAGIC
# MAGIC **Documentación de referencia**: [docs/ModeloDatos.md](../../../docs/ModeloDatos.md)
# MAGIC
# MAGIC ## Celdas de este notebook
# MAGIC
# MAGIC 1. Parámetros (widgets)
# MAGIC 2. Constantes del modelo: conjunto de 21 tablas UC
# MAGIC 3. Diccionario de comentarios de tablas (`COMENTARIOS_TABLAS`)
# MAGIC 4. Diccionario de comentarios de columnas (`COMENTARIOS_COLUMNAS`)
# MAGIC 5. Helpers: `_escapar_literal_sql()` + `aplicar_comentarios()`
# MAGIC 6. Assert de paridad: `set(COMENTARIOS_COLUMNAS.keys()) == tablas_modelo_datos`
# MAGIC 7. Aplicación por medalla (Bronce → Plata → Oro)
# MAGIC 8. DataFrame resumen de resultados

# COMMAND ----------
# MAGIC %md ## 1. Parámetros

# COMMAND ----------
# DBTITLE 1,Parámetros via dbutils.widgets
# Nota: usar dbutils.widgets EXCLUSIVAMENTE para parámetros en notebooks standalone.
# spark.conf.get() solo funciona dentro del contexto de ejecución de un pipeline DLT.

dbutils.widgets.removeAll()

dbutils.widgets.text("catalogo",       "lsdp_bronce",   "Catálogo Bronce")
dbutils.widgets.text("esquema",        "lab_dwh",        "Esquema Bronce")
dbutils.widgets.text("catalogo_plata", "lsdp_plata",    "Catálogo Plata")
dbutils.widgets.text("esquema_plata",  "lab_dwh",        "Esquema Plata")
dbutils.widgets.text("catalogo_oro",   "lsdp_oro",      "Catálogo Oro")
dbutils.widgets.text("esquema_oro",    "lab_dwh",        "Esquema Oro")

# Leer valores
CAT_B  = dbutils.widgets.get("catalogo")
ESQ_B  = dbutils.widgets.get("esquema")
CAT_P  = dbutils.widgets.get("catalogo_plata")
ESQ_P  = dbutils.widgets.get("esquema_plata")
CAT_O  = dbutils.widgets.get("catalogo_oro")
ESQ_O  = dbutils.widgets.get("esquema_oro")

print(f"Bronce  : {CAT_B}.{ESQ_B}")
print(f"Plata   : {CAT_P}.{ESQ_P}")
print(f"Oro     : {CAT_O}.{ESQ_O}")

# COMMAND ----------
# MAGIC %md ## 2. Constantes del modelo

# COMMAND ----------
# DBTITLE 1,Conjunto de 21 tablas del modelo (base de paridad)
# Estas son las ÚNICAS tablas que se publican en Unity Catalog.
# Trx_ATM_Stream y Map_Cliente_Operacion_Dominante son temporary=True y no se incluyen.

TABLAS_MODELO_DATOS = {
    # ── Bronce ──────────────────────────────────────────────
    "CMSTFL",
    "TRXPFL",
    "BLNCFL",
    # ── Plata — Hubs ────────────────────────────────────────
    "Hub_Cliente",
    "Hub_Transaccion",
    "Hub_Operacion",
    # ── Plata — Links ───────────────────────────────────────
    "Link_Cliente_Operacion",
    "Link_Cliente_Transaccion",
    # ── Plata — Satellites Cliente ──────────────────────────
    "Sat_Cliente_DatosEstables",
    "Sat_Cliente_Contacto",
    "Sat_Cliente_Clasificacion",
    "Sat_Cliente_Financiero",
    # ── Plata — Satellites Operación ────────────────────────
    "Sat_Operacion_DatosEstables",
    "Sat_Operacion_Montos",
    "Sat_Operacion_FechasEvento",
    # ── Plata — Satellites Transacción ──────────────────────
    "Sat_Transaccion_DatosEstables",
    "Sat_Transaccion_Montos",
    # ── Oro ─────────────────────────────────────────────────
    "Dim_Cliente",
    "Dim_Operacion",
    "Dim_Tiempo",
    "Hec_Transacciones_ATM",
}

# Mapa tabla → (catálogo, esquema) para construir nombres de 3 partes
_MEDALLA_MAP = {
    "CMSTFL": (CAT_B, ESQ_B), "TRXPFL": (CAT_B, ESQ_B), "BLNCFL": (CAT_B, ESQ_B),
    "Hub_Cliente": (CAT_P, ESQ_P), "Hub_Transaccion": (CAT_P, ESQ_P),
    "Hub_Operacion": (CAT_P, ESQ_P),
    "Link_Cliente_Operacion": (CAT_P, ESQ_P), "Link_Cliente_Transaccion": (CAT_P, ESQ_P),
    "Sat_Cliente_DatosEstables": (CAT_P, ESQ_P), "Sat_Cliente_Contacto": (CAT_P, ESQ_P),
    "Sat_Cliente_Clasificacion": (CAT_P, ESQ_P), "Sat_Cliente_Financiero": (CAT_P, ESQ_P),
    "Sat_Operacion_DatosEstables": (CAT_P, ESQ_P), "Sat_Operacion_Montos": (CAT_P, ESQ_P),
    "Sat_Operacion_FechasEvento": (CAT_P, ESQ_P),
    "Sat_Transaccion_DatosEstables": (CAT_P, ESQ_P), "Sat_Transaccion_Montos": (CAT_P, ESQ_P),
    "Dim_Cliente": (CAT_O, ESQ_O), "Dim_Operacion": (CAT_O, ESQ_O),
    "Dim_Tiempo": (CAT_O, ESQ_O), "Hec_Transacciones_ATM": (CAT_O, ESQ_O),
}

print(f"Total tablas en modelo: {len(TABLAS_MODELO_DATOS)}")

# COMMAND ----------
# MAGIC %md ## 3. Diccionario de comentarios de tablas

# COMMAND ----------
# DBTITLE 1,COMENTARIOS_TABLAS — 21 entradas
COMENTARIOS_TABLAS = {
    # ── Bronce ──────────────────────────────────────────────────────────────────
    "CMSTFL": (
        "Streaming Table Bronce — Maestro de clientes bancarios. Ingesta incremental "
        "desde AS400 via AutoLoader (cloudFiles/parquet). ~4M registros. "
        "Liquid Clustering: FechaRegistroParquet."
    ),
    "TRXPFL": (
        "Streaming Table Bronce — Transacciones bancarias (OLTP AS400). Incluye "
        "retiros ATM (DATM) y depósitos ATM (CATM). ~7M registros. "
        "Liquid Clustering: FechaRegistroParquet."
    ),
    "BLNCFL": (
        "Streaming Table Bronce — Saldos y operaciones bancarias (cuentas). "
        "Llave primaria compuesta: CUSTID + BLSQ. ~4M registros. "
        "Liquid Clustering: FechaRegistroParquet."
    ),
    # ── Plata — Hubs ────────────────────────────────────────────────────────────
    "Hub_Cliente": (
        "Hub Data Vault 2.0 — Llave de negocio de Cliente. "
        "Hash SHA2-256 de CUSTID. Estrategia OPT-001: dp.create_auto_cdc_flow SCD Tipo 1. "
        "Fuente: CMSTFL. Liquid Clustering: Hash_Cliente, FechaRegistro."
    ),
    "Hub_Transaccion": (
        "Hub Data Vault 2.0 — Llave de negocio de Transacción. "
        "Hash SHA2-256 de TRXID. Estrategia: append_flow puro sobre vista_trxpfl_cdf (CDF). "
        "Liquid Clustering: FechaRegistro, Hash_Transaccion."
    ),
    "Hub_Operacion": (
        "Hub Data Vault 2.0 — Llave de negocio de Operación (cuenta bancaria). "
        "Hash SHA2-256 de CUSTID|BLSQ. Estrategia OPT-001: dp.create_auto_cdc_flow SCD Tipo 1. "
        "Fuente: BLNCFL. Liquid Clustering: Hash_Operacion, FechaRegistro."
    ),
    # ── Plata — Links ───────────────────────────────────────────────────────────
    "Link_Cliente_Operacion": (
        "Link Data Vault 2.0 — Relación M:M entre Cliente y Operación (cuenta). "
        "Estrategia OPT-001: dp.create_auto_cdc_flow SCD Tipo 1. "
        "Fuente: BLNCFL. Liquid Clustering: Hash_Cliente, Hash_Operacion, FechaRegistro."
    ),
    "Link_Cliente_Transaccion": (
        "Link Data Vault 2.0 — Relación M:M entre Cliente y Transacción. "
        "Estrategia: append_flow puro sobre vista_trxpfl_cdf (CDF). "
        "Liquid Clustering: FechaRegistro, Hash_Cliente, Hash_Transaccion."
    ),
    # ── Plata — Satellites Cliente ──────────────────────────────────────────────
    "Sat_Cliente_DatosEstables": (
        "Satellite Data Vault 2.0 — Datos estables de Cliente: "
        "sexo, fecha nacimiento, país, idioma, rangos categóricos (RangoEtario, CategoriaIngresos). "
        "Deduplicación por Hash_Diferenciador (SHA2-512). Fuente: CMSTFL."
    ),
    "Sat_Cliente_Contacto": (
        "Satellite Data Vault 2.0 — Datos de contacto de Cliente: "
        "nombre, apellido, dirección, teléfonos, correo, estado civil, ocupación, educación. "
        "Deduplicación por Hash_Diferenciador (SHA2-512). Fuente: CMSTFL."
    ),
    "Sat_Cliente_Clasificacion": (
        "Satellite Data Vault 2.0 — Clasificación de Cliente: "
        "tipo, segmento, región, nivel de riesgo, indicador VIP, estado KYC, calificación crediticia. "
        "Deduplicación por Hash_Diferenciador (SHA2-512). Fuente: CMSTFL."
    ),
    "Sat_Cliente_Financiero": (
        "Satellite Data Vault 2.0 — Datos financieros de Cliente: "
        "score, ingresos, cantidad de cuentas y transacciones, fechas de eventos bancarios. "
        "Deduplicación por Hash_Diferenciador (SHA2-512). Fuente: CMSTFL."
    ),
    # ── Plata — Satellites Operación ────────────────────────────────────────────
    "Sat_Operacion_DatosEstables": (
        "Satellite Data Vault 2.0 — Datos estáticos de cuenta bancaria: "
        "tipo, número, moneda, estado, producto, subproducto, riesgo, región. "
        "Deduplicación por Hash_Diferenciador (SHA2-512). Fuente: BLNCFL."
    ),
    "Sat_Operacion_Montos": (
        "Satellite Data Vault 2.0 — Saldos y límites de cuenta bancaria: "
        "saldo disponible, total, reservado, bloqueado, crédito utilizado, ratio, sobregiro. "
        "Incluye clasificadores CategoriaSaldo, EstadoUtilizacionCredito, IndicadorSobregiro. "
        "Deduplicación por Hash_Diferenciador (SHA2-512). Fuente: BLNCFL."
    ),
    "Sat_Operacion_FechasEvento": (
        "Satellite Data Vault 2.0 — Fechas de eventos de cuenta bancaria: "
        "apertura, último movimiento, cierre, actualización. "
        "Deduplicación por Hash_Diferenciador (SHA2-512). Fuente: BLNCFL."
    ),
    # ── Plata — Satellites Transacción ──────────────────────────────────────────
    "Sat_Transaccion_DatosEstables": (
        "Satellite Data Vault 2.0 — Datos no monetarios de transacción bancaria: "
        "tipo (DATM/CATM), moneda, estado, canal, descripción, referencia, fechas de procesamiento, "
        "ClasificacionCanalATM. Incluye VersionCarga y FechaCargaBronce de la Change Data Feed. "
        "Estrategia: append_flow puro sobre vista_trxpfl_cdf. Fuente: TRXPFL."
    ),
    "Sat_Transaccion_Montos": (
        "Satellite Data Vault 2.0 — Datos monetarios de transacción bancaria: "
        "monto principal, comisión, saldos anterior y posterior, totales. "
        "Incluye RangoMontoTransaccion, NivelRiesgoFraude, VersionCarga, FechaCargaBronce (CDF). "
        "Estrategia: append_flow puro sobre vista_trxpfl_cdf. Fuente: TRXPFL."
    ),
    # ── Oro ─────────────────────────────────────────────────────────────────────
    "Dim_Cliente": (
        "Dimensión Oro — Cliente (SCD Tipo 1). Materialised View con full refresh. "
        "Consolida Hub_Cliente + 4 Satellites. Llave subrogada DimIdCliente = xxhash64(Hash_Cliente). "
        "28 columnas de negocio. Liquid Clustering: DimIdCliente."
    ),
    "Dim_Operacion": (
        "Dimensión Oro — Operación/Cuenta bancaria (SCD Tipo 1). Materialised View con full refresh. "
        "Consolida Hub_Operacion + 3 Satellites. Llave subrogada DimIdOperacion = xxhash64(Hash_Operacion). "
        "24 columnas de negocio. Liquid Clustering: DimIdOperacion."
    ),
    "Dim_Tiempo": (
        "Dimensión Oro — Tiempo (calendario). Materialised View con refresh incremental (Enzyme CDF). "
        "Generada desde fechas únicas de Sat_Transaccion_Montos. "
        "Incluye Anio, Mes, Dia, Trimestre, Semestre, NombreDia, NombreMes, EsFinSemana. "
        "Liquid Clustering: FechaClave."
    ),
    "Hec_Transacciones_ATM": (
        "Tabla de Hechos Oro — Transacciones ATM (retiros DATM + depósitos CATM). "
        "Materialised View con refresh incremental (CDF sobre Trx_ATM_Stream). "
        "Grano: una fila por transacción ATM. FK: DimIdCliente, DimIdOperacion, FechaClave. "
        "16 columnas. Liquid Clustering: FechaClave, DimIdCliente."
    ),
}

print(f"Total comentarios de tabla: {len(COMENTARIOS_TABLAS)}")

# COMMAND ----------
# MAGIC %md ## 4. Diccionario de comentarios de columnas

# COMMAND ----------
# DBTITLE 1,COMENTARIOS_COLUMNAS — columnas clave por tabla
# Estructura: { "NombreTabla": { "nombre_columna": "comentario", ... }, ... }
# Se documentan las columnas más relevantes; columnas menores sin cambios frecuentes
# se pueden omitir (el helper las marca como SKIPPED).

COMENTARIOS_COLUMNAS = {
    # ── Bronce ──────────────────────────────────────────────────────────────────
    "CMSTFL": {
        "FechaRegistroParquet": "Fecha derivada de la partición física año/mes/dia del Landing Zone. Liquid Clustering key.",
        "CUSTID":  "Identificador único de cliente en el sistema AS400. Llave primaria de negocio.",
        "CUSNM":   "Nombre de pila del cliente.",
        "CUSLN":   "Apellido del cliente.",
        "CUSFN":   "Nombre completo (concatenación CUSNM + CUSLN).",
        "CUSSX":   "Sexo del cliente. Valores: M, F, O.",
        "CUSDB":   "Fecha de nacimiento del cliente.",
        "CUSYR":   "Año de nacimiento (derivado de CUSDB).",
        "CUSAG2":  "Edad calculada del cliente a la fecha del snapshot.",
        "CUSIN":   "Ingresos mensuales estimados del cliente.",
        "_rescued_data": "Columna automática de AutoLoader. Captura campos no mapeados por schema evolution.",
    },
    "TRXPFL": {
        "FechaRegistroParquet": "Fecha derivada de la partición física año/mes/dia del Landing Zone. Liquid Clustering key.",
        "TRXID":   "Identificador único global de la transacción en AS400. Llave primaria de negocio.",
        "CUSTID":  "Identificador del cliente que realizó la transacción. FK hacia CMSTFL.",
        "TRXTYP":  "Tipo de transacción. DATM=retiro ATM, CATM=depósito ATM, otros tipos posibles.",
        "TRXCUR":  "Moneda de la transacción. Código ISO 4217.",
        "TRXST":   "Estado de la transacción. Ej: APROBADA, REVERTIDA.",
        "TRXCH":   "Canal de la transacción. Ej: ATM, WEB, APP.",
        "TRXAMT":  "Monto principal de la transacción.",
        "TRXCM":   "Comisión aplicada a la transacción.",
        "TRXDT":   "Fecha de la transacción (fecha de corte del snapshot AS400).",
        "TRXFR":   "Indicador de riesgo de fraude. Escala 0–100.",
        "_rescued_data": "Columna automática de AutoLoader. Captura campos no mapeados por schema evolution.",
    },
    "BLNCFL": {
        "FechaRegistroParquet": "Fecha derivada de la partición física año/mes/dia del Landing Zone. Liquid Clustering key.",
        "CUSTID":  "Identificador del cliente propietario de la cuenta. FK hacia CMSTFL.",
        "BLSQ":    "Secuencia de saldo — diferenciador de operación por cliente. Llave compuesta con CUSTID.",
        "BLACT":   "Tipo de cuenta bancaria. Ej: AHORRO, CORRIENTE.",
        "BLACN":   "Número de cuenta bancaria.",
        "BLCUR":   "Moneda de la cuenta. Código ISO 4217.",
        "BLST":    "Estado de la cuenta. Ej: ACTIVA, BLOQUEADA.",
        "BLAV":    "Saldo disponible para transacciones.",
        "BLTB":    "Saldo total de la cuenta.",
        "BLCR":    "Límite de crédito de la cuenta.",
        "BLCU":    "Crédito utilizado actualmente.",
        "_rescued_data": "Columna automática de AutoLoader. Captura campos no mapeados por schema evolution.",
    },
    # ── Plata — Hubs ────────────────────────────────────────────────────────────
    "Hub_Cliente": {
        "Hash_Cliente":          "Llave de Hub. SHA2-256 de CUSTID. Identificador único del cliente en Data Vault 2.0.",
        "IdentificadorCliente":  "Llave de negocio original. CUSTID del sistema AS400.",
        "FechaRegistro":         "Fecha de carga (Load Date DV2.0). Timestamp de inserción o última vista del registro.",
        "FuenteDatos":           "Nombre de 3 partes de la tabla Bronce origen. Ej: lsdp_bronce.lab_dwh.CMSTFL.",
    },
    "Hub_Transaccion": {
        "Hash_Transaccion":           "Llave de Hub. SHA2-256 de TRXID. Identificador único de la transacción en Data Vault 2.0.",
        "IdentificadorTransaccion":   "Llave de negocio original. TRXID del sistema AS400.",
        "FechaRegistro":              "Fecha de carga (Load Date DV2.0). Timestamp de inserción.",
        "FuenteDatos":                "Nombre de 3 partes de la tabla Bronce origen. Ej: lsdp_bronce.lab_dwh.TRXPFL.",
    },
    "Hub_Operacion": {
        "Hash_Operacion":        "Llave de Hub. SHA2-256 de concat(CUSTID, '|', BLSQ). Identificador único de la operación en Data Vault 2.0.",
        "IdentificadorCliente":  "Parte 1 de la llave de negocio compuesta. CUSTID del sistema AS400.",
        "SecuenciaSaldo":        "Parte 2 de la llave de negocio compuesta. BLSQ del sistema AS400.",
        "FechaRegistro":         "Fecha de carga (Load Date DV2.0). Timestamp de inserción o última vista del registro.",
        "FuenteDatos":           "Nombre de 3 partes de la tabla Bronce origen. Ej: lsdp_bronce.lab_dwh.BLNCFL.",
    },
    # ── Plata — Links ───────────────────────────────────────────────────────────
    "Link_Cliente_Operacion": {
        "Hash_Cliente":                   "SHA2-256 de CUSTID. FK hacia Hub_Cliente.",
        "Hash_Operacion":                 "SHA2-256 de CUSTID|BLSQ. FK hacia Hub_Operacion.",
        "Hash_Link_Cliente_Operacion":    "Llave de Link. SHA2-256 de concat(Hash_Cliente, '|', Hash_Operacion).",
        "FechaRegistro":                  "Fecha de carga (Load Date DV2.0).",
        "FuenteDatos":                    "Nombre de 3 partes de la tabla Bronce origen.",
    },
    "Link_Cliente_Transaccion": {
        "Hash_Cliente":                    "SHA2-256 de CUSTID. FK hacia Hub_Cliente.",
        "Hash_Transaccion":                "SHA2-256 de TRXID. FK hacia Hub_Transaccion.",
        "Hash_Link_Cliente_Transaccion":   "Llave de Link. SHA2-256 de concat(Hash_Cliente, '|', Hash_Transaccion).",
        "FechaRegistro":                   "Fecha de carga (Load Date DV2.0).",
        "FuenteDatos":                     "Nombre de 3 partes de la tabla Bronce origen.",
    },
    # ── Plata — Satellites Cliente ──────────────────────────────────────────────
    "Sat_Cliente_DatosEstables": {
        "Hash_Cliente":       "SHA2-256 de CUSTID. FK hacia Hub_Cliente.",
        "Hash_Diferenciador": "SHA2-512 de todos los campos de negocio del satellite. Detecta cambios (SCD2).",
        "FechaRegistro":      "Fecha de carga (Load Date DV2.0).",
        "FuenteDatos":        "Nombre de 3 partes de la tabla Bronce origen.",
        "sexo_cliente":       "Sexo del cliente (M/F/O). Derivado de CUSSX.",
        "RangoEtario":        "Categoría de edad calculada: JOVEN_ADULTO, ADULTO, ADULTO_MAYOR, etc.",
        "CategoriaIngresos":  "Categoría de ingresos mensuales: BAJO, MEDIO, ALTO, MUY_ALTO.",
    },
    "Sat_Cliente_Contacto": {
        "Hash_Cliente":          "SHA2-256 de CUSTID. FK hacia Hub_Cliente.",
        "Hash_Diferenciador":    "SHA2-512 de todos los campos de negocio del satellite. Detecta cambios (SCD2).",
        "FechaRegistro":         "Fecha de carga (Load Date DV2.0).",
        "FuenteDatos":           "Nombre de 3 partes de la tabla Bronce origen.",
        "nombre_cliente":        "Nombre de pila del cliente.",
        "apellido_cliente":      "Apellido del cliente.",
        "nombre_completo_cliente": "Nombre completo (nombre + apellido). Calculado con concat_ws.",
        "correo_electronico":    "Correo electrónico del cliente.",
        "telefono_principal":    "Teléfono principal del cliente.",
    },
    "Sat_Cliente_Clasificacion": {
        "Hash_Cliente":           "SHA2-256 de CUSTID. FK hacia Hub_Cliente.",
        "Hash_Diferenciador":     "SHA2-512 de todos los campos de negocio del satellite. Detecta cambios (SCD2).",
        "FechaRegistro":          "Fecha de carga (Load Date DV2.0).",
        "FuenteDatos":            "Nombre de 3 partes de la tabla Bronce origen.",
        "tipo_cliente":           "Tipo de cliente: RETAIL, CORP, PYME.",
        "segmento_cliente":       "Segmento de cliente: PREMIUM, STANDARD, BASICO.",
        "nivel_riesgo":           "Nivel de riesgo crediticio: BAJO, MEDIO, ALTO.",
        "indicador_vip":          "S si el cliente es VIP, N en caso contrario.",
        "estado_kyc":             "Estado del proceso KYC: COMPLETO, PENDIENTE, VENCIDO.",
    },
    "Sat_Cliente_Financiero": {
        "Hash_Cliente":               "SHA2-256 de CUSTID. FK hacia Hub_Cliente.",
        "Hash_Diferenciador":         "SHA2-512 de todos los campos de negocio del satellite. Detecta cambios (SCD2).",
        "FechaRegistro":              "Fecha de carga (Load Date DV2.0).",
        "FuenteDatos":                "Nombre de 3 partes de la tabla Bronce origen.",
        "score_cliente":              "Score crediticio del cliente. Rango 300–1150.",
        "ingresos_cliente":           "Ingresos mensuales estimados.",
        "cantidad_cuentas":           "Número de cuentas activas del cliente.",
        "cantidad_transacciones":     "Número histórico de transacciones del cliente.",
    },
    # ── Plata — Satellites Operación ────────────────────────────────────────────
    "Sat_Operacion_DatosEstables": {
        "Hash_Operacion":     "SHA2-256 de CUSTID|BLSQ. FK hacia Hub_Operacion.",
        "Hash_Diferenciador": "SHA2-512 de todos los campos de negocio del satellite. Detecta cambios (SCD2).",
        "FechaRegistro":      "Fecha de carga (Load Date DV2.0).",
        "FuenteDatos":        "Nombre de 3 partes de la tabla Bronce origen.",
        "tipo_cuenta":        "Tipo de cuenta bancaria: AHORRO, CORRIENTE. Derivado de BLACT.",
        "moneda_cuenta":      "Moneda de la cuenta. Código ISO 4217. Derivado de BLCUR.",
        "estado_cuenta":      "Estado de la cuenta: ACTIVA, BLOQUEADA. Derivado de BLST.",
    },
    "Sat_Operacion_Montos": {
        "Hash_Operacion":            "SHA2-256 de CUSTID|BLSQ. FK hacia Hub_Operacion.",
        "Hash_Diferenciador":        "SHA2-512 de todos los campos de negocio del satellite. Detecta cambios (SCD2).",
        "FechaRegistro":             "Fecha de carga (Load Date DV2.0).",
        "FuenteDatos":               "Nombre de 3 partes de la tabla Bronce origen.",
        "saldo_disponible":          "Saldo disponible para transacciones. Derivado de BLAV.",
        "saldo_total":               "Saldo total de la cuenta. Derivado de BLTB.",
        "limite_credito":            "Límite de crédito de la cuenta. Derivado de BLCR.",
        "credito_utilizado":         "Crédito utilizado actualmente. Derivado de BLCU.",
        "CategoriaSaldo":            "Categoría del saldo disponible: BAJO, MEDIO, ALTO, MUY_ALTO.",
        "EstadoUtilizacionCredito":  "Estado de uso del crédito: SIN_USO, USO_BAJO, USO_MODERADO, USO_ALTO, MAXIMO.",
        "IndicadorSobregiro":        "Indicador de sobregiro: SIN_SOBREGIRO, SOBREGIRO_LEVE, SOBREGIRO_CRITICO.",
    },
    "Sat_Operacion_FechasEvento": {
        "Hash_Operacion":            "SHA2-256 de CUSTID|BLSQ. FK hacia Hub_Operacion.",
        "Hash_Diferenciador":        "SHA2-512 de todos los campos de negocio del satellite. Detecta cambios (SCD2).",
        "FechaRegistro":             "Fecha de carga (Load Date DV2.0).",
        "FuenteDatos":               "Nombre de 3 partes de la tabla Bronce origen.",
        "fecha_apertura_cuenta":     "Fecha en que se abrió la cuenta bancaria.",
        "fecha_ultimo_movimiento":   "Fecha del último movimiento registrado en la cuenta.",
        "fecha_cierre_cuenta":       "Fecha de cierre de la cuenta. NULL si la cuenta está activa.",
        "fecha_actualizacion_cuenta":"Fecha de la última actualización de datos de la cuenta.",
    },
    # ── Plata — Satellites Transacción ──────────────────────────────────────────
    "Sat_Transaccion_DatosEstables": {
        "Hash_Transaccion":      "SHA2-256 de TRXID. FK hacia Hub_Transaccion.",
        "Hash_Diferenciador":    "SHA2-512 de todos los campos de negocio del satellite.",
        "FechaRegistro":         "Fecha de carga (Load Date DV2.0).",
        "FuenteDatos":           "Nombre de 3 partes de la tabla Bronce origen.",
        "VersionCarga":          "Versión Delta (_commit_version) de TRXPFL al momento de la carga. Trazabilidad CDF.",
        "FechaCargaBronce":      "Timestamp Delta (_commit_timestamp) de TRXPFL al momento de la carga. Trazabilidad CDF.",
        "tipo_transaccion":      "Tipo de transacción. DATM=retiro ATM, CATM=depósito ATM.",
        "moneda_transaccion":    "Moneda de la transacción. Código ISO 4217.",
        "estado_transaccion":    "Estado de la transacción: APROBADA, REVERTIDA.",
        "canal_transaccion":     "Canal de la transacción: ATM, WEB, APP.",
        "ClasificacionCanalATM": "Clasificación de canal ATM: RETIRO_ATM, DEPOSITO_ATM, OTRA_OP_ATM, NO_ATM.",
    },
    "Sat_Transaccion_Montos": {
        "Hash_Transaccion":          "SHA2-256 de TRXID. FK hacia Hub_Transaccion.",
        "Hash_Diferenciador":        "SHA2-512 de todos los campos de negocio del satellite.",
        "FechaRegistro":             "Fecha de carga (Load Date DV2.0).",
        "FuenteDatos":               "Nombre de 3 partes de la tabla Bronce origen.",
        "VersionCarga":              "Versión Delta (_commit_version) de TRXPFL al momento de la carga. Trazabilidad CDF.",
        "FechaCargaBronce":          "Timestamp Delta (_commit_timestamp) de TRXPFL al momento de la carga. Trazabilidad CDF.",
        "monto_principal":           "Monto principal de la transacción. Derivado de TRXAMT.",
        "comision_transaccion":      "Comisión aplicada a la transacción. Derivado de TRXCM.",
        "total_transaccion":         "Monto total de la transacción (monto + comisión).",
        "RangoMontoTransaccion":     "Categoría del monto: MICRO, PEQUEÑA, MEDIANA, GRANDE, MUY_GRANDE.",
        "NivelRiesgoFraude":         "Nivel de riesgo de fraude: SIN_RIESGO, RIESGO_BAJO, RIESGO_MEDIO, RIESGO_ALTO.",
    },
    # ── Oro ─────────────────────────────────────────────────────────────────────
    "Dim_Cliente": {
        "DimIdCliente":          "Llave subrogada. xxhash64(Hash_Cliente).cast('long'). Puede ser negativo.",
        "Hash_Cliente":          "Hash de negocio SHA2-256 de CUSTID. Rastreabilidad hacia Plata.",
        "IdentificadorCliente":  "Identificador original del cliente en AS400 (CUSTID).",
        "SexoCliente":           "Sexo del cliente: M, F, O.",
        "EdadCliente":           "Edad actual del cliente.",
        "RangoEtario":           "Categoría de edad: JOVEN_ADULTO, ADULTO, ADULTO_MAYOR, etc.",
        "CategoriaIngresos":     "Categoría de ingresos mensuales: BAJO, MEDIO, ALTO, MUY_ALTO.",
        "NombreCompletoCliente": "Nombre completo del cliente.",
        "CorreoElectronico":     "Correo electrónico del cliente.",
        "TipoCliente":           "Tipo de cliente: RETAIL, CORP, PYME.",
        "SegmentoCliente":       "Segmento de cliente: PREMIUM, STANDARD, BASICO.",
        "NivelRiesgo":           "Nivel de riesgo crediticio: BAJO, MEDIO, ALTO.",
        "ScoreCliente":          "Score crediticio del cliente. Rango 300–1150.",
        "IngresosCliente":       "Ingresos mensuales estimados.",
    },
    "Dim_Operacion": {
        "DimIdOperacion":            "Llave subrogada. xxhash64(Hash_Operacion).cast('long'). Puede ser negativo.",
        "Hash_Operacion":            "Hash de negocio SHA2-256 de CUSTID|BLSQ. Rastreabilidad hacia Plata.",
        "IdentificadorCliente":      "CUSTID del propietario de la cuenta.",
        "SecuenciaSaldo":            "BLSQ — secuencia de saldo en AS400.",
        "TipoCuenta":                "Tipo de cuenta: AHORRO, CORRIENTE.",
        "MonedaCuenta":              "Moneda de la cuenta (ISO 4217).",
        "EstadoCuenta":              "Estado de la cuenta: ACTIVA, BLOQUEADA.",
        "CategoriaSaldo":            "Categoría de saldo disponible: BAJO, MEDIO, ALTO, MUY_ALTO.",
        "EstadoUtilizacionCredito":  "Estado de uso de crédito: SIN_USO, USO_BAJO, USO_MODERADO, USO_ALTO, MAXIMO.",
        "IndicadorSobregiro":        "Indicador de sobregiro: SIN_SOBREGIRO, SOBREGIRO_LEVE, SOBREGIRO_CRITICO.",
        "SaldoDisponible":           "Saldo disponible para transacciones.",
        "SaldoTotal":                "Saldo total de la cuenta.",
    },
    "Dim_Tiempo": {
        "FechaClave":    "Llave primaria. Fecha de transacción como DATE. Granularidad: día.",
        "Anio":          "Año de la fecha. Ej: 2024.",
        "Mes":           "Mes del año (1–12).",
        "Dia":           "Día del mes (1–31).",
        "Trimestre":     "Trimestre del año (1–4).",
        "Semestre":      "Semestre del año (1–2).",
        "DiaSemana":     "Día de la semana (1=domingo, 7=sábado). ISO estándar Spark.",
        "NombreDia":     "Nombre del día en español: Lunes, Martes, ..., Domingo.",
        "NombreMes":     "Nombre del mes en español: Enero, Febrero, ..., Diciembre.",
        "EsFinSemana":   "true si el día es sábado o domingo.",
        "DiaDelAnio":    "Día del año (1–366).",
        "SemanaDelAnio": "Semana ISO del año.",
    },
    "Hec_Transacciones_ATM": {
        "FechaClave":               "FK → Dim_Tiempo. Fecha de la transacción ATM.",
        "DimIdCliente":             "FK → Dim_Cliente. Llave subrogada del cliente.",
        "DimIdOperacion":           "FK → Dim_Operacion. Llave subrogada de la cuenta de la transacción.",
        "IdentificadorTransaccion": "Dimensión degenerada. TRXID original del sistema AS400.",
        "Hash_Transaccion":         "Hash de negocio SHA2-256 de TRXID. Rastreabilidad hacia Plata.",
        "TipoTransaccion":          "Tipo de transacción ATM. DATM=retiro, CATM=depósito.",
        "MonedaTransaccion":        "Moneda de la transacción. Código ISO 4217.",
        "EstadoTransaccion":        "Estado de la transacción: APROBADA, REVERTIDA.",
        "RangoMontoTransaccion":    "Categoría del monto: MICRO, PEQUEÑA, MEDIANA, GRANDE, MUY_GRANDE.",
        "ClasificacionCanalATM":    "Clasificación de canal ATM: RETIRO_ATM, DEPOSITO_ATM, OTRA_OP_ATM.",
        "MontoPrincipal":           "Monto principal de la transacción (métrica principal).",
        "ComisionTransaccion":      "Comisión aplicada a la transacción.",
        "TotalTransaccion":         "Monto total = monto principal + comisión.",
        "EsRetiro":                 "true si TipoTransaccion = 'DATM' (retiro ATM).",
        "EsDeposito":               "true si TipoTransaccion = 'CATM' (depósito ATM).",
    },
}

print(f"Total tablas en COMENTARIOS_COLUMNAS: {len(COMENTARIOS_COLUMNAS)}")

# COMMAND ----------
# MAGIC %md ## 5. Helpers: `_escapar_literal_sql()` + `aplicar_comentarios()`

# COMMAND ----------
# DBTITLE 1,Helper functions
def _escapar_literal_sql(texto: str) -> str:
    """
    Escapa el texto para usarlo como literal SQL entre comillas simples.
    Reemplaza cada comilla simple por dos comillas simples (estándar SQL).
    """
    return texto.replace("'", "''")


def aplicar_comentarios(
    cat: str,
    esq: str,
    tabla: str,
    comentario_tabla: str,
    comentarios_columnas: dict,
) -> dict:
    """
    Aplica el comentario de tabla y los comentarios de columnas en Unity Catalog.

    Estrategia con fallback:
      1. COMMENT ON TABLE {nombre_3partes} IS '...'
         Si falla → SKIPPED con mensaje de error.
      2. Para cada columna: ALTER TABLE ... ALTER COLUMN ... COMMENT '...'
         Si falla → COMMENT ON COLUMN (sintaxis alternativa)
         Si ambos fallan → columna marcada SKIPPED.

    Retorna:
        dict con claves:
            estado_tabla        : 'OK' | 'SKIPPED'
            cols_correctas      : int
            cols_omitidas       : int
            mensaje             : str (detalle de errores si los hay)
    """
    nombre_3partes = f"`{cat}`.`{esq}`.`{tabla}`"
    cols_correctas = 0
    cols_omitidas  = 0
    mensajes_error = []

    # ── 1. Comentario de tabla ──────────────────────────────────────────────────
    estado_tabla = "OK"
    texto_tabla  = _escapar_literal_sql(comentario_tabla)
    try:
        spark.sql(
            f"COMMENT ON TABLE {nombre_3partes} IS '{texto_tabla}'"
        )
    except Exception as e_tabla:
        estado_tabla = "SKIPPED"
        mensajes_error.append(f"TABLE COMMENT: {str(e_tabla)[:120]}")

    # ── 2. Comentarios de columnas ──────────────────────────────────────────────
    for col_nombre, col_comentario in comentarios_columnas.items():
        texto_col = _escapar_literal_sql(col_comentario)
        aplicado  = False

        # Intento 1: ALTER TABLE ... ALTER COLUMN ... COMMENT
        try:
            spark.sql(
                f"ALTER TABLE {nombre_3partes} "
                f"ALTER COLUMN `{col_nombre}` COMMENT '{texto_col}'"
            )
            aplicado = True
        except Exception:
            pass

        # Intento 2 (fallback): COMMENT ON COLUMN
        if not aplicado:
            try:
                spark.sql(
                    f"COMMENT ON COLUMN {nombre_3partes}.`{col_nombre}` "
                    f"IS '{texto_col}'"
                )
                aplicado = True
            except Exception as e_col:
                mensajes_error.append(
                    f"COL {col_nombre}: {str(e_col)[:80]}"
                )

        if aplicado:
            cols_correctas += 1
        else:
            cols_omitidas += 1

    mensaje_final = "; ".join(mensajes_error) if mensajes_error else "Sin errores"

    return {
        "estado_tabla":   estado_tabla,
        "cols_correctas": cols_correctas,
        "cols_omitidas":  cols_omitidas,
        "mensaje":        mensaje_final,
    }

# COMMAND ----------
# MAGIC %md ## 6. Assert de paridad con el modelo de datos

# COMMAND ----------
# DBTITLE 1,Verificar paridad entre COMENTARIOS_COLUMNAS y TABLAS_MODELO_DATOS
claves_comentarios = set(COMENTARIOS_COLUMNAS.keys())
claves_modelo      = set(TABLAS_MODELO_DATOS)

faltantes_en_comentarios  = claves_modelo - claves_comentarios
sobrantes_en_comentarios  = claves_comentarios - claves_modelo

if faltantes_en_comentarios:
    raise AssertionError(
        f"Las siguientes tablas están en TABLAS_MODELO_DATOS "
        f"pero NO tienen entrada en COMENTARIOS_COLUMNAS:\n"
        f"  {sorted(faltantes_en_comentarios)}\n"
        f"Actualizar COMENTARIOS_COLUMNAS antes de continuar."
    )

if sobrantes_en_comentarios:
    raise AssertionError(
        f"Las siguientes tablas están en COMENTARIOS_COLUMNAS "
        f"pero NO están en TABLAS_MODELO_DATOS:\n"
        f"  {sorted(sobrantes_en_comentarios)}\n"
        f"Verificar si se agregó una tabla nueva al modelo o si es un error de tipeo."
    )

print("✓ Paridad verificada: COMENTARIOS_COLUMNAS cubre exactamente las 21 tablas del modelo.")
print(f"  Tablas cubiertas: {sorted(claves_comentarios)}")

# COMMAND ----------
# MAGIC %md ## 7. Aplicación de comentarios por medalla

# COMMAND ----------
# MAGIC %md ## Bronce

# COMMAND ----------
# DBTITLE 1,Aplicar comentarios Bronce
resultados_bronce = []

for tabla in sorted(t for t in TABLAS_MODELO_DATOS if _MEDALLA_MAP[t][0] == CAT_B):
    comentario_tabla = COMENTARIOS_TABLAS.get(tabla, f"Tabla {tabla} del modelo LSDP.")
    comentarios_cols = COMENTARIOS_COLUMNAS.get(tabla, {})
    print(f"[Bronce] Procesando {CAT_B}.{ESQ_B}.{tabla} ...")
    resultado = aplicar_comentarios(CAT_B, ESQ_B, tabla, comentario_tabla, comentarios_cols)
    resultados_bronce.append({
        "medalla":        "Bronce",
        "tabla":          tabla,
        "estado_tabla":   resultado["estado_tabla"],
        "cols_correctas": resultado["cols_correctas"],
        "cols_omitidas":  resultado["cols_omitidas"],
        "mensaje":        resultado["mensaje"][:200],
    })
    estado_icon = "✓" if resultado["estado_tabla"] == "OK" else "✗"
    print(
        f"  {estado_icon} estado_tabla={resultado['estado_tabla']} | "
        f"cols_correctas={resultado['cols_correctas']} | "
        f"cols_omitidas={resultado['cols_omitidas']}"
    )

print(f"\nBronce: {len(resultados_bronce)} tabla(s) procesada(s)")

# COMMAND ----------
# MAGIC %md ## Plata

# COMMAND ----------
# DBTITLE 1,Aplicar comentarios Plata
resultados_plata = []

for tabla in sorted(t for t in TABLAS_MODELO_DATOS if _MEDALLA_MAP[t][0] == CAT_P):
    comentario_tabla = COMENTARIOS_TABLAS.get(tabla, f"Tabla {tabla} del modelo LSDP.")
    comentarios_cols = COMENTARIOS_COLUMNAS.get(tabla, {})
    print(f"[Plata] Procesando {CAT_P}.{ESQ_P}.{tabla} ...")
    resultado = aplicar_comentarios(CAT_P, ESQ_P, tabla, comentario_tabla, comentarios_cols)
    resultados_plata.append({
        "medalla":        "Plata",
        "tabla":          tabla,
        "estado_tabla":   resultado["estado_tabla"],
        "cols_correctas": resultado["cols_correctas"],
        "cols_omitidas":  resultado["cols_omitidas"],
        "mensaje":        resultado["mensaje"][:200],
    })
    estado_icon = "✓" if resultado["estado_tabla"] == "OK" else "✗"
    print(
        f"  {estado_icon} estado_tabla={resultado['estado_tabla']} | "
        f"cols_correctas={resultado['cols_correctas']} | "
        f"cols_omitidas={resultado['cols_omitidas']}"
    )

print(f"\nPlata: {len(resultados_plata)} tabla(s) procesada(s)")

# COMMAND ----------
# MAGIC %md ## Oro

# COMMAND ----------
# DBTITLE 1,Aplicar comentarios Oro
resultados_oro = []

for tabla in sorted(t for t in TABLAS_MODELO_DATOS if _MEDALLA_MAP[t][0] == CAT_O):
    comentario_tabla = COMENTARIOS_TABLAS.get(tabla, f"Tabla {tabla} del modelo LSDP.")
    comentarios_cols = COMENTARIOS_COLUMNAS.get(tabla, {})
    print(f"[Oro] Procesando {CAT_O}.{ESQ_O}.{tabla} ...")
    resultado = aplicar_comentarios(CAT_O, ESQ_O, tabla, comentario_tabla, comentarios_cols)
    resultados_oro.append({
        "medalla":        "Oro",
        "tabla":          tabla,
        "estado_tabla":   resultado["estado_tabla"],
        "cols_correctas": resultado["cols_correctas"],
        "cols_omitidas":  resultado["cols_omitidas"],
        "mensaje":        resultado["mensaje"][:200],
    })
    estado_icon = "✓" if resultado["estado_tabla"] == "OK" else "✗"
    print(
        f"  {estado_icon} estado_tabla={resultado['estado_tabla']} | "
        f"cols_correctas={resultado['cols_correctas']} | "
        f"cols_omitidas={resultado['cols_omitidas']}"
    )

print(f"\nOro: {len(resultados_oro)} tabla(s) procesada(s)")

# COMMAND ----------
resultados = resultados_bronce + resultados_plata + resultados_oro
print(f"\nTotal tablas procesadas: {len(resultados)}")

# COMMAND ----------
# MAGIC %md ## 8. DataFrame resumen de resultados

# COMMAND ----------
# DBTITLE 1,Mostrar resumen final como DataFrame
from pyspark.sql import Row

df_resumen = spark.createDataFrame([Row(**r) for r in resultados])

# Ordenar por medalla (Bronce → Plata → Oro) y luego por nombre de tabla
_ORDEN_MEDALLA = {"Bronce": 1, "Plata": 2, "Oro": 3}
from pyspark.sql import functions as F

df_resumen = (
    df_resumen
    .withColumn(
        "_orden_medalla",
        F.when(F.col("medalla") == "Bronce", 1)
         .when(F.col("medalla") == "Plata",  2)
         .otherwise(3)
    )
    .orderBy("_orden_medalla", "tabla")
    .drop("_orden_medalla")
)

total_tablas        = df_resumen.count()
tablas_ok           = df_resumen.filter(F.col("estado_tabla") == "OK").count()
total_cols_ok       = df_resumen.agg(F.sum("cols_correctas")).collect()[0][0]
total_cols_omitidas = df_resumen.agg(F.sum("cols_omitidas")).collect()[0][0]

print("=" * 60)
print(f"RESUMEN DE APLICACIÓN DE COMENTARIOS")
print("=" * 60)
print(f"  Tablas procesadas     : {total_tablas}")
print(f"  Tablas OK             : {tablas_ok} / {total_tablas}")
print(f"  Columnas comentadas   : {total_cols_ok}")
print(f"  Columnas omitidas     : {total_cols_omitidas}")
print("=" * 60)

display(df_resumen)
