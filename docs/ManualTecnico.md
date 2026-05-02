# Manual Técnico — LSDP Lab DataVault DWH

**Proyecto**: LSDP Lab DataVault DWH  
**Autor**: LSDP Lab DataVault DWH  
**Fecha**: 2026-05-01  
**Spec activo**: [documentacion-consolidada-y-metadata](../.kiro/specs/documentacion-consolidada-y-metadata/spec.json)  
**Plataforma**: Databricks Free Edition · Serverless Compute · Unity Catalog · Lakeflow Spark Declarative Pipelines

---

## Índice

1. [Arquitectura Medallón y Decisiones Técnicas](#1-arquitectura-medallón-y-decisiones-técnicas)
2. [Patrones de Carga en Plata: Auto CDC SCD=1, append_flow y Helpers DV2.0](#2-patrones-de-carga-en-plata-auto-cdc-scd1-append_flow-y-helpers-dv20)
3. [Temporalidad: Streaming Tables vs Materialized Views](#3-temporalidad-streaming-tables-vs-materialized-views)
4. [Restricciones Serverless y Reglas ANSI Mode](#4-restricciones-serverless-y-reglas-ansi-mode)
5. [Patrón CDF: vista_trxpfl_cdf](#5-patrón-cdf-vista_trxpfl_cdf)
6. [Incrementalidad de Hec_Transacciones_ATM](#6-incrementalidad-de-hec_transacciones_atm)
7. [Utilities del Pipeline](#7-utilities-del-pipeline)
8. [Tests y Calidad](#8-tests-y-calidad)

---

## 1. Arquitectura Medallón y Decisiones Técnicas

### Capas de datos

El pipeline implementa una arquitectura medallón de tres capas, cada una con responsabilidades
claramente separadas:

| Capa | Propósito | Tipo LSDP principal |
|------|-----------|-------------------|
| **Bronce** | Ingesta incremental desde Landing Zone (Volume UC). Preserva el dato origen sin transformar. | `@dp.table()` (Streaming Table) |
| **Plata** | Normalización en Data Vault 2.0 Raw Vault. Trazabilidad completa de cada cambio. | `dp.create_streaming_table()` + `@dp.append_flow()` / Auto CDC |
| **Oro** | Modelo Estrella optimizado para análisis. Versión única del dato (SCD Tipo 1). | `@dp.materialized_view()` |

### Aislamiento de catálogos por capa

El sistema usa **tres catálogos distintos** en Unity Catalog para garantizar gobernanza
independiente y permisos granulares:

| Parámetro | Capa | Descripción |
|-----------|------|-------------|
| `pipeline.catalogo` + `pipeline.esquema` | Bronce | Catálogo de ingesta cruda |
| `pipeline.catalogo_plata` + `pipeline.esquema_plata` | Plata | Catálogo Data Vault 2.0 |
| `pipeline.catalogo_oro` + `pipeline.esquema_oro` | Oro | Catálogo de consumo analítico |

### Fuentes de datos

Tres archivos Parquet ubicados en un **Unity Catalog Volume**:

```
/Volumes/{catalogo}/{esquema}/{volumen}/{ruta_cmstfl}/
/Volumes/{catalogo}/{esquema}/{volumen}/{ruta_trxpfl}/
/Volumes/{catalogo}/{esquema}/{volumen}/{ruta_blncfl}/
```

Los Parquets son exportaciones incrementales del sistema AS400 bancario y se depositan en
subdirectorios particionados por `año/mes/dia`.

### Decisión de usar Lakeflow Spark Declarative Pipelines (LSDP)

LSDP fue elegido porque:
- Permite mezclar semántica **streaming** (append_flow) con **refresh completo** (MV) en el
  mismo pipeline, con orquestación automática del grafo DAG.
- Gestiona automáticamente la **Change Data Feed** (CDF) de Delta Lake para detectar incrementos.
- En **Serverless** no hay gestión de clúster: Databricks asigna recursos automáticamente.
- La declaratividad garantiza **idempotencia**: reiniciar el pipeline desde cero produce el
  mismo resultado final.

---

## 2. Patrones de Carga en Plata: Auto CDC SCD=1, append_flow y Helpers DV2.0

### 2.1 OPT-001: Auto CDC SCD Tipo 1

Aplicado a Hubs y Links del linaje CMSTFL/BLNCFL:
`Hub_Cliente`, `Hub_Operacion`, `Link_Cliente_Operacion`.

**Implementación**:
```python
dp.create_streaming_table(
    name=f"{cat}.{esq}.Hub_Cliente",
    cluster_by=["Hash_Cliente", "FechaRegistro"]
)

@dp.view(name="Hub_Cliente_staged")
def staged():
    # ... transformaciones ...
    return df_hub

dp.create_auto_cdc_flow(
    name=f"{cat}.{esq}.Hub_Cliente",
    source="Hub_Cliente_staged",
    keys=["Hash_Cliente"],
    sequence_by="FechaRegistro",
    stored_as_scd_type=1,
)
```

**Por qué OPT-001 para Hubs y Links CMSTFL/BLNCFL**:
El origen AS400 envía snapshots completos del maestro de clientes y saldos en cada incremento.
El Auto CDC con `sequence_by="FechaRegistro"` garantiza que solo se procesen inserciones
nuevas (cuando `FechaRegistro` aumenta), actualizando el Hub sin duplicar llaves.

### 2.2 append_flow puro para el linaje transaccional

Aplicado a `Hub_Transaccion`, `Link_Cliente_Transaccion`, `Sat_Transaccion_DatosEstables`,
`Sat_Transaccion_Montos`.

**Implementación**:
```python
dp.create_streaming_table(
    name=f"{cat}.{esq}.Hub_Transaccion",
    cluster_by=["FechaRegistro", "Hash_Transaccion"]
)

@dp.append_flow(target=f"{cat}.{esq}.Hub_Transaccion")
def hub_transaccion():
    return spark.readStream.table("vista_trxpfl_cdf") \
        .select(...)
```

**Por qué append_flow puro para Transacciones**:
`TRXID` es globalmente único y no se modifica en el sistema AS400. La fuente
`vista_trxpfl_cdf` entrega solo los eventos del último commit CDF. La combinación de ambas
garantías hace innecesario cualquier LEFT ANTI JOIN de deduplicación.

### 2.3 procesar_satellite() para Satellites con SCD 2

Los Satellites de Cliente y Operación capturan **historial de cambios** (SCD2 simplificado):
cada vez que algún campo de negocio cambia, se inserta una nueva fila con el nuevo
`Hash_Diferenciador`.

**Lógica de `procesar_satellite()` en `LSDPUtilidadPrincipal.py`**:
```python
# Pseudocódigo conceptual
def procesar_satellite(spark, cat_plata, esq_plata, nombre_sat,
                       hash_col, datos_nuevos):
    existentes = spark.read.table(f"{cat_plata}.{esq_plata}.{nombre_sat}")
    # LEFT JOIN por Hash_Diferenciador (SHA2-512 de todos los campos)
    # Si no existe = nuevo registro → insertar
    # Si existe = sin cambio → no insertar
    nuevos = datos_nuevos.join(existentes,
                               on="Hash_Diferenciador",
                               how="left_anti")
    return nuevos
```

### 2.4 procesar_hub() y procesar_link()

Funciones en `LSDPUtilidadPrincipal.py` para Hubs y Links cuando se requiere LEFT ANTI JOIN
explícito (no OPT-001). Actualmente no invocadas por los notebooks del linaje transaccional
(que usan append_flow sobre CDF). Disponibles para futuros pipelines.

---

## 3. Temporalidad: Streaming Tables vs Materialized Views

### Tabla de referencia rápida

| Tipo LSDP | Cuándo usar | Semántica | ¿Acumula datos? |
|-----------|-------------|-----------|-----------------|
| `@dp.table()` (ST) | Ingesta incremental sin fin, append-only | Micro-batch streaming | Sí — acumula commits |
| `dp.create_streaming_table()` | Entidades DV2.0 con múltiples flujos | Streaming con múltiples append_flow | Sí — acumula commits |
| `@dp.materialized_view()` | Modelo Estrella, agregaciones, joins | Refresh incremental o completo vía CDF | Depende del plan |
| `@dp.view` | Vista auxiliar efímera dentro del pipeline | No persiste nada | No |

### Propiedades obligatorias en todas las Streaming Tables de Plata

```python
properties = {
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days",
}
```

`delta.enableChangeDataFeed = true` es **obligatorio** en todas las entidades Plata porque
permite que las Vistas Materializadas de Oro lean solo los cambios incrementales sin releer
el Streaming Table completo en cada refresh.

### Dim_Tiempo: caso especial de MV incremental

`Dim_Tiempo` usa `@dp.materialized_view` con refresh incremental (Enzyme CDF). Solo aplica
operadores determinísticos: `select`, `distinct`, `withColumn`, `F.year()`, `F.month()`, etc.
No admite `orderBy`, `sort` ni `Window` en el contexto de refresh incremental Enzyme.

### Hec_Transacciones_ATM: elegibilidad para refresh incremental

`Hec_Transacciones_ATM` lee de `Trx_ATM_Stream` (Streaming Table con CDF activo). Databricks
puede detectar que el plan es eligible para refresh incremental automáticamente. El plan es
un `select` simple sin funciones no determinísticas, lo que garantiza idempotencia.

---

## 4. Restricciones Serverless y Reglas ANSI Mode

### Restricciones de Serverless Compute

El pipeline corre en **Serverless Compute**. Las siguientes operaciones están **prohibidas**:

| Prohibición | Alternativa |
|-------------|------------|
| `.cache()` / `.persist()` | No aplicar; LSDP gestiona materialización automáticamente |
| `.rdd` / operaciones RDD | Usar DataFrame API únicamente |
| UDFs Python sin serialización | Usar funciones nativas de `pyspark.sql.functions` |
| `spark.sparkContext.*` | No acceder al SparkContext directamente |
| `spark.conf.get()` para parámetros de pipeline | Usar `dbutils.widgets.get()` en notebooks de exploración |

### ANSI Mode (activado por defecto en Serverless)

Serverless activa ANSI SQL Mode. Impacto en el código:

**Operador `+` sobre strings prohibido** — usar `F.concat_ws`:
```python
# ❌ Incorrecto en ANSI Mode
F.col("nombre") + " " + F.col("apellido")

# ✅ Correcto
F.concat_ws(" ", F.col("nombre"), F.col("apellido"))
```

**División por cero lanza excepción** — proteger con `when`:
```python
# ✅
F.when(F.col("denominador") != 0, F.col("numerador") / F.col("denominador")).otherwise(None)
```

**Overflow de LONG lanza excepción** — usar `F.abs(F.hash(...).cast("long"))`:
```python
# Para llaves subrogadas — el campo puede ser negativo, ambos son válidos
F.xxhash64(hash_col).cast("long")   # permite negativos
F.abs(F.hash(...).cast("long"))     # fuerza positivo (menos eficiente)
```

### Patrones de Hash

| Función | Bits | Uso |
|---------|------|-----|
| `F.sha2(col.cast("string"), 256)` | SHA2-256 | Hubs y Links — una sola columna de negocio |
| `F.sha2(F.concat_ws("\|", col1, col2, ...), 256)` | SHA2-256 | Links — múltiples columnas con separador `\|` |
| `F.sha2(F.concat_ws("\|", hash, *campos_negocio), 512)` | SHA2-512 | Satellites — Hash_Diferenciador |
| `F.xxhash64(hash_col).cast("long")` | 64-bit | Llaves subrogadas de Oro — determinístico, puede ser negativo |

---

## 5. Patrón CDF: vista_trxpfl_cdf

### Problema que resuelve

TRXPFL es la tabla de mayor volumen (~7M registros). Los notebooks de Plata para el linaje
transaccional (Hub_Transaccion, Link_Cliente_Transaccion, Sat_Transaccion_*) necesitan
leer solo los registros del último incremento, no toda la tabla.

### Implementación

```python
# LSDPPlataVistaTRXPFLCDF.py
@dp.view(name="vista_trxpfl_cdf")
def vista_trxpfl_cdf():
    return (
        spark.readStream
             .option("readChangeFeed", "true")
             .option("startingVersion", "latest")
             .table(f"{catalogo}.{esquema}.TRXPFL")
             .filter(F.col("_change_type").isin(["insert", "update_postimage"]))
             .withColumn("VersionCarga", F.col("_commit_version"))
             .withColumn("FechaCargaBronce", F.col("_commit_timestamp"))
             .drop("_change_type", "_commit_version", "_commit_timestamp")
    )
```

### Columnas adicionales de trazabilidad

La vista expone dos columnas de trazabilidad CDF que se propagan hasta los Satellites
transaccionales:

| Columna | Origen CDF | Descripción |
|---------|-----------|-------------|
| `VersionCarga` | `_commit_version` | Versión Delta de TRXPFL en la que llegó la transacción |
| `FechaCargaBronce` | `_commit_timestamp` | Timestamp del commit en TRXPFL |

Estas columnas permiten auditar exactamente cuándo y en qué commit de Bronce fue procesada
cada transacción.

### Por qué `readChangeFeed` + `startingVersion = latest`

- `readChangeFeed=true` activa la lectura del log CDF de Delta Lake en lugar del dato completo.
- `startingVersion=latest` garantiza que en cada micro-batch solo se procesen las filas
  nuevas del último commit, no todo el historial.
- El filtro `_change_type IN ('insert', 'update_postimage')` descarta deletes y pre-images.

---

## 6. Incrementalidad de Hec_Transacciones_ATM

### Grafo de dependencias de Oro

```
Sat_Transaccion_DatosEstables ──┐
Sat_Transaccion_Montos         ──┤──► Trx_ATM_Stream (ST temp)
Hub_Transaccion                ──┤                    │
Link_Cliente_Transaccion       ──┘                    │
                                                      ▼
Link_Cliente_Operacion ──┐                Hec_Transacciones_ATM (MV)
Dim_Cliente            ──┤──► Map_Cliente_Operacion_Dominante (MV temp)
Dim_Operacion          ──┘
```

### Trx_ATM_Stream — Pre-composición transaccional

`Trx_ATM_Stream` (`@dp.table(temporary=True)`, `@dp.append_flow()`) es el dataset más crítico
del pipeline. Su propósito es construir una fila completa por transacción ATM antes de la
materialización de Hec_Transacciones_ATM:

1. Lee `Sat_Transaccion_DatosEstables` + `Sat_Transaccion_Montos` (join por `Hash_Transaccion`)
2. Une `Hub_Transaccion` (para obtener `IdentificadorTransaccion`)
3. Une `Link_Cliente_Transaccion` (para resolver `Hash_Cliente`)
4. **Filtra** `TipoTransaccion IN ('DATM', 'CATM')` — solo ATM
5. Pre-calcula `DimIdCliente = xxhash64(Hash_Cliente)` y `DimIdOperacion` via join con
   `Map_Cliente_Operacion_Dominante`

Al ser una Streaming Table con `@dp.append_flow()`, Trx_ATM_Stream acumula filas incrementalmente.

### Map_Cliente_Operacion_Dominante — Operación dominante por cliente

`Map_Cliente_Operacion_Dominante` (`@dp.materialized_view(temporary=True)`) resuelve el
problema de que un cliente puede tener múltiples operaciones (cuentas) en BLNCFL. Para
la tabla de hechos se necesita una única `DimIdOperacion` por cliente.

**Lógica de selección**:
```python
# LSDPOroMapClienteOperacionDominante.py
# seleccionar_operacion_dominante() en LSDPUtilidadOro.py
df_map = (
    df_link_cli_op
    .join(df_hub_op, on="Hash_Operacion")
    .groupBy("Hash_Cliente")
    .agg(
        F.max(
            F.struct(
                F.col("SaldoTotal"),    # columna de desempate primario
                F.col("Hash_Operacion") # desempate secundario determinístico
            )
        ).alias("dominante")
    )
    .select("Hash_Cliente",
            F.col("dominante.Hash_Operacion").alias("Hash_Operacion"))
)
```

**¿Por qué `groupBy().agg(max(struct(...)))` en lugar de Window?**
En Serverless, los operadores `Window` con rangos ilimitados pueden generar planes ineficientes.
`max(struct(...))` es un agregado estándar Spark, determinístico y sin spill en Serverless.
La semántica es equivalente: selecciona la operación con mayor saldo total; en caso de empate,
el `Hash_Operacion` lexicográficamente mayor (determinístico).

---

## 7. Utilities del Pipeline

### 7.1 LSDPConfiguracion.py

**Ubicación**: `src/LSDP_Lab_DataVault_DWH/utilities/LSDPConfiguracion.py`

Centraliza la obtención de parámetros del pipeline DLT y las constantes de negocio.

**Función principal**:
```python
def obtener_configuracion(spark) -> dict:
    """Retorna los 13 parámetros del pipeline más constantes de negocio."""
    cfg = spark.conf  # accede a spark.conf.get() dentro del contexto DLT
    return {
        # Bronce
        "catalogo": cfg.get("pipeline.catalogo"),
        "esquema":  cfg.get("pipeline.esquema"),
        "volumen":  cfg.get("pipeline.volumen"),
        # Plata
        "catalogo_plata": cfg.get("pipeline.catalogo_plata"),
        "esquema_plata":  cfg.get("pipeline.esquema_plata"),
        # Oro
        "catalogo_oro": cfg.get("pipeline.catalogo_oro"),
        "esquema_oro":  cfg.get("pipeline.esquema_oro"),
        # Rutas de fuentes (relativas al Volume)
        "ruta_cmstfl":            cfg.get("pipeline.ruta_cmstfl"),
        "ruta_trxpfl":            cfg.get("pipeline.ruta_trxpfl"),
        "ruta_blncfl":            cfg.get("pipeline.ruta_blncfl"),
        "schema_location_cmstfl": cfg.get("pipeline.schema_location_cmstfl"),
        "schema_location_trxpfl": cfg.get("pipeline.schema_location_trxpfl"),
        "schema_location_blncfl": cfg.get("pipeline.schema_location_blncfl"),
    }
```

**Constantes**:
- `TIPO_DATM = "DATM"` — código retiro ATM
- `TIPO_CATM = "CATM"` — código depósito ATM
- `HASH_HUB_LINK_BITS = 256` — SHA2 bits para Hubs/Links
- `HASH_SATELLITE_BITS = 512` — SHA2 bits para Satellite Diferenciador
- `HASH_SEPARATOR = "|"` — separador en concat_ws para hashes compuestos

**Umbrales de clasificación** (retornados como parte del dict de configuración):
- `UMBRAL_RANGO_ETARIO` — lista de rangos para clasificar edad
- `UMBRAL_CATEGORIA_INGRESOS` — rangos de ingresos mensuales
- `UMBRAL_CATEGORIA_SALDO` — rangos de saldo disponible
- `UMBRAL_UTILIZACION_CREDITO` — porcentajes de uso de crédito
- `UMBRAL_SOBREGIRO` — montos de sobregiro
- `UMBRAL_RANGO_MONTO` — rangos del monto de transacción
- `UMBRAL_RIESGO_FRAUDE` — escala de riesgo 0–100

### 7.2 LSDPUtilidadPrincipal.py

**Ubicación**: `src/LSDP_Lab_DataVault_DWH/utilities/LSDPUtilidadPrincipal.py`

| Función | Firma | Uso |
|---------|-------|-----|
| `calcular_hash_hub` | `(columnas, bits=256, separador="\|")` | SHA2-N de una o más columnas de negocio |
| `calcular_hash_diferenciador` | `(hash_entidad, *campos)` | SHA2-512 para Hash_Diferenciador de Satellites |
| `reordenar_columnas_lc` | `(df, columnas_lc)` | Mueve columnas de Liquid Clustering al principio |
| `procesar_satellite` | `(spark, cat_plata, esq_plata, nombre_sat, hash_col, datos_nuevos)` | Deduplicación por Hash_Diferenciador para Satellites de Cliente y Operación |
| `procesar_hub` | `(spark, cat_plata, esq_plata, nombre_hub, columnas_llave, datos_nuevos)` | LEFT ANTI JOIN por llave de Hub |
| `procesar_link` | `(spark, cat_plata, esq_plata, nombre_link, columnas_hash, datos_nuevos)` | LEFT ANTI JOIN por hashes del Link |
| `procesar_satellite_transaccional` | `(spark, cat_plata, esq_plata, nombre_sat, hash_col, fecha_col, datos_nuevos)` | LEFT ANTI JOIN por `[hash_col]`; preservada para futuros usos |
| `clasificar_por_umbral` | `(columna, umbrales)` | Clasificación por rangos vía `when().otherwise()` |

### 7.3 LSDPUtilidadOro.py

**Ubicación**: `src/LSDP_Lab_DataVault_DWH/utilities/LSDPUtilidadOro.py`

| Función | Firma | Uso |
|---------|-------|-----|
| `obtener_ultimo_por_hash` | `(df, hash_col, orden_col="FechaRegistro")` | ROW_NUMBER partitionBy hash_col, orderBy FechaRegistro DESC; desempate por Hash_Diferenciador DESC. Obtiene el Satellite más reciente por entidad. |
| `asignar_dim_id_estable` | `(df, hash_col, id_col)` | `F.xxhash64(hash_col).cast("long")` — llave subrogada determinística de Oro |
| `seleccionar_operacion_dominante` | `(df_hub_op, df_link_cli_op)` | `groupBy(Hash_Cliente).agg(max(struct(SaldoTotal, Hash_Operacion)))` |
| `validar_columnas_oro` | `(df, columnas_esperadas)` | Verifica presencia de columnas; lanza ValueError si faltan |

---

## 8. Tests y Calidad

### Cobertura de tests

Los tests están en `tests/` y validan el pipeline con datos mockeados en PySpark local.

| Archivo de test | Cobertura |
|-----------------|-----------|
| `test_configuracion.py` | `obtener_configuracion()` — 13 parámetros, valores por defecto |
| `test_notebooks_bronce.py` | Lógica de transformación de CMSTFL, TRXPFL, BLNCFL |
| `test_notebooks_exploracion.py` | Notebooks generadores de Parquets |
| `test_notebooks_oro.py` | Dim_Cliente, Dim_Operacion, Dim_Tiempo, Hec_Transacciones_ATM |
| `test_notebooks_plata.py` | Todos los Hubs, Links y Satellites de Plata |
| `test_utilidad_oro.py` | `obtener_ultimo_por_hash`, `asignar_dim_id_estable`, `seleccionar_operacion_dominante` |
| `test_utilidad_principal.py` | `calcular_hash_hub`, `calcular_hash_diferenciador`, `procesar_satellite`, `procesar_hub` |
| `test_utilidades_plata.py` | `clasificar_por_umbral`, `reordenar_columnas_lc` |

### Data Quality Expectations

Cada entidad de Plata y Oro define expectations en el decorador LSDP. Las reglas siguen la
convención:

- **`ON VIOLATION FAIL UPDATE`**: El batch completo falla si algún registro viola la regla.
  Usado para llaves primarias nulas (`IS NOT NULL`) y rangos críticos.
- **`ON VIOLATION WARN`**: El batch continúa pero queda registrado en el log de calidad.
  Usado para validaciones opcionales como rangos de año.

**Ejemplo en Hec_Transacciones_ATM**:
```python
@dp.materialized_view(
    name=f"{cat_oro}.{esq_oro}.Hec_Transacciones_ATM",
    expectations={
        "DimIdCliente_not_null": "DimIdCliente IS NOT NULL",
        "TRXID_not_null": "IdentificadorTransaccion IS NOT NULL",
        "FechaClave_not_null": "FechaClave IS NOT NULL",
        "TipoTransaccion_ATM": "TipoTransaccion IN ('DATM', 'CATM')",
    },
    violations={
        "DimIdCliente_not_null": "FAIL UPDATE",
        "TRXID_not_null": "FAIL UPDATE",
        "FechaClave_not_null": "FAIL UPDATE",
        "TipoTransaccion_ATM": "FAIL UPDATE",
    }
)
```

---

_Documento generado durante el incremento `documentacion-consolidada-y-metadata` · 2026-05-01_  
_Mantenido en: [docs/ManualTecnico.md](./ManualTecnico.md)_  
_Ver también_: [Modelo de Datos](./ModeloDatos.md) · [Quickstart](./Quickstart.md)
