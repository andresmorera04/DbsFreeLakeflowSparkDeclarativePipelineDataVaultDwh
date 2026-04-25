# Stack Tecnológico

## Arquitectura

Pipeline declarativo de datos con **Arquitectura Medallón** (Bronce → Plata → Oro), orquestado por LSDP sobre Databricks Free Edition Serverless. Almacenamiento exclusivo en Delta Lake dentro de Unity Catalog.

## Tecnologías Principales

- **Lenguaje**: PySpark (Python)
- **Framework de Pipelines**: Lakeflow Spark Declarative Pipelines (LSDP) — anteriormente Delta Live Tables (DLT)
- **Plataforma**: Databricks Free Edition con Serverless Compute
- **Catálogo**: Unity Catalog (metadatos, permisos, linaje)
- **Formato**: Delta Lake
- **Orquestación**: Lakeflow Jobs
- **Ingesta**: AutoLoader (`cloudFiles`)

## Import Obligatorio de LSDP

```python
# CORRECTO — Módulo nativo del runtime de Databricks
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# INCORRECTO — SDK REST, NO tiene decoradores @dp.table
# import databricks.sdk.pipelines as dp  ← NUNCA USAR
```

## API de Decoradores LSDP

| Decorador | Uso | Nota Clave |
|-----------|-----|------------|
| `@dp.table()` | Streaming Table (incremental con `spark.readStream`) | `temporary=True` para tablas no registradas en UC |
| `@dp.materialized_view()` | Vista materializada (batch con `spark.read`) | Nombre de 3 partes en `name=`; **NO** usar `catalog=`/`schema=` separados |
| `@dp.temporary_view()` | Vista temporal solo durante ejecución | No persiste en UC |
| `dp.create_streaming_table()` | Crear ST programáticamente | Expectations se definen aquí, no en `@dp.append_flow` |
| `@dp.append_flow()` | Flujo Append hacia ST existente | Para inserción incremental acumulativa |

## Restricciones Serverless (CRÍTICO)

### Prohibiciones Absolutas

- **NO** `.cache()` / `.persist()` → `NOT_SUPPORTED_WITH_SERVERLESS`
- **NO** `spark.sparkContext` / `sc.` → no existe en Serverless
- **NO** operaciones RDD: `.rdd`, `.parallelize()`, `.mapPartitions()`, `.foreachPartition()`, `.toLocalIterator()`
- **NO** `sc.broadcast()` → usar `F.broadcast(df)` como join hint
- **NO** `sc.accumulator()` / `sc.longAccumulator()` → usar agregaciones DataFrame
- **NO** UDFs → usar siempre funciones nativas `pyspark.sql.functions.*`
- **NO** threading / multiprocessing
- **NO** cambiar `spark.sql.ansi.enabled` (viene `true` por defecto)
- **NO** `spark.conf.set()` excepto `spark.sql.shuffle.partitions` y `spark.sql.adaptive.*`

### Reglas ANSI Mode

- `F.hash()` retorna `IntegerType` (32 bits). **SIEMPRE**: `F.abs(F.hash(...).cast("long"))` — cast a `long` ANTES de `abs()`.
- Operador `+` en Column es suma aritmética, no concatenación. **SIEMPRE**: usar `F.concat()` o `F.concat_ws()` para strings.
- Literales grandes (>2B): usar `.cast("long")` explícito.

## Patrones de Hash

| Contexto | Algoritmo | Patrón |
|----------|-----------|--------|
| Hubs / Links | SHA2-256 | `F.sha2(col.cast("string"), 256)` |
| Hash_Diferenciador (Satellites) | SHA2-512 | `F.sha2(F.concat_ws("\|", hash_entidad, *campos), 512)` |
| Llave compuesta | SHA2-256 | `F.sha2(F.concat_ws("\|", col1.cast("string"), col2.cast("string")), 256)` |
| Separador | Pipe `\|` | Constante `HASH_SEPARATOR = "\|"` |

## Parametrización del Pipeline

Los parámetros se configuran en el JSON del pipeline y se acceden con `spark.conf.get()`:

```python
catalogo = spark.conf.get("pipeline.catalogo")          # Bronce
esquema = spark.conf.get("pipeline.esquema")
catalogo_plata = spark.conf.get("pipeline.catalogo_plata")  # Plata
esquema_plata = spark.conf.get("pipeline.esquema_plata")
catalogo_oro = spark.conf.get("pipeline.catalogo_oro")      # Oro
esquema_oro = spark.conf.get("pipeline.esquema_oro")
ruta_base = spark.conf.get("pipeline.ruta_base")
ruta_base_autoloader = spark.conf.get("pipeline.ruta_base_autoloader")
```

## Decisiones Técnicas Clave

1. **LSDP sobre código imperativo** — El framework declarativo gestiona dependencias, reintentos y linaje automáticamente.
2. **SHA2 sobre F.hash()** — Para llaves de negocio se usa SHA2-256/512 (determinístico, sin colisiones prácticas). `F.hash()` solo donde se necesite hash simple con protección ANSI.
3. **Liquid Clustering sobre Z-Order** — Optimización nativa de Delta Lake; `FechaRegistro` siempre primera columna del cluster.
4. **Append Only en Data Vault** — Hubs, Links y Satellites nunca actualizan ni eliminan; solo insertan nuevos registros.
5. **ST única en Bronce** — Cada fuente de datos tiene una única Streaming Table persistente (AutoLoader directo). Se eliminó la arquitectura de 2 capas (ST temporal + MV snapshot). Las entidades de Plata leen Bronce con `dp.read_stream()` dentro de `@dp.append_flow()`.
6. **Todas las entidades de Plata como ST+AppendFlow** — Hubs, Links y Satellites usan `dp.create_streaming_table()` + `@dp.append_flow()`. No se usan Materialized Views en Plata. Deduplicación via:
   - **Hubs**: `procesar_hub()` — LEFT ANTI JOIN por columnas de llave de negocio.
   - **Links**: `procesar_link()` — LEFT ANTI JOIN por columnas de hash.
   - **Satellites de estado**: `procesar_satellite()` — LEFT JOIN+WHERE via `Hash_Diferenciador` con ROW_NUMBER.
   - **Satellites transaccionales**: `procesar_satellite_transaccional()` — LEFT ANTI JOIN por `[hash_col, fecha_col]`.
7. **Constantes centralizadas** — Todos los umbrales de negocio se definen en el notebook de configuración, nunca hard-coded en transformaciones.

## Trazabilidad de Lecturas en Plata

| Función en append_flow | Lee Bronce con | Lee tabla existente con |
|------------------------|-----------------|-------------------------|
| Hub / Link flow | `dp.read_stream(f"{cat}.{esq}.{Origen}")` | `spark.read.table()` dentro de `procesar_hub/link()` |
| Satellite flow | `dp.read_stream(f"{cat}.{esq}.{Origen}")` | `spark.read.table()` dentro de `procesar_satellite*()` |

## Expectations (Calidad de Datos)

| Tipo | Comportamiento |
|------|---------------|
| `@dp.expect()` | Warn — registra métricas, escribe el registro |
| `@dp.expect_or_drop()` | Drop — descarta registros inválidos |
| `@dp.expect_or_fail()` | Fail — aborta pipeline si hay violación |
| `@dp.expect_all_or_fail(dict)` | Fail grupal — validaciones múltiples en un decorador |

## Limitaciones de Databricks Free Edition

- Máximo 5 task concurrentes por job.
- 1 pipeline LSDP activo por tipo simultáneamente.
- Solo Python/PySpark (sin R ni Scala).

---
_Documenta estándares y patrones, no cada dependencia._
