# Propósito Archivo SYSTEM

Este archivo constituye la **fuente de verdad centralizada** (Single Source of Truth) del proyecto. Su función principal es alimentar el flujo completo del framework **cc-sdd** (Spec-Driven Development inspirado en Kiro), de tal manera que cada fase del ciclo AI-DLC — desde el Steering hasta la Implementación — disponga de un contexto exhaustivo, preciso y sin ambigüedades.

El documento está diseñado para que los comandos `/kiro-steering` y `/kiro-spec-init` lo consuman como entrada primaria y obligatoria. Los demás slash commands (`/kiro-spec-requirements`, `/kiro-spec-design`, `/kiro-spec-tasks`, `/kiro-spec-impl`, `/kiro-validate-*`) pueden referenciarlo opcionalmente o cuando el usuario lo indique de forma explícita.

**Criterio de calidad**: Toda información contenida aquí debe ser lo suficientemente detallada para que la IA genere artefactos (requirements.md, design.md, tasks.md) de alta precisión sin necesidad de solicitar aclaraciones adicionales al usuario.

---

# Dinámica con cc-sdd (Kiro GitHub)

## Relación entre SYSTEM.md y el Flujo SDD

El framework cc-sdd (https://github.com/gotalab/cc-sdd) implementa un ciclo de desarrollo de 3 fases con compuertas de aprobación humana:

```
Steering → Requirements (EARS) → Design (research.md + design.md) → Tasks (tasks.md) → Implementation
```

### Interacción de cada comando con este archivo

| Comando | Interacción con SYSTEM.md | Tipo |
|---------|---------------------------|------|
| `/kiro-steering` | Lee SYSTEM.md como fuente primaria para generar `product.md`, `tech.md`, `structure.md` | **Obligatoria** |
| `/kiro-spec-init` | Lee SYSTEM.md para extraer la descripción del feature y generar `spec.json` + `requirements.md` inicial | **Obligatoria** |
| `/kiro-spec-requirements` | Puede referenciar secciones específicas para contextualizar los requisitos EARS | Opcional |
| `/kiro-spec-design` | Puede referenciar la sección de Research, el Modelo de Datos y la Arquitectura Medallón para informar el `research.md` y `design.md` | Opcional |
| `/kiro-spec-tasks` | Puede referenciar las Reglas del Modelo de Datos para descomponer las tareas con mayor precisión | Opcional |
| `/kiro-spec-impl` | Puede referenciar las restricciones técnicas de Serverless y las prohibiciones de la sección Stack Técnico | Opcional |
| `/kiro-validate-*` | Puede validar la implementación contra las reglas definidas en este documento | Opcional |

### Artefactos generados por el flujo SDD

```
.kiro/
├── steering/
│   ├── product.md          ← Contexto de negocio (derivado de este SYSTEM.md)
│   ├── tech.md             ← Stack y restricciones técnicas
│   └── structure.md        ← Estructura del proyecto y convenciones
└── specs/
    └── <feature-name>/
        ├── spec.json       ← Metadatos y estado de la especificación
        ├── requirements.md ← Requisitos en formato EARS
        ├── research.md     ← Hallazgos de investigación (cuando aplica)
        ├── design.md       ← Diseño técnico con diagramas Mermaid
        └── tasks.md        ← Tareas con waves paralelas (P0, P1, ...)
```

---

---

# Objetivo del Laboratorio

## Descripción General

El objetivo es construir un laboratorio avanzado de ingeniería de datos que demuestre, de extremo a extremo, la construcción de un **Data Warehouse** sobre **Databricks Free Edition** con cómputo Serverless, utilizando **Lakeflow Spark Declarative Pipelines (LSDP)** como motor de orquestación. El laboratorio abarca desde la ingesta incremental de archivos Parquet en la zona de aterrizaje (Landing Zone) hasta la exposición de un **Modelo Estrella** listo para consumo analítico.

## Componentes Principales

1. **Ingesta incremental** con AutoLoader (Medalla de Bronce)
2. **Modelado Data Vault 2.0 — Raw Vault** con tablas Hub, Satellite y Link (Medalla de Plata)
3. **Modelo Estrella dimensional** con dimensiones y tabla de hechos (Medalla de Oro)
4. **Parametrización completa** sin valores hard-coded
5. **Compatibilidad total** con Databricks Free Edition Serverless Compute

## Arquitectura Medallón Detallada

### Medalla de Bronce — Ingesta Incremental Persistente

La medalla de Bronce se compone de **una única Streaming Table persistente** por cada fuente de datos de origen (CMSTFL, TRXPFL, BLNCFL). Esta tabla lee directamente desde AutoLoader (`cloudFiles`) y acumula todos los registros históricos de forma incremental.

#### Streaming Table Persistente (AutoLoader Directo)

- **Tipo de tabla LSDP**: Streaming Table Persistente (`@dp.table` sin `temporary=True`)
- **Mecanismo de ingesta**: AutoLoader (`cloudFiles`)
- **Comportamiento**: Ingesta de forma incremental todos los archivos Parquet nuevos depositados en la Landing Zone. Cada ejecución procesa únicamente los archivos no leídos previamente, gracias al checkpoint de AutoLoader.
- **Ruta de origen (parámetro)**: El pipeline recibe como parámetro la ruta base con el siguiente formato:  
  ```
  /Volumes/{Catalogo}/{Esquema}/{NombreVolume}/archivos/LSDP_DataVault_Dwh/As400/{NombreTablaOrigen}/
  ```
  La estructura de particionamiento físico sigue el patrón `año=YYYY/mes=MM/dia=DD/`, y Spark infiere las columnas de partición automáticamente mediante lazy evaluation.
- **Columna derivada `FechaRegistroParquet`**: Se genera una nueva columna `DATE` a partir de `año`, `mes` y `dia`, con formato `YYYY-MM-DD`. Representa la fecha en que los datos fueron depositados en la Landing Zone.
- **Columna `_rescued_data`**: AutoLoader con esquema evolutivo genera automáticamente la columna `_rescued_data` (StringType) para capturar datos que no coincidan con el esquema.
- **Liquid Clustering**: La Streaming Table usa **exclusivamente** `FechaRegistroParquet` como clave de Liquid Cluster.
- **Columnas exclusivas de Bronce**: Los campos `año`, `mes`, `dia`, `FechaRegistroParquet` y `_rescued_data` son de uso interno de Bronce. **NO se propagan a Plata ni a Oro**.
- **Lectura desde Plata**: Las capas superiores leen la tabla de Bronce mediante `dp.read_stream(f"{catalogo}.{esquema}.{Nombre}")` dentro de funciones `@dp.append_flow()`. **No** se usan tablas temporales intermedias.

> **Decisión de arquitectura**: Se eliminó el patrón anterior de 2 capas (ST temporal + MV con snapshot del último día). La nueva arquitectura usa una ST persistente única que acumula toda la historia. Las capas de Plata y Oro consumen la Bronce directamente vía `dp.read_stream()`, aplicando la lógica de selección de datos pertinentes dentro de sus propias transformaciones.

### Medalla de Plata — Data Vault 2.0 (Raw Vault)

La medalla de Plata implementa el **Raw Vault del modelado Data Vault 2.0**, compuesto por tres tipos de tablas: Hubs, Links y Satellites.

**Estrategia de tipos de tabla LSDP por entidad Data Vault:**

| Entidad DV2.0 | Tipo de tabla LSDP | Decorador/API | Justificación |
|---------------|-------------------|---------------|----------------|
| **Hub_Cliente**, **Hub_Operacion** | Streaming Table Acumulativa | `dp.create_streaming_table()` + `@dp.view` + `dp.create_auto_cdc_flow()` | Deduplicación cross-batch gestionada por el motor LSDP vía MERGE (SCD Type 1). Elimina el full scan O(histórico) que `procesar_hub()` requería en cada microbatch. `FechaRegistro` se incluye en el esquema (generada en el `@dp.view` con `F.current_timestamp()`) y se actualiza en cada MERGE (semántica "última vez vista"). **Nota**: `except_column_list` NO preserva columnas de updates — las excluye del esquema target; no se usa. |
| **Hub_Transaccion** | Streaming Table Acumulativa | `dp.create_streaming_table()` + `@dp.append_flow()` | TRXID es globalmente único entre ejecuciones; `procesar_hub()` con LEFT ANTI JOIN es suficiente y su coste es amortizado por el volumen bajo de re-apariciones. |
| **Link_Cliente_Operacion** | Streaming Table Acumulativa | `dp.create_streaming_table()` + `@dp.view` + `dp.create_auto_cdc_flow()` | Misma optimización que Hub_Cliente/Hub_Operacion: el par (Hash_Cliente, Hash_Operacion) se garantiza único mediante MERGE gestionado por el motor sin full scan. |
| **Link_Cliente_Transaccion** | Streaming Table Acumulativa | `dp.create_streaming_table()` + `@dp.append_flow()` | La relación Cliente↔Transacción es 1:1 con TRXID único; `procesar_link()` con LEFT ANTI JOIN es suficiente. |
| **Satellites** (9) | Streaming Table Acumulativa | `dp.create_streaming_table()` + `@dp.append_flow()` | Los Satellites son estrictamente **Append-Only** y acumulan historial indefinidamente. **Sat_Cliente_\*** y **Sat_Operacion_\***: la función `procesar_satellite()` detecta cambios vía `Hash_Diferenciador` con LEFT JOIN+WHERE (ROW_NUMBER para estado actual). **Sat_Transaccion_\***: flujo puro sin helper de deduplicación — `vista_trxpfl_cdf` entrega solo los eventos del último commit (CDF), y TRXID es globalmente único entre ejecuciones por diseño. `procesar_satellite_transaccional()` existe en LSDPUtilidadPrincipal pero no es invocada en la implementación actual. |

> **Decisión de diseño crítica**: Todos los elementos de Plata (Hubs, Links y Satellites) usan el patrón `dp.create_streaming_table()` como base. Esto garantiza que **ningún registro existente sea eliminado o reprocesado**, preservando la inmutabilidad histórica inherente al modelo Data Vault 2.0. La estrategia de escritura varía por entidad:
>
> **Herramientas de deduplicación**:
> - **Hub_Cliente, Hub_Operacion** (OPT-001): `dp.create_auto_cdc_flow(stored_as_scd_type=1)` con `@dp.view` como fuente — MERGE gestionado por el motor (O(delta), sin full scan). `FechaRegistro` se genera con `F.current_timestamp()` en el `@dp.view` y se actualiza en cada MERGE (semántica "última vez vista"). **`except_column_list` NO se usa**: excluye la columna del esquema target causando `DELTA_COLUMN_NOT_FOUND_IN_SCHEMA`.
> - **Hub_Transaccion**: `@dp.append_flow()` + función `procesar_hub(spark, catalogo_plata, esquema_plata, nombre_hub, columnas_llave, datos_nuevos)` — LEFT ANTI JOIN por `IdentificadorTransaccion` contra la tabla existente. `AnalysisException` para primera ejecución.
> - **Link_Cliente_Operacion** (OPT-001): `dp.create_auto_cdc_flow(stored_as_scd_type=1)` con `@dp.view` como fuente — MERGE gestionado por el motor (O(delta), sin full scan). Garantiza unicidad del par `(Hash_Cliente, Hash_Operacion)`. `FechaRegistro` se actualiza en cada MERGE.
> - **Link_Cliente_Transaccion**: `@dp.append_flow()` + función `procesar_link(spark, catalogo_plata, esquema_plata, nombre_link, columnas_hash, datos_nuevos)` — LEFT ANTI JOIN por columnas de hash. `AnalysisException` para primera ejecución.
> - **Satellites de estado (Cliente, Operación)**: función `procesar_satellite(spark, catalogo_plata, esquema_plata, nombre_sat, hash_col, datos_nuevos)` — LEFT JOIN+WHERE sobre `Hash_Diferenciador` con ROW_NUMBER para obtener el estado actual por entidad. Solo se insertan registros donde el hash cambió o la entidad no existe.
> - **Satellites transaccionales (Transacción)**: `@dp.append_flow()` puro — sin llamada a ningún helper de deduplicación. La fuente `vista_trxpfl_cdf` (CDF sobre TRXPFL) entrega solo los eventos del último commit; TRXID es globalmente único entre ejecuciones. `procesar_satellite_transaccional()` existe en LSDPUtilidadPrincipal.py pero **no es invocada** por los notebooks actuales (`LSDPPlataSatTransaccion.py`).
>
> **Lectura de Bronce**:
> - Entidades con `@dp.append_flow()`: leen Bronce con `dp.read_stream(f"{catalogo}.{esquema}.{Nombre}")` dentro del flujo.
> - Entidades con `dp.create_auto_cdc_flow()`: leen Bronce con `dp.read_stream(f"{catalogo}.{esquema}.{Nombre}")` dentro de la función decorada con `@dp.view`.
> - En ningún caso se usa `spark.read.table()` para leer Bronce.

> **Referencia bibliográfica**: El diseño del Data Vault 2.0 sigue los principios descritos en el libro *"Building a Scalable Data Warehouse with Data Vault 2.0"* (disponible en `Temporal/Building-a-Scalable-Data-Warehouse-with-Data-Vault-2.0.pdf`) y la guía oficial de Databricks: https://www.databricks.com/blog/what-is-data-vault

#### Tablas Hub (Entidades de Negocio)

Las tablas Hub almacenan las **llaves de negocio** de cada entidad. Representan el núcleo identitario de las entidades del dominio bancario.

**Estructura de columnas estándar**:

| Columna | Tipo de Dato | Descripción |
|---------|-------------|-------------|
| `{Campo(s)_LlaveNegocio}` | Tipo original | Uno o más campos que conforman la llave de negocio de la entidad. Se respeta el tipo de dato original. |
| `Hash_{NombreHub}` | STRING | Hash calculado con **SHA2-256** sobre la(s) llave(s) de negocio. Reglas: (a) Llave única no-STRING: se convierte a STRING y aplica SHA2-256. (b) Llave compuesta: se concatenan con separador pipe `\|` y se aplica SHA2-256. |
| `FechaRegistro` | TIMESTAMP | Momento exacto de inserción de la tupla (`Load_Date` en Data Vault 2.0). |
| `FuenteDatos` | STRING | Nombre de tres partes (`catalogo.esquema.tabla`) de la tabla de Bronce origen. |

**Procesamiento — Append Only**:

> **Hub_Cliente y Hub_Operacion (OPT-001)**: Usan `dp.create_auto_cdc_flow(stored_as_scd_type=1)`. El motor gestiona un MERGE cross-batch: si la llave de negocio no existe, inserta; si ya existe, actualiza todos los campos incluyendo `FechaRegistro` (semántica "última vez vista"). Coste O(delta del microbatch), sin full scan del Hub histórico. **`except_column_list` no se usa** — en `dp.create_auto_cdc_flow` este parámetro excluye la columna del esquema del target (no la protege de updates), lo que causa `DELTA_COLUMN_NOT_FOUND_IN_SCHEMA` al referenciarla en `cluster_by`.

Para **Hub_Transaccion**: `procesar_hub()` hace LEFT ANTI JOIN entre los datos nuevos y la tabla Hub existente (leída con `spark.read.table()`). Si la tabla no existe (primera ejecución), se captura `AnalysisException` y se retornan todos los datos como nuevos. Los registros ya almacenados son **inmutables y persistentes**.

**Criterio de creación**: Se crea un Hub por cada entidad de negocio que disponga de una llave de negocio identificable. Ejemplos: Clientes (CUSTID), Operaciones/Saldos (CUSTID + BLSQ), Transacciones (TRXID, o llave compuesta CUSTID + TRXID + TRXSQ).

**Liquid Clustering**: `FechaRegistro`, `Hash_{NombreHub}` (en ese orden).

#### Tablas Link (Relaciones entre Entidades)

Las tablas Link materializan las **relaciones entre dos tablas Hub**, creando un puente entre las entidades de negocio que están relacionadas en el origen de datos.

**Estructura de columnas estándar**:

| Columna | Tipo de Dato | Descripción |
|---------|-------------|-------------|
| `Hash_{NombreLink}` | STRING | Hash calculado con SHA2-256 sobre la concatenación `'Hash_{NombreHub1}\|Hash_{NombreHub2}'`. Representa la identidad única de la relación. |
| `Hash_{NombreHub1}` | STRING | Hash de la primera tabla Hub involucrada. Se toma directamente del Hub correspondiente. |
| `Hash_{NombreHub2}` | STRING | Hash de la segunda tabla Hub involucrada. Se toma directamente del Hub correspondiente. |
| `FechaRegistro` | TIMESTAMP | Momento exacto de inserción (Load_Date en Data Vault 2.0). |
| `FuenteDatos` | STRING | Nombre completo de tres partes de la tabla de Bronce origen. |

**Procesamiento — Append Only**:

> **Link_Cliente_Operacion (OPT-001)**: Usa `dp.create_auto_cdc_flow(stored_as_scd_type=1)`. El motor garantiza unicidad de la combinación `(Hash_Cliente, Hash_Operacion)` mediante MERGE cross-batch. `FechaRegistro` se actualiza en cada MERGE (semántica "última vez vista"). **`except_column_list` no se usa** — excluiría la columna del esquema target.

Para **Link_Cliente_Transaccion**: `procesar_link()` hace LEFT ANTI JOIN entre los datos nuevos y la tabla Link existente (leída con `spark.read.table()`). Si la tabla no existe (primera ejecución), se captura `AnalysisException` y se retornan todos los datos como nuevos. Los datos ya almacenados son persistentes e inmutables.

**Criterio de creación**: Se crea un Link cuando en el origen de datos existe una relación (por ejemplo, vía Foreign Key) entre dos entidades. **Alcance del laboratorio**: Cada Link relaciona exactamente **dos Hubs** (no se modelan Links de tres o más Hubs).

**Liquid Clustering**: `FechaRegistro`, `Hash_{NombreHub1}`, `Hash_{NombreHub2}` (en ese orden).

#### Tablas Satellite (Atributos de Entidades y Relaciones)

Las tablas Satellite almacenan todas las **variables cualitativas y cuantitativas** de las entidades (Hubs) y de sus relaciones (Links). Un Satellite se vincula a un Hub (usando `Hash_{NombreHub}`) o a un Link (usando `Hash_{NombreLink}`).

La relación entre un Hub y sus Satellites es **1:N** (un Hub puede tener múltiples Satellites). La separación de los Satellites sigue el principio de **tasa de cambio**:

- **Satellite de atributos estables**: Variables que no cambian o cuya tasa de cambio es muy baja (ej: fecha de nacimiento, sexo, nacionalidad, estado civil).
- **Satellites temáticos de atributos variables**: Variables que cambian con frecuencia, agrupadas por concepto funcional:
  - `Sat_{NombreHub}_Montos`: Todos los montos, saldos, límites y valores monetarios.
  - `Sat_{NombreHub}_FechasVariantes`: Fechas que se modifican con frecuencia (ej: fecha de última visita, última actualización, último movimiento).
  - Otros agrupamientos por concepto según el dominio de negocio.

**Estructura de columnas estándar**:

| Columna | Tipo de Dato | Descripción |
|---------|-------------|-------------|
| `Hash_{NombreHubOLink}` | STRING | Hash del Hub o Link al cual está vinculado este Satellite. Se obtiene a partir de las llaves de negocio correspondientes. |
| `{Campos_Cualitativos_Cuantitativos}` | Tipos originales o transformados | Variables del origen (Bronce) relacionadas a la entidad o relación. Los tipos de datos se respetan o se transforman cuando es necesario por optimización, formato o enriquecimiento. |
| `{Campos_Calculados_RowByRow}` | Según regla de negocio | Campos calculados a nivel de detalle (fila por fila) derivados de reglas de negocio (ej: clasificaciones con lógica CASE, rangos, indicadores). |
| `Hash_Diferenciador` | STRING | Hash con **SHA2-512** calculado sobre la concatenación (separada por pipe `\|`) de: `Hash_{NombreHubOLink}`, todos los campos cualitativos/cuantitativos, y todos los campos calculados. Su propósito es detectar cambios en cualquiera de los valores del registro. |
| `FechaRegistro` | TIMESTAMP | Momento exacto de inserción (Load_Date en Data Vault 2.0). |
| `FuenteDatos` | STRING | Nombre completo de tres partes de la tabla de Bronce origen. |

**Procesamiento — Append Only con Detección de Cambios (Change Data Capture lógico)**:  

El procesamiento de cada Satellite sigue estrategias distintas según el tipo de entidad:

**Para Satellites de Estado (Cliente, Operación)**:
1. Obtener el último `Hash_Diferenciador` por entidad usando `ROW_NUMBER() OVER (PARTITION BY hash_col ORDER BY FechaRegistro DESC) = 1`.
2. LEFT JOIN con los datos nuevos; insertar solo donde el hash cambió o no existe.

**Para Satellites Transaccionales (Transacción)**:
1. LEFT ANTI JOIN por `[hash_col, fecha_transaccion]` contra la tabla existente.
2. Cualquier fila cuya combinación (`Hash_Transaccion`, `fecha_transaccion`) no esté en el Satellite se inserta. No se usa ROW_NUMBER ni se compara `Hash_Diferenciador` en el join.
3. `Hash_Diferenciador` se calcula sobre todos los campos de negocio del registro (se preserva en la tabla pero no interviene en la deduplicación por ser transaccional).

En ambos casos: los registros previamente almacenados son **persistentes e inmutables**.

**Criterio de creación**: Toda tabla Hub debe tener al menos un Satellite. Las tablas Link pueden tener Satellites en casos específicos donde la relación misma posea atributos propios.

**Liquid Clustering**: `FechaRegistro`, `Hash_{NombreHubOLink}` (en ese orden).

### Medalla de Oro — Modelo Estrella (Data Warehouse Dimensional)

La medalla de Oro implementa un **Modelo Estrella** (Star Schema) orientado al análisis del comportamiento transaccional de los clientes en cajeros automáticos (ATMs). Este modelo se construye a partir de los datos actuales del Data Vault 2.0 de la medalla de Plata.

#### Dimensiones Requeridas

| Dimensión | Tipo | Fuente (Plata) | Descripción |
|-----------|------|-----------------|-------------|
| **Dim_Cliente** | Tipo 1 (sobrescritura) | Hub_Cliente + Satellites de Cliente | Contiene los atributos actuales de cada cliente: datos demográficos, segmentación, contacto, clasificación. |
| **Dim_Operacion** | Tipo 1 (sobrescritura) | Hub_Operacion + Satellites de Operación | Contiene los atributos actuales de cada operación/cuenta: tipo de cuenta, moneda, estado, producto bancario, saldos. |
| **Dim_Tiempo** | Tipo 1 — Vista Materializada incremental | `Sat_Transaccion_Montos.fecha_transaccion` (valores distintos) | Dimensión de fecha con granularidad diaria. Se construye exclusivamente a partir de las fechas de transacción presentes en el Satellite de montos, sin lógica de fechas explícita. Contiene atributos como año, mes, día, trimestre, semestre, nombre del día, indicador de fin de semana, etc. |

**Comportamiento incremental de Dim_Tiempo**: La dimensión se implementa como **Vista Materializada** con refresh incremental nativo de LSDP. Cada vez que aparecen nuevas fechas de transacción en `Sat_Transaccion_Montos`, el motor las incorpora automáticamente en el siguiente refresh del pipeline. No requiere lógica imperativa de fechas ni validaciones manuales de "ayer/hoy".

#### Tabla de Hechos

| Tabla | Fuente (Plata) | Descripción |
|-------|-----------------|-------------|
| **Hec_Transacciones_ATM** | `Trx_ATM_Stream` (ST `temporary=True`) + `Map_Cliente_Operacion_Dominante` (MV `temporary=True`) | Vista Materializada (`@dp.materialized_view`) que registra la transaccionalidad de **Retiros (DATM)** y **Depósitos (CATM)** en ATMs. Las FKs `DimIdCliente`/`DimIdOperacion` se pre-resuelven en `Trx_ATM_Stream`, dejando el plan del hecho libre de joins para refresh incremental por CDF. |

**Métricas clave esperadas**: Cantidad de depósitos (créditos), cantidad de retiros (débitos), monto total de depósitos, monto total de retiros, monto promedio de depósitos, monto promedio de retiros — todo desglosable por cliente, por operación/cuenta y por fecha.

---

---

# Objetivo del Caso de Uso

## Contexto de Negocio

El área de negocio de Clientes de la entidad bancaria necesita un **producto de datos analítico** que les permita comprender y monitorear el comportamiento de los clientes con respecto a dos dimensiones fundamentales:

1. **Saldos de las cuentas**: Evolución, estado actual y clasificación de los saldos de cada cliente.
2. **Uso de cajeros automáticos (ATMs)**: Patrones de retiros (débitos) y depósitos (créditos) realizados a través de ATMs.

## Preguntas de Negocio que el Producto de Datos Debe Responder

- ¿Cuántos depósitos (créditos) y retiros (débitos) realiza cada cliente en ATMs?
- ¿Cuál es el monto promedio de los retiros y depósitos por cliente?
- ¿Cuál es el monto total de retiros y depósitos por cliente en un período determinado?
- ¿Cómo se distribuyen las transacciones de ATM por segmento de cliente, tipo de cuenta o región?
- ¿Qué clientes presentan comportamientos atípicos en sus transacciones de ATM?

## Entregable

Un **Modelo Estrella** (Data Warehouse dimensional) expuesto a través de Delta Tables en el catálogo de Unity Catalog, listo para ser consumido por herramientas de visualización (Databricks SQL, dashboards) o consultas analíticas ad-hoc.

## Reglas de Negocio

### Reglas de Filtrado de Transacciones ATM

- Las transacciones relevantes para la tabla de Hechos son aquellas cuyo campo `tipo_transaccion` (`TRXTYP`) corresponde a operaciones en cajeros automáticos:
  - **`DATM`** (Débito en ATM): Representa un retiro de efectivo en cajero automático.
  - **`CATM`** (Crédito en ATM): Representa un depósito de efectivo en cajero automático.
- Todas las demás transacciones (PGSL, transferencias electrónicas, pagos de servicios, etc.) quedan **fuera del alcance** de la tabla de Hechos pero **sí se almacenan** en el Data Vault 2.0 de Plata (donde se preserva la historia completa).

### Reglas de Clasificación Aplicables a Campos Calculados

- Los campos calculados deben implementarse con la función `F.when().when()...otherwise()` de PySpark (equivalente a la cláusula `CASE` de SQL).
- Los campos calculados se ubican en los Satellites donde funcional y lógicamente correspondan según la naturaleza de los datos que clasifican.

---

# Stack Técnico

## Plataforma y Entorno de Ejecución

| Componente | Detalle |
|------------|---------|
| **Plataforma** | Databricks Free Edition |
| **Motor de Cómputo** | Serverless Compute (sin clusters gestionados por el usuario) |
| **Catálogo de Datos** | Unity Catalog (gestión centralizada de metadatos, permisos y linaje) |
| **Lenguaje de Programación** | PySpark (Python) |
| **Framework de Pipelines y Desarrollo** | Lakeflow Spark Declarative Pipelines (LSDP) |
| **Entorno de Exploracion y Pruebas** | Notebooks de Databricks |
| **Orquestación** | Lakeflow Jobs |
| **Formato de Almacenamiento** | Delta Lake (tablas Delta) |

## Repositorio

El repositorio Git del proyecto sigue una estructura organizada en tres áreas principales: **documentación**, **código fuente** y **configuración de AI-DLC**.

```
DbsFreeLakeflowSparkDeclarativePipelineDataVaultDwh/
│
├── README.md                          ← Descripción general del proyecto y guía de inicio rápido
├── SYSTEM.md                          ← Spec-Driven Development completa — especificación inicial de la solución (este archivo)
├── AGENTS.md                          ← Configuración de agentes AI-DLC y comandos Kiro
│
├── .github/
│   └── prompts/                       ← Prompt files para comandos Kiro del flujo SDD
│       ├── kiro-spec-init.prompt.md
│       ├── kiro-spec-requirements.prompt.md
│       ├── kiro-spec-design.prompt.md
│       ├── kiro-spec-tasks.prompt.md
│       ├── kiro-spec-impl.prompt.md
│       ├── kiro-spec-status.prompt.md
│       ├── kiro-steering.prompt.md
│       ├── kiro-steering-custom.prompt.md
│       ├── kiro-validate-design.prompt.md
│       ├── kiro-validate-gap.prompt.md
│       └── kiro-validate-impl.prompt.md
│
├── .kiro/
│   └── settings/
│       ├── rules/                     ← Reglas de diseño, descubrimiento y generación de tareas
│       └── templates/
│           ├── specs/                 ← Plantillas para especificaciones de features
│           ├── steering/              ← Archivos steering del proyecto (product.md, tech.md, structure.md)
│           └── steering-custom/       ← Archivos steering personalizados
│
├── docs/
│   ├── ManualTecnico.md               ← Documentación técnica detallada de la solución implementada
│   ├── ModeloDatosFinal.md            ← Modelo de datos final con diagramas y esquemas
│   └── Demo.md                        ← Guía de demostración del pipeline funcionando
│
└── src/
    └── LSDP_Lab_DataVault_DWH/        ← Proyecto principal — Pipeline LSDP en Databricks
        ├── explorations/              ← Notebooks auxiliares (NO son parte del pipeline)
        │   ├── {Consultas SQL de validación y pruebas funcionales sobre tablas}   ← Validan diseño de tablas y comportamiento de datos
        │   └── {Notebooks generadores de archivos Parquet para la Landing Zone}   ← Generan los archivos parquets en el Volume y Ruta Absoluta recibida por parametros
        │
        ├── transformations/           ← Notebooks del pipeline LSDP (código de producción)
        │   ├── LSDPBronce{Origen}      ← ST persistente AutoLoader directo por cada fuente Parquet (CMSTFL, TRXPFL, BLNCFL)
        │   ├── LSDPPlata{Entidad}     ← Hubs, Links y Satellites del modelo Data Vault 2.0
        │   └── LSDPOro{Entidad}       ← Dimensiones y tabla de hechos del modelo Estrella
        │
        └── utilities/                 ← Módulos Python puro reutilizables (no son notebooks)
            └── LSDP{NombreUtilidad}.py   ← Funciones de Utilidades
            ├── LSDPConfiguracion.py      ← Parámetros, constantes, funciones helper compartidas
```

> **Convención de nombres de notebooks**: Los notebooks de `transformations/` siguen el patrón `LSDP{Medalla}{Nombre}` (ej: `LSDPBronceCMSTFL`, `LSDPPlataHubCliente`, `LSDPOroDimensiones`). Los archivos de `utilities/` siguen el patrón `LSDP{NombreUtilidad}.py` (ej: `LSDPUtilidadPrincipal.py`). Ver la sección [Estructura de Notebooks Propuesta](#estructura-de-notebooks-propuesta) para el desglose completo.

## Referencia Técnica LSDP

### ¿Qué es LSDP?

**Lakeflow Spark Declarative Pipelines (LSDP)** es el framework declarativo de Databricks para definir pipelines de datos. Anteriormente conocido como **Delta Live Tables (DLT)**, fue renombrado a LSDP. El módulo Python nativo reside en `pyspark.pipelines`.

### Import Correcto

```python
# CORRECTO — Módulo nativo del runtime de Databricks
from pyspark import pipelines as dp

# INCORRECTO — SDK REST, no tiene decoradores
# import databricks.sdk.pipelines as dp  ← NO USAR
```

El paquete `databricks-sdk` (PyPI) es el SDK cliente REST, **no** contiene decoradores `@dp.table` ni `@dp.materialized_view`.

### 1.3 API de Decoradores

#### `@dp.table()` — Streaming Table

Crea una Streaming Table que procesa datos de forma incremental. Se usa con `spark.readStream`.

```python
@dp.table(
    name=f"{catalogo}.{esquema}.ST_CMSTFL_Historica",
    table_properties={"quality": "bronze"},
    cluster_by=["FechaRegistroParquet"],
    temporary=True
)
def st_cmstfl_historica():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(ruta_cmstfl)
    )
```

**Parámetros soportados de `@dp.table`**:
- `name`: Nombre completo de 3 partes (`catalogo.esquema.tabla`) o nombre simple si `temporary=True`.
- `table_properties`: Diccionario de propiedades Delta.
- `cluster_by`: Lista de columnas para Liquid Clustering.
- `temporary`: Si es `True`, la tabla es temporal y no se registra en el Unity Catalog.

#### `@dp.materialized_view()` — Vista Materializada

Crea una vista persistida que se recalcula completamente cuando cambian los datos fuente. Se usa con `spark.read` (batch, no streaming).

```python
@dp.materialized_view(
    name=f"{catalogo}.{esquema}.CMSTFL",
    cluster_by=["FechaRegistroParquet"]
)
def cmstfl():
    return (
        spark.read.table("CMSTFL_temp")
        .filter(F.col("FechaRegistroParquet") == fecha_max)
    )
```

**Parámetros soportados**:
- `name`: Nombre completo de 3 partes. **NO** usar `catalog=` ni `schema=` por separado.
- `table_properties`: Diccionario de propiedades Delta.
- `cluster_by`: Lista de columnas para Liquid Clustering.

**Error conocido**: `@dp.materialized_view(name="vista", catalog="cat", schema="sch")` genera `materialized_view() got an unexpected keyword argument 'catalog'`.

#### `@dp.temporary_view()` — Vista Temporal

Vista que existe solo durante la ejecución del pipeline, no se persiste en el catálogo.

```python
@dp.temporary_view()
def filtro_atm():
    return (
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Transaccion_DatosEstables")
        .filter(F.col("tipo_transaccion").isin(TIPO_DATM, TIPO_CATM))
    )
```

#### `dp.create_streaming_table()` — Crear Streaming Table Programáticamente

Crea una Streaming Table sin decorador, útil cuando se necesita separar la definición de la tabla del flujo de datos.

```python
# Ejemplo: Dim_Tiempo como Vista Materializada incremental
# (patrón aprobado para la Medalla de Oro)
@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.Dim_Tiempo",
    cluster_by=["FechaClave"]
)
def dim_tiempo():
    return (
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Transaccion_Montos")
        .select(F.col("fecha_transaccion").alias("FechaClave"))
        .distinct()
        .withColumn("Anio", F.year("FechaClave"))
        .withColumn("Mes", F.month("FechaClave"))
        # ... demás atributos calendario con funciones determinísticas ...
    )
```

**Nota**: Las expectations de las Materialized Views se declaran como decoradores sobre la función, igual que en Hubs y Links de Plata.

#### `@dp.append_flow()` — Flujo de Append

Define un flujo de datos que inserta registros en una Streaming Table existente. Se usa para cargar datos de múltiples fuentes en una misma tabla o para lógica de inserción incremental (acumulativa).

```python
# Ejemplo genérico de append_flow (NO aplica a Dim_Tiempo en Oro)
@dp.append_flow(target=f"{catalogo}.{esquema}.MiStreamingTable")
def cargar_datos():
    return spark.read.table(f"{catalogo_plata}.{esquema_plata}.FuentePlata")
```

**Nota**: Las expectations se definen en `dp.create_streaming_table()`, no en `@dp.append_flow`.

### 1.4 AutoLoader (cloudFiles)

AutoLoader es el mecanismo de ingesta incremental que detecta y procesa archivos nuevos automáticamente.

```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.schemaLocation", f"{ruta_base_autoloader}/CMSTFL") \
    .load(ruta_cmstfl)
```

**Características clave**:
- Detecta archivos nuevos sin reprocesar los anteriores (checkpoint interno).
- Infiere columnas de partición automáticamente desde la estructura `clave=valor` del directorio.
- Las columnas de partición `año`, `mes`, `dia` se infieren como strings por defecto; requieren transformación a `DateType`.
- En la ruta de la Landing Zone se pasa la ruta base (sin `año=/mes=/dia=`) para que Spark aplique lazy evaluation sobre todas las sub-carpetas.
- `cloudFiles.schemaEvolutionMode = "addNewColumns"` permite evolución de esquema; requiere `cloudFiles.schemaLocation` para almacenar el checkpoint del schema.

### 1.5 Expectations (Calidad de Datos)

Las expectations son restricciones SQL booleanas que validan cada registro en el pipeline.

#### Tipos de Expectations

| Decorador Python | Acción | Comportamiento |
|------------------|--------|----------------|
| `@dp.expect("nombre", "condición SQL")` | **warn** | Registros inválidos se escriben en la tabla destino. Se registran métricas. |
| `@dp.expect_or_drop("nombre", "condición SQL")` | **drop** | Registros inválidos se descartan antes de escribir. Se registran métricas. |
| `@dp.expect_or_fail("nombre", "condición SQL")` | **fail** | La actualización falla si se detecta un registro inválido. Rollback atómico. |

#### Expectations Múltiples (Agrupadas)

```python
validaciones_hub_cliente = {
    "id_cliente_no_nulo": "IdentificadorCliente IS NOT NULL",
    "id_cliente_positivo": "IdentificadorCliente > 0",
    "hash_cliente_no_nulo": "Hash_Cliente IS NOT NULL"
}

@dp.materialized_view(...)
@dp.expect_all_or_fail(validaciones_hub_cliente)
def hub_cliente():
    ...

# Alternativas:
# @dp.expect_all(validaciones)           → warn para todas
# @dp.expect_all_or_drop(validaciones)   → drop si falla cualquiera
# @dp.expect_all_or_fail(validaciones)   → fail si falla cualquiera
```

**Restricciones de las Expectations**:
- La condición debe ser SQL estándar válido.
- No se permiten funciones Python personalizadas dentro de la condición.
- No se permiten subconsultas que referencien otras tablas.
- Las expectations se definen en el decorador de la tabla, no en `@dp.append_flow`.

### 1.6 Parámetros del Pipeline

Los parámetros se configuran en la definición del pipeline (JSON/UI) y se acceden en el código con:

```python
# Acceder a parámetros del pipeline
catalogo = spark.conf.get("pipeline.catalogo")
esquema = spark.conf.get("pipeline.esquema")
catalogo_plata = spark.conf.get("pipeline.catalogo_plata")
esquema_plata = spark.conf.get("pipeline.esquema_plata")
catalogo_oro = spark.conf.get("pipeline.catalogo_oro")
esquema_oro = spark.conf.get("pipeline.esquema_oro")
```

**Nota**: En Databricks Free Edition, `spark.conf.get()` funciona para parámetros definidos en la configuración del pipeline bajo la sección `configuration` del JSON de definición.

### 1.7 Flujos (Flows) — Conceptos Clave

- **Default Flow**: Se crea automáticamente al definir una tabla con `@dp.table()`.
- **Append Flow**: Se crea explícitamente con `@dp.append_flow()` para insertar registros en una tabla existente.
- **Auto CDC Flow**: Para ingesta de datos con Change Data Capture. Se crea con `dp.create_auto_cdc_flow()`.
- Los flujos son identificados por nombre. Renombrar un flujo implica perder el checkpoint.
- Si un flujo falla, los demás flujos del pipeline continúan ejecutándose.

### 1.8 Patrón de Ingesta Bronce (Aprobado)

El patrón aprobado para la Medalla de Bronce es una **única Streaming Table persistente** con AutoLoader directo. La arquitectura de 2 capas (ST temporal + MV snapshot) fue eliminada.

#### Streaming Table Persistente con AutoLoader Directo

Se ingestan los parquets incremental y automáticamente usando AutoLoader. Los datos se escriben en una **Streaming Table Persistente** registrada en Unity Catalog. En este paso se construye la columna `FechaRegistroParquet`.

```python
@dp.table(
    name=f"{catalogo}.{esquema}.CMSTFL",
    cluster_by=["FechaRegistroParquet"],
)
def cmstfl():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", config["schema_location_cmstfl"])
        .load(config["ruta_cmstfl"])
        .withColumn(
            "FechaRegistroParquet",
            F.to_date(F.concat_ws("-", F.col("año"), F.col("mes"), F.col("dia"))),
        )
    )
    return reordenar_columnas_lc(df, ["FechaRegistroParquet"])
```

**Este patrón se repite para las 3 tablas fuente**:
- `CMSTFL`
- `TRXPFL`
- `BLNCFL`

**Las entidades de Plata leen Bronce con**:
```python
dp.read_stream(f"{catalogo}.{esquema}.CMSTFL")  # dentro de @dp.append_flow()
```
)
def cmstfl_temp_OLD():
    # PATRÓN ELIMINADO — ver sección anterior
    ...
```

#### Paso 2 — Vista Materializada con Snapshot Más Reciente

> **ELIMINADO**: El patrón de MV + snapshot fue reemplazado por ST persistente. Esta sección se mantiene sólo como referencia histórica para proyectos legados.

```python
# PATRÓN LEGADO — NO USAR
@dp.materialized_view(
    name=f"{catalogo}.{esquema}.CMSTFL",
    cluster_by=["FechaRegistroParquet"]
)
def cmstfl():
    st = spark.read.table("CMSTFL_temp")
    max_fecha = st.select(F.max("FechaRegistroParquet").alias("max_fecha"))
    return (
        st.join(F.broadcast(max_fecha),
                st.FechaRegistroParquet == max_fecha.max_fecha)
        .drop("max_fecha")
    )
```

**Este patrón se repite para las 3 tablas fuente**:
- `CMSTFL_temp` → `CMSTFL`
- `TRXPFL_temp` → `TRXPFL`
- `BLNCFL_temp` → `BLNCFL`

---

---

## Restricciones y Compatibilidad Serverless

> **CRÍTICO**: Todo el código PySpark generado **DEBE** ser 100% compatible con Databricks Free Edition Serverless Compute. Los siguientes errores ya han ocurrido en producción y **NO deben repetirse**.

### Prohibiciones Absolutas (generan error en runtime)

| Prohibición | Error Esperado | Alternativa Segura |
|-------------|----------------|---------------------|
| `.cache()` / `.persist()` | `NOT_SUPPORTED_WITH_SERVERLESS` | No usar; confiar en el optimizador de Spark |
| `spark.sparkContext` / `sc.` | No existe en Serverless | Usar APIs de DataFrame nativas |
| Operaciones RDD: `.rdd`, `.parallelize()`, `.mapPartitions()`, `.foreachPartition()`, `.toLocalIterator()` | No soportado | Usar funciones nativas de DataFrame y `spark.range()` |
| Broadcast variables: `sc.broadcast()` | No existe | Usar broadcast hints: `F.broadcast(df)` |
| Accumulators: `sc.accumulator()`, `sc.longAccumulator()` | No existe | Usar agregaciones de DataFrame |
| `dbutils.library.install()` / `%pip install` | No soportado | Usar cluster libraries |
| Threading / Multiprocessing | No soportado | Procesamiento secuencial dentro del pipeline |
| `spark.sql.ansi.enabled = false` | Viene `true` por defecto; no cambiarlo | Trabajar con ANSI mode habilitado |
| `spark.conf.set()` para configuraciones no permitidas | Error de configuración | Solo se permite: `spark.sql.shuffle.partitions` y `spark.sql.adaptive.*` |
| UDFs (User Defined Functions) | No optimizadas en Serverless/Photon | Usar siempre funciones nativas de PySpark (`pyspark.sql.functions.*`) |

### Reglas del ANSI Mode (spark.sql.ansi.enabled = true)

| Escenario | Problema | Solución Obligatoria |
|-----------|----------|----------------------|
| `F.hash()` retorna `IntegerType` (32 bits) | `Integer.MIN_VALUE` causa overflow en `F.abs()` | **SIEMPRE** usar `F.abs(F.hash(...).cast("long"))` — cast a `LongType` **antes** de `abs()` |
| Operador `+` en columnas | Es **suma aritmética**, no concatenación de strings | Usar `F.concat()` para concatenar strings; usar `F.hash(col1, col2)` con argumentos separados |
| Literales grandes (>2B) en expresiones aritméticas | Overflow por tipo `IntegerType` | Usar `.cast("long")` explícito |

### Regla del Decorador @dp.materialized_view

| Correcto | Incorrecto | Error |
|----------|-----------|-------|
| `@dp.materialized_view(name=f"{catalogo}.{esquema}.nombre_vista", ...)` | `@dp.materialized_view(name="vista", catalog="cat", schema="sch")` | `materialized_view() got an unexpected keyword argument 'catalog'` |

El nombre de tres partes (`catalogo.esquema.tabla`) debe pasarse completo en el parámetro `name`. No existen parámetros separados `catalog=` ni `schema=`.

### Compatibilidad con Databricks Free Edition

#### 5.4 Compatibilidad con Databricks Free Edition

| Característica | Disponible | Notas |
|---------------|-----------|-------|
| LSDP (Declarative Pipelines) | ✅ Sí | Motor principal del proyecto |
| AutoLoader (cloudFiles) | ✅ Sí | Para ingesta incremental en Bronce |
| Liquid Clustering | ✅ Sí | Optimización de queries |
| Unity Catalog | ✅ Sí | Gestión de catálogos/esquemas |
| Volumes | ✅ Sí | Landing Zone para parquets |
| Materialized Views | ⚠️ Solo Oro | En Plata y Bronce se usa ST+AppendFlow. Las MVs de Bronce fueron eliminadas. |
| Streaming Tables | ✅ Sí | Para Bronce (AutoLoader directo) y todas las entidades de Plata (ST Acumulativas con AppendFlow). **Dim_Tiempo en Oro NO usa Streaming Table** — se implementa como Vista Materializada incremental. |
| Lakeflow Jobs | ✅ Sí | Orquestación del pipeline |
| Delta Lake | ✅ Sí | Formato de almacenamiento |
| `F.sha2()` | ✅ Sí | Para hashes SHA2-256/512 |
| `F.broadcast()` | ✅ Sí | Join hints (reemplaza `sc.broadcast()`) |
| Max 5 concurrent job tasks | ⚠️ Limitación | Diseñar pipeline según propone LSDP |
| 1 active pipeline por tipo | ⚠️ Limitación | No ejecutar múltiples pipelines LSDP simultáneamente |
| R / Scala | ❌ No | Solo Python/PySpark |

---

---


### Patrones de Código Seguros y Verificados

## 5.3 Patrones de Código Seguros para este Proyecto

### Patrón de Hash SHA2-256 (Hubs y Links)

```python
# Llave simple (CUSTID → IdentificadorCliente en Hub_Cliente)
F.sha2(F.col("CUSTID").cast("string"), 256).alias("Hash_Cliente")

# Llave compuesta (CUSTID + BLSQ → Hub_Operacion)
F.sha2(
    F.concat_ws("|", F.col("CUSTID").cast("string"), F.col("BLSQ").cast("string")),
    256
).alias("Hash_Operacion")

# TRXID ya es StringType → no requiere cast
F.sha2(F.col("TRXID"), 256).alias("Hash_Transaccion")

# Link (combinación de hashes)
F.sha2(F.concat_ws("|", F.col("Hash_Cliente"), F.col("Hash_Operacion")), 256)
    .alias("Hash_Link_Cliente_Operacion")
```

> **Nota**: Los hashes se calculan a partir de las columnas del Parquet fuente (nombres AS400: CUSTID, BLSQ, TRXID). El renombramiento a español (IdentificadorCliente, SecuenciaSaldo, IdentificadorTransaccion) ocurre en la misma transformación mediante `.alias()`.

### Patrón de Hash SHA2-512 (Hash_Diferenciador en Satellites)

```python
F.sha2(
    F.concat_ws("|",
        F.col("Hash_Cliente"),
        F.col("sexo_cliente"),
        F.col("tratamiento_cliente"),
        F.col("fecha_nacimiento").cast("string"),
        # ... todos los campos del Satellite ...
        F.col("RangoEtario")
    ),
    512
).alias("Hash_Diferenciador")
```

### Patrón de Detección de Cambios (Append Only en Satellites)

```python
from pyspark.sql.window import Window

def procesar_satellite(spark, nombre_sat, hash_col, datos_nuevos):
    """
    Patrón seguro para procesamiento Append Only de Satellites.
    Compara Hash_Diferenciador entre último registro existente y datos nuevos.
    """
    w = Window.partitionBy(hash_col).orderBy(F.col("FechaRegistro").desc())
    try:
        existente = (
            spark.read.table(f"{catalogo_plata}.{esquema_plata}.{nombre_sat}")
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .select(hash_col, F.col("Hash_Diferenciador").alias("Hash_Existente"))
        )
        return (
            datos_nuevos
            .join(existente, hash_col, "left")
            .filter(
                F.col("Hash_Existente").isNull() |
                (F.col("Hash_Diferenciador") != F.col("Hash_Existente"))
            )
            .drop("Hash_Existente")
        )
    except AnalysisException:
        return datos_nuevos  # Primera ejecución (tabla no existe)
```

### Patrón para Snapshot Más Reciente (Bronce Capa 2)

```python
@dp.materialized_view(
    name=f"{catalogo}.{esquema}.CMSTFL",
    cluster_by=["FechaRegistroParquet"]
)
def cmstfl():
    st = spark.read.table("CMSTFL_temp")
    max_fecha = st.select(F.max("FechaRegistroParquet").alias("max_fecha"))
    return (
        st.join(F.broadcast(max_fecha),
                st.FechaRegistroParquet == max_fecha.max_fecha)
        .drop("max_fecha")
    )
```


```python
# Import correcto de LSDP
from pyspark import pipelines as dp

# Generación de secuencias (alternativa a sc.parallelize())
df = spark.range(0, 1000)

# Hash seguro con protección de overflow ANSI
F.sha2(F.concat_ws("|", col1.cast("string"), col2.cast("string")), 256)

# Para hashes simples con F.hash() — SIEMPRE cast a long antes de abs
F.abs(F.hash(col1, col2).cast("long"))

# PATRÓN LEGADO — NO USAR para Bronce ni Plata
@dp.materialized_view(
    name=f"{catalogo}.{esquema}.nombre_vista",
    table_properties={"pipelines.autoOptimize.zOrderCols": "col1"},
    cluster_by=["col1", "col2"]
)
def mi_vista():
    return spark.read.table(f"{catalogo}.{esquema}.tabla_fuente")
```

## Parametrización del Pipeline

### Parámetros del Pipeline LSDP

## 6.1 Parámetros del Pipeline LSDP

Estos se configuran en el JSON de definición del pipeline y se acceden con `spark.conf.get()`:

```json
{
  "configuration": {
    "pipeline.catalogo": "mi_catalogo_bronce",
    "pipeline.esquema": "mi_esquema_bronce",
    "pipeline.volumen": "mi_volumen",
    "pipeline.ruta_base": "/Volumes/mi_catalogo/mi_esquema/mi_volumen/archivos/LSDP_DataVault_Dwh/As400",
    "pipeline.catalogo_plata": "mi_catalogo_plata",
    "pipeline.esquema_plata": "mi_esquema_plata",
    "pipeline.catalogo_oro": "mi_catalogo_oro",
    "pipeline.esquema_oro": "mi_esquema_oro",
    "pipeline.ruta_base_autoloader": "/Volumes/mi_catalogo/mi_esquema/mi_volumen/checkpoints/autoloader"
  }
}
```

```python
# En el notebook de configuración — Parámetros del Pipeline

# Bronce / Landing Zone
catalogo = spark.conf.get("pipeline.catalogo")
esquema = spark.conf.get("pipeline.esquema")
volumen = spark.conf.get("pipeline.volumen")
ruta_base = spark.conf.get("pipeline.ruta_base")

# Plata (Data Vault 2.0)
catalogo_plata = spark.conf.get("pipeline.catalogo_plata")
esquema_plata = spark.conf.get("pipeline.esquema_plata")

# Oro (Modelo Estrella)
catalogo_oro = spark.conf.get("pipeline.catalogo_oro")
esquema_oro = spark.conf.get("pipeline.esquema_oro")

# AutoLoader — Checkpoint para Schema Evolution
ruta_base_autoloader = spark.conf.get("pipeline.ruta_base_autoloader")
```

**Descripción de cada parámetro**:

| Parámetro | Propósito |
|-----------|-----------|
| `pipeline.catalogo` | Catálogo Unity Catalog para tablas de Bronce (Snapshots) |
| `pipeline.esquema` | Esquema dentro del catálogo de Bronce |
| `pipeline.volumen` | Nombre del volumen de Unity Catalog (Landing Zone) |
| `pipeline.ruta_base` | Ruta absoluta a la carpeta raíz de los parquets fuente AS400 |
| `pipeline.catalogo_plata` | Catálogo Unity Catalog para tablas de Plata (Data Vault) |
| `pipeline.esquema_plata` | Esquema dentro del catálogo de Plata |
| `pipeline.catalogo_oro` | Catálogo Unity Catalog para tablas de Oro (Modelo Estrella) |
| `pipeline.esquema_oro` | Esquema dentro del catálogo de Oro |
| `pipeline.ruta_base_autoloader` | Ruta para almacenar el checkpoint de Schema Evolution de AutoLoader (`cloudFiles.schemaLocation`) |


### Constantes de Negocio (Notebook de Configuración)

## 6.2 Constantes de Negocio (Notebook de Configuración)

Definidas como constantes Python en un notebook centralizado `00_Configuracion`:

```python
# =========================================================
# Constantes de Negocio — No hardcodeadas en transformaciones
# =========================================================

# Tipos de transacciones ATM
TIPO_DATM = "DATM"  # Débito ATM (retiro)
TIPO_CATM = "CATM"  # Crédito ATM (depósito)
TIPOS_ATM = [TIPO_DATM, TIPO_CATM]

# Algoritmos de hash
HASH_HUB_LINK_BITS = 256   # SHA2-256 para Hubs y Links
HASH_SATELLITE_BITS = 512  # SHA2-512 para Hash_Diferenciador
HASH_SEPARATOR = "|"        # Separador para concatenación de hashes

# Umbrales de campos calculados — Cliente
UMBRAL_RANGO_ETARIO = {
    "JOVEN_ADULTO": (18, 25),
    "ADULTO": (26, 35),
    "ADULTO_MEDIO": (36, 45),
    "ADULTO_MAYOR": (46, 55),
    "SENIOR": (56, 999)
}

UMBRAL_CATEGORIA_INGRESOS = {
    "BAJO": (0, 15000),
    "MEDIO": (15001, 35000),
    "ALTO": (35001, 65000),
    "MUY_ALTO": (65001, 85000),
    "PREMIUM": (85001, 999999999)
}

# Umbrales de campos calculados — Operación
UMBRAL_CATEGORIA_SALDO = {
    "BAJO": (0, 10000),
    "MEDIO": (10001, 30000),
    "ALTO": (30001, 60000),
    "MUY_ALTO": (60001, 90000),
    "PREMIUM": (90001, 999999999)
}

UMBRAL_UTILIZACION_CREDITO = {
    "SIN_USO": (0, 0),
    "USO_BAJO": (0.001, 0.05),
    "USO_MODERADO": (0.051, 0.10),
    "USO_ALTO": (0.101, 0.15),
    "SOBRE_UTILIZADO": (0.151, 1.0)
}

UMBRAL_SOBREGIRO = {
    "SIN_SOBREGIRO": (0, 100),
    "SOBREGIRO_LEVE": (101, 1000),
    "SOBREGIRO_MODERADO": (1001, 3000),
    "SOBREGIRO_CRITICO": (3001, 999999999)
}

# Umbrales de campos calculados — Transacción
UMBRAL_RANGO_MONTO = {
    "MICRO": (0, 1000),
    "PEQUENA": (1001, 10000),
    "MEDIANA": (10001, 50000),
    "GRANDE": (50001, 90000),
    "MUY_GRANDE": (90001, 999999999)
}

UMBRAL_RIESGO_FRAUDE = {
    "SIN_RIESGO": (0, 20),
    "RIESGO_BAJO": (21, 40),
    "RIESGO_MODERADO": (41, 60),
    "RIESGO_ALTO": (61, 80),
    "RIESGO_CRITICO": (81, 100)
}

# Rutas de tablas fuente (derivadas de parámetros del pipeline)
ruta_cmstfl = f"{ruta_base}/CMSTFL/"
ruta_trxpfl = f"{ruta_base}/TRXPFL/"
ruta_blncfl = f"{ruta_base}/BLNCFL/"
```


### Funciones Helper Centralizadas

## 6.3 Funciones Helper Centralizadas

```python
def calcular_hash_hub(columnas, bits=HASH_HUB_LINK_BITS, separador=HASH_SEPARATOR):
    """Genera hash SHA2 combinando una o más columnas."""
    if len(columnas) == 1:
        col_str = columnas[0].cast("string") if str(columnas[0].dataType) != "StringType" else columnas[0]
        return F.sha2(col_str, bits)
    else:
        cols_str = [c.cast("string") for c in columnas]
        return F.sha2(F.concat_ws(separador, *cols_str), bits)

def calcular_hash_diferenciador(hash_entidad, *campos):
    """Genera Hash_Diferenciador SHA2-512 para detección de cambios en Satellites."""
    cols_str = [hash_entidad] + [c.cast("string") for c in campos]
    return F.sha2(F.concat_ws(HASH_SEPARATOR, *cols_str), HASH_SATELLITE_BITS)

def reordenar_columnas_lc(df, columnas_lc):
    """Reordena las columnas del DataFrame colocando las columnas de Liquid Clustering
    en las primeras posiciones del esquema.

    Justificación técnica: Según la documentación oficial de Databricks
    (https://docs.databricks.com/aws/en/delta/clustering), las columnas de
    Liquid Clustering deben tener estadísticas recopiladas. Por defecto, solo
    las primeras 32 columnas de una tabla Delta tienen estadísticas recopiladas.
    Colocar las columnas de LC en las primeras posiciones garantiza que siempre
    tengan estadísticas disponibles para optimizar las operaciones de lectura.

    Args:
        df: DataFrame de PySpark.
        columnas_lc: Lista de nombres de columnas que forman el Liquid Clustering.

    Returns:
        DataFrame con las columnas de LC en las primeras posiciones, seguidas
        del resto de columnas en su orden original.
    """
    resto = [c for c in df.columns if c not in columnas_lc]
    return df.select(*columnas_lc, *resto)
```


---

# Modelo de Datos

## Origen — Landing Zone (Parquets AS400)

A continuación se definen los esquemas de los tres archivos Parquet que constituyen los datos fuente del laboratorio. Estos Parquets simulan datos provenientes de un sistema AS400 bancario y residen en la carpeta `Temporal/` del proyecto para efectos de desarrollo y pruebas. En producción, los Parquets se depositan en la Landing Zone del Volume de Unity Catalog.

> **Nota sobre la nomenclatura AS400**: Cada campo incluye su nombre original AS400 (abreviatura de 6 caracteres heredada del sistema legacy) y su nombre descriptivo en español que se utilizará en el pipeline de procesamiento.

## CMSTFL — Maestro de Clientes (70 columnas)

**Tipo de datos**: 1 LongType (CUSTID) + 40 StringType + 7 LongType + 18 DateType + 2 DoubleType = 70 columnas  
**Registros**: 4,000,000 (cantidad modificable vía parámetro) clientes únicos  
**Llave primaria**: `CUSTID` (identificador_cliente)  
**Naturaleza de los datos**: Entidad maestra; cada fila representa un cliente único del banco con sus atributos demográficos, de contacto, de clasificación y de fechas de eventos.

| Campo AS400 | Nombre en Español | Tipo PySpark | Descripción |
|-------------|-------------------|-------------|-------------|
| `CUSTID` | identificador_cliente | LongType | Identificador único del cliente (clave primaria) |
| `CUSNM` | nombre_cliente | StringType | Nombre de pila del cliente |
| `CUSLN` | apellido_cliente | StringType | Apellido del cliente |
| `CUSMD` | nombre_medio_cliente | StringType | Nombre medio o segundo nombre |
| `CUSFN` | nombre_completo_cliente | StringType | Nombre completo concatenado |
| `CUSSX` | sexo_cliente | StringType | Sexo del cliente (M/F/O) |
| `CUSTT` | tratamiento_cliente | StringType | Tratamiento formal (Sr., Sra., Dr., etc.) |
| `CUSDB` | fecha_nacimiento | DateType | Fecha de nacimiento del cliente |
| `CUSYR` | anio_nacimiento | LongType | Año de nacimiento derivado de CUSDB |
| `CUSAG2` | edad_cliente | LongType | Edad calculada a la fecha actual |
| `CUSAD` | direccion_calle | StringType | Dirección de la calle principal |
| `CUSA2` | direccion_apartamento | StringType | Departamento o número de apartamento |
| `CUSCT` | ciudad_residencia | StringType | Ciudad de residencia |
| `CUSST` | estado_provincia | StringType | Estado o provincia de residencia |
| `CUSZP` | codigo_postal | StringType | Código postal de la residencia |
| `CUSCN` | pais_residencia | StringType | País de residencia (ISO 3166) |
| `CUSNA` | nacionalidad_cliente | StringType | Nacionalidad del cliente |
| `CUSPH` | telefono_principal | StringType | Teléfono principal de contacto |
| `CUSMB` | telefono_movil | StringType | Número de teléfono móvil |
| `CUSEM` | correo_electronico | StringType | Correo electrónico del cliente |
| `CUSMS` | estado_civil | StringType | Estado civil (S/C/D/V) |
| `CUSOC` | ocupacion_cliente | StringType | Ocupación o profesión |
| `CUSED` | nivel_educativo | StringType | Nivel de educación alcanzado |
| `CUSDL` | numero_licencia_conducir | StringType | Número de licencia de conducir |
| `CUSDP` | tipo_documento_pasaporte | StringType | Tipo de documento de identidad |
| `CUSDP2` | cantidad_pasaportes | LongType | Cantidad de documentos de identidad registrados |
| `CUSLG` | idioma_preferido | StringType | Idioma de preferencia del cliente |
| `CUSRG` | region_geografica | StringType | Región geográfica de clasificación interna |
| `CUSTP` | tipo_cliente | StringType | Tipo de cliente (RETAIL, CORP, PYME) |
| `CUSSG` | segmento_cliente | StringType | Segmento de cliente (PREMIUM, STANDARD, etc.) |
| `CUSBR` | sucursal_principal | StringType | Código de la sucursal principal asignada |
| `CUSMG` | gerente_asignado | StringType | Código del gerente de cuenta asignado |
| `CUSRF` | referencia_interna | StringType | Referencia interna del cliente |
| `CUSRS` | fuente_referencia | StringType | Fuente de referencia del cliente |
| `CUSAG` | grupo_afinidad | StringType | Grupo de afinidad de marketing |
| `CUSPC` | preferencia_comunicacion | StringType | Canal de comunicación preferido |
| `CUSRK` | nivel_riesgo | StringType | Nivel de riesgo (BAJO, MEDIO, ALTO) |
| `CUSVP` | indicador_vip | StringType | Indicador de cliente VIP (S/N) |
| `CUSPF` | estado_perfil | StringType | Estado del perfil del cliente (ACTIVO, etc.) |
| `CUSKT` | estado_kyc | StringType | Estado del proceso KYC (COMPLETO, PENDIENTE) |
| `CUSFM` | indicador_flags | StringType | Indicadores de alertas internas |
| `CUSLC` | ultimo_canal | StringType | Último canal de interacción |
| `CUSCR` | calificacion_crediticia | StringType | Calificación crediticia interna |
| `CUSAC` | cuenta_activa | StringType | Indicador de cuenta activa principal (S/N) |
| `CUSCL` | clasificacion_interna | StringType | Clasificación interna del cliente |
| `CUSAC2` | cantidad_cuentas | LongType | Cantidad total de cuentas activas |
| `CUSTX` | cantidad_transacciones | LongType | Total de transacciones históricas |
| `CUSSC` | score_cliente | LongType | Score de riesgo interno (300-1150) |
| `CUSLR` | ranking_prestamos | LongType | Ranking de préstamos (0-9) |
| `CUSRC` | cantidad_registros | LongType | Cantidad de registros de historial |
| `CUSIN` | ingresos_cliente | DoubleType | Ingresos mensuales estimados |
| `CUSBL` | saldo_disponible_maestro | DoubleType | Saldo disponible consolidado del maestro |
| `CUSNT` | nota_cliente | StringType | Nota o comentario interno del agente |
| `CUSOD` | fecha_apertura_relacion | DateType | Fecha de apertura de la relación bancaria |
| `CUSCD` | fecha_cierre_relacion | DateType | Fecha de cierre de la relación bancaria |
| `CUSLV` | fecha_ultima_visita | DateType | Fecha de la última visita a sucursal |
| `CUSUD` | fecha_ultima_actualizacion | DateType | Fecha de la última actualización del perfil |
| `CUSKD` | fecha_verificacion_kyc | DateType | Fecha de la última verificación KYC |
| `CUSRD` | fecha_renovacion | DateType | Fecha de renovación del contrato |
| `CUSXD` | fecha_expiracion | DateType | Fecha de expiración del documento principal |
| `CUSFD` | fecha_primer_producto | DateType | Fecha de adquisición del primer producto |
| `CUSLD` | fecha_ultimo_producto | DateType | Fecha de adquisición del último producto |
| `CUSMD2` | fecha_migracion | DateType | Fecha de migración al sistema actual |
| `CUSAD2` | fecha_activacion | DateType | Fecha de activación del cliente |
| `CUSBD` | fecha_bloqueo | DateType | Fecha del último bloqueo de cuenta |
| `CUSVD` | fecha_verificacion | DateType | Fecha de la última verificación de identidad |
| `CUSPD` | fecha_promocion | DateType | Fecha de la última promoción aplicada |
| `CUSDD` | fecha_desactivacion | DateType | Fecha de desactivación del cliente |
| `CUSED2` | fecha_educacion_financiera | DateType | Fecha de completar educación financiera |
| `CUSND` | fecha_notificacion | DateType | Fecha de la última notificación enviada |

## TRXPFL — Transaccional de Clientes (60 columnas)

**Tipo de datos**: 1 LongType (CUSTID) + mixtos (StringType, LongType, DoubleType, DateType, TimestampType)  
**Registros**: 7,000,000 (cantidad modificable vía parámetro) transacciones  
**Llave primaria**: `TRXID` (identificador_transaccion)  
**Foreign Key**: `CUSTID` → CMSTFL.CUSTID  
**Naturaleza de los datos**: Entidad transaccional; cada fila representa una transacción financiera individual con todos sus atributos monetarios, temporales y de clasificación.

| Campo AS400 | Nombre en Español | Tipo PySpark | Descripción |
|-------------|-------------------|-------------|-------------|
| `TRXID` | identificador_transaccion | StringType | Identificador único de la transacción |
| `CUSTID` | identificador_cliente | LongType | Identificador del cliente (FK hacia CMSTFL) |
| `TRXSQ` | secuencia_transaccion | LongType | Número de secuencia de la transacción |
| `TRXTYP` | tipo_transaccion | StringType | Tipo de transacción (DATM, CATM, PGSL, etc.) |
| `TRXCUR` | moneda_transaccion | StringType | Moneda de la transacción (ISO 4217) |
| `TRXST` | estado_transaccion | StringType | Estado de la transacción (APROBADA, REVERTIDA) |
| `TRXCH` | canal_transaccion | StringType | Canal de la transacción (ATM, WEB, APP, etc.) |
| `TRXDSC` | descripcion_transaccion | StringType | Descripción descriptiva de la transacción |
| `TRXREF` | referencia_externa | StringType | Referencia externa o código de autorización |
| `TRXAMT` | monto_principal | DoubleType | Monto principal de la transacción |
| `TRXCM` | comision_transaccion | DoubleType | Comisión cobrada por la transacción |
| `TRXBA` | saldo_posterior | DoubleType | Saldo de la cuenta después de la transacción |
| `TRXBP` | saldo_anterior | DoubleType | Saldo de la cuenta antes de la transacción |
| `TRXTC` | cargo_fiscal | DoubleType | Cargo fiscal aplicado a la transacción |
| `TRXAL` | monto_local | DoubleType | Monto equivalente en moneda local |
| `TRXPN` | monto_pago | DoubleType | Monto de pago aplicado |
| `TRXBF` | beneficio_transaccion | DoubleType | Beneficio o cashback asociado |
| `TRXRL` | perdida_tasa | DoubleType | Pérdida por diferencia de tasa de cambio |
| `TRXMX` | monto_maximo | DoubleType | Monto máximo autorizado para el tipo |
| `TRXMN` | monto_minimo | DoubleType | Monto mínimo requerido para el tipo |
| `TRXAV` | monto_promedio | DoubleType | Promedio histórico del monto de transacciones |
| `TRXDV` | desviacion_monto | DoubleType | Desviación estándar del monto promedio |
| `TRXRK` | riesgo_transaccion | DoubleType | Factor de riesgo de la transacción (0-100) |
| `TRXFR` | riesgo_fraude | DoubleType | Probabilidad de fraude calculada (0-100) |
| `TRXLM` | limite_transaccion | DoubleType | Límite por transacción configurado |
| `TRXLP` | porcentaje_limite | DoubleType | Porcentaje del límite utilizado |
| `TRXCP` | cargo_plataforma | DoubleType | Cargo de la plataforma de pagos |
| `TRXCI` | cargo_institucion | DoubleType | Cargo cobrado por la institución |
| `TRXCF` | cargo_extranjero | DoubleType | Cargo por transacción en moneda extranjera |
| `TRXCV` | cargo_varianza | DoubleType | Cargo adicional por varianza de monto |
| `TRXSB` | subtotal_transaccion | DoubleType | Subtotal antes de impuestos y cargos |
| `TRXTL` | total_transaccion | DoubleType | Total final de la transacción |
| `TRXRS` | residuo_transaccion | DoubleType | Residuo o diferencia de redondeo |
| `TRXIM` | margen_interes | DoubleType | Margen de interés aplicado |
| `TRXNT` | monto_neto | DoubleType | Monto neto de la transacción (TRXAMT - TRXCM) |
| `TRXAO` | monto_original | DoubleType | Monto original antes de ajustes |
| `TRXIN` | monto_inversion | DoubleType | Monto destinado a componente de inversión |
| `TRXDS` | descuento_transaccion | DoubleType | Descuento aplicado promocionalmente |
| `TRXPT` | monto_principal_prestamo | DoubleType | Porción de amortización del préstamo |
| `TRXDT` | fecha_transaccion | DateType | Fecha de la transacción |
| `TRXVD` | fecha_valor | DateType | Fecha valor de la transacción |
| `TRXPD` | fecha_procesamiento | DateType | Fecha de procesamiento en el sistema |
| `TRXSD` | fecha_liquidacion | DateType | Fecha de liquidación de la transacción |
| `TRXCD` | fecha_compensacion | DateType | Fecha de compensación interbancaria |
| `TRXED` | fecha_efectiva | DateType | Fecha efectiva de aplicación |
| `TRXRD` | fecha_reverso | DateType | Fecha de reverso de la transacción (si aplica) |
| `TRXAD` | fecha_autorizacion | DateType | Fecha y hora de autorización |
| `TRXND` | fecha_notificacion_trx | DateType | Fecha de notificación al cliente |
| `TRXXD` | fecha_expiracion_trx | DateType | Fecha de expiración de la autorización |
| `TRXFD` | fecha_fondeo_trx | DateType | Fecha de fondeo de la transacción |
| `TRXGD` | fecha_gracia_trx | DateType | Fecha de período de gracia |
| `TRXHD` | fecha_historica_trx | DateType | Fecha histórica de la transacción AS400 |
| `TRXBD` | fecha_bloqueo_trx | DateType | Fecha de bloqueo temporal de fondos |
| `TRXMD` | fecha_maduracion_trx | DateType | Fecha de maduración del instrumento |
| `TRXLD` | fecha_limite_trx | DateType | Fecha límite de ejecución |
| `TRXUD` | fecha_actualizacion_trx | DateType | Fecha de última actualización del registro |
| `TRXOD` | fecha_origen_trx | DateType | Fecha de origen de la instrucción |
| `TRXKD` | fecha_kyc_trx | DateType | Fecha de verificación KYC asociada |
| `TRXTS` | timestamp_transaccion | TimestampType | Timestamp de la transacción (nanosegundos) |
| `TRXUS` | timestamp_actualizacion | TimestampType | Timestamp de la última actualización |

## BLNCFL — Saldos de Clientes (100 columnas)

**Tipo de datos**: LongType (CUSTID) + StringType (atributos) + DoubleType (saldos) + DateType (fechas)  
**Registros**: 4,000,000 (cantidad modificable vía parámetro) operaciones/secuencias de saldo  
**Relación con CMSTFL**: 1:1 por CUSTID (cada cliente tiene exactamente un registro de saldo)  
**Llave primaria compuesta**: `CUSTID` + `BLSQ`  
**Naturaleza de los datos**: Entidad de saldos/operaciones; cada fila representa el estado actual de una cuenta/operación con todos sus saldos, límites, indicadores y fechas de eventos financieros.

| Campo AS400 | Nombre en Español | Tipo PySpark | Descripción |
|-------------|-------------------|-------------|-------------|
| `CUSTID` | identificador_cliente | LongType | Identificador del cliente (clave primaria/FK hacia CMSTFL) |
| `BLSQ` | secuencia_saldo | LongType | Secuencia del registro de saldo |
| `BLACT` | tipo_cuenta | StringType | Tipo de cuenta bancaria (AHORRO, CORRIENTE, etc.) |
| `BLACN` | numero_cuenta | StringType | Número de cuenta bancaria |
| `BLCUR` | moneda_cuenta | StringType | Moneda de la cuenta (ISO 4217) |
| `BLST` | estado_cuenta | StringType | Estado de la cuenta (ACTIVA, BLOQUEADA, etc.) |
| `BLBR` | sucursal_cuenta | StringType | Código de la sucursal de la cuenta |
| `BLPR` | producto_cuenta | StringType | Código del producto bancario |
| `BLSP` | subproducto_cuenta | StringType | Código del subproducto bancario |
| `BLNM` | nombre_cuenta | StringType | Nombre descriptivo de la cuenta |
| `BLCL` | clase_cuenta | StringType | Clase o categoría de la cuenta |
| `BLRK` | riesgo_cuenta | StringType | Nivel de riesgo asignado a la cuenta |
| `BLTP` | tipo_producto_cuenta | StringType | Tipo específico del producto |
| `BLMG` | gerente_cuenta | StringType | Código del gerente asignado a la cuenta |
| `BLRF` | referencia_cuenta | StringType | Referencia interna de la cuenta |
| `BLCC` | centro_costos_cuenta | StringType | Centro de costos contable |
| `BLAG` | grupo_afinidad_cuenta | StringType | Grupo de afinidad de la cuenta |
| `BLPL` | plan_cuenta | StringType | Plan tarifario de la cuenta |
| `BLRG` | region_cuenta | StringType | Región geográfica de la cuenta |
| `BLSF` | sufijo_cuenta | StringType | Sufijo numérico de la cuenta |
| `BLNT` | nota_cuenta | StringType | Nota interna del operador |
| `BLLC` | ultimo_canal_cuenta | StringType | Último canal de acceso a la cuenta |
| `BLPF` | perfil_cuenta | StringType | Perfil de uso de la cuenta |
| `BLAU` | autorizado_cuenta | StringType | Indicador de autorización activa (S/N) |
| `BLTX` | texto_cuenta | StringType | Texto libre descriptivo |
| `BLGR` | grupo_cuenta | StringType | Grupo contable de la cuenta |
| `BLEM` | email_cuenta | StringType | Email alternativo asociado a la cuenta |
| `BLFR` | frecuencia_cuenta | StringType | Frecuencia de transacciones (ALTA, MEDIA, BAJA) |
| `BLKY` | clave_cuenta | StringType | Clave de seguridad interna |
| `BLVP` | vip_cuenta | StringType | Indicador de cuenta VIP (S/N) |
| `BLFC` | factor_cuenta | StringType | Factor de clasificación interna |
| `BLAV` | saldo_disponible | DoubleType | Saldo disponible para transacciones |
| `BLTB` | saldo_total | DoubleType | Saldo total de la cuenta |
| `BLRV` | saldo_reservado | DoubleType | Monto reservado o bloqueado |
| `BLBK` | saldo_bloqueado | DoubleType | Saldo bloqueado por orden judicial u otro |
| `BLCR` | limite_credito | DoubleType | Límite de crédito aprobado |
| `BLCU` | credito_utilizado | DoubleType | Crédito actualmente utilizado |
| `BLCD` | credito_disponible | DoubleType | Crédito disponible (límite - utilizado) |
| `BLOV` | valor_sobregiro | DoubleType | Valor del sobregiro actual |
| `BLOL` | limite_sobregiro | DoubleType | Límite de sobregiro autorizado |
| `BLPD` | depositos_pendientes | DoubleType | Total depósitos en proceso de acreditación |
| `BLPC` | cargos_pendientes | DoubleType | Total cargos pendientes de debitar |
| `BLPA` | ajustes_pendientes | DoubleType | Ajustes contables pendientes de aplicar |
| `BLDI` | depositos_ingreso | DoubleType | Total de depósitos del período |
| `BLWI` | retenciones_cuenta | DoubleType | Retenciones aplicadas a la cuenta |
| `BLTI` | transferencias_ingreso | DoubleType | Total transferencias recibidas del período |
| `BLTC` | cargos_transferencia | DoubleType | Cargos por transferencias |
| `BLCA` | comisiones_anuales | DoubleType | Total comisiones anuales acumuladas |
| `BLIM` | intereses_mensuales | DoubleType | Total intereses del mes |
| `BLRF2` | reembolsos_cuenta | DoubleType | Total reembolsos acreditados |
| `BLPN` | penalidades_cuenta | DoubleType | Total penalidades cobradas |
| `BLBN` | bonificaciones_cuenta | DoubleType | Total bonificaciones acreditadas |
| `BLAP` | ajustes_positivos | DoubleType | Ajustes positivos (créditos) |
| `BLAM` | ajustes_miscelaneos | DoubleType | Ajustes varios (débitos/créditos) |
| `BLAY` | ajustes_anuales | DoubleType | Ajustes anuales acumulados |
| `BLHI` | marca_alta_saldo | DoubleType | Saldo más alto registrado en el período |
| `BLLO` | marca_baja_saldo | DoubleType | Saldo más bajo registrado en el período |
| `BLVR` | varianza_saldo | DoubleType | Varianza estadística del saldo |
| `BLRT` | ratio_cuenta | DoubleType | Ratio de utilización de crédito |
| `BLCP` | porcentaje_aporte | DoubleType | Porcentaje de aporte al pool de fondos |
| `BLCI` | ingresos_aporte | DoubleType | Ingresos derivados del aporte |
| `BLMN` | saldo_minimo | DoubleType | Saldo mínimo requerido por el producto |
| `BLMX` | saldo_maximo | DoubleType | Saldo máximo permitido por el producto |
| `BLIR` | tasa_interes | DoubleType | Tasa de interés nominal anual |
| `BLPM` | multiplicador_penalidad | DoubleType | Multiplicador para cálculo de penalidades |
| `BLOD` | fecha_apertura_cuenta | DateType | Fecha de apertura de la cuenta |
| `BLXD` | fecha_expiracion_cuenta | DateType | Fecha de expiración del contrato |
| `BLUD` | fecha_actualizacion_cuenta | DateType | Fecha de la última actualización |
| `BLLD` | fecha_ultimo_movimiento | DateType | Fecha del último movimiento registrado |
| `BLSD` | fecha_estado_cuenta | DateType | Fecha del último cambio de estado |
| `BLPD2` | fecha_penalidad | DateType | Fecha de la última penalidad aplicada |
| `BLRD` | fecha_renovacion_cuenta | DateType | Fecha de renovación del contrato |
| `BLMD` | fecha_maduracion | DateType | Fecha de maduración del instrumento financiero |
| `BLCD2` | fecha_cierre_cuenta | DateType | Fecha de cierre de la cuenta |
| `BLBD` | fecha_bloqueo_cuenta | DateType | Fecha del último bloqueo |
| `BLFD` | fecha_fondeo | DateType | Fecha de fondeo inicial |
| `BLGD` | fecha_gracia | DateType | Fecha de fin del período de gracia |
| `BLHD` | fecha_historica | DateType | Fecha histórica de referencia AS400 |
| `BLID` | fecha_interes | DateType | Fecha de liquidación de intereses |
| `BLJD` | fecha_ajuste | DateType | Fecha del último ajuste contable |
| `BLKD` | fecha_kyc_cuenta | DateType | Fecha de verificación KYC de la cuenta |
| `BLND` | fecha_notificacion_cuenta | DateType | Fecha de la última notificación de la cuenta |
| `BLTD` | fecha_transferencia | DateType | Fecha de la última transferencia |
| `BLVD` | fecha_verificacion_cuenta | DateType | Fecha de verificación de la cuenta |
| `BLWD` | fecha_retiro | DateType | Fecha del último retiro |
| `BLYD` | fecha_rendimiento | DateType | Fecha de cálculo de rendimiento |
| `BLZD` | fecha_cierre_periodo | DateType | Fecha de cierre del período contable |
| `BLED` | fecha_evaluacion | DateType | Fecha de evaluación del riesgo |
| `BLAD2` | fecha_activacion_cuenta | DateType | Fecha de activación de la cuenta |
| `BLDD` | fecha_desactivacion_cuenta | DateType | Fecha de desactivación de la cuenta |
| `BLFP` | fecha_primer_pago | DateType | Fecha del primer pago realizado |
| `BLLP` | fecha_ultimo_pago | DateType | Fecha del último pago realizado |
| `BLMP` | fecha_mora_pago | DateType | Fecha de inicio de mora |
| `BLNP` | fecha_proximo_pago | DateType | Fecha del próximo pago programado |
| `BLOP` | fecha_origen_pago | DateType | Fecha de origen de la instrucción de pago |
| `BLPP` | fecha_programacion_pago | DateType | Fecha de programación del pago |
| `BLQP` | fecha_quiebre_pago | DateType | Fecha de quiebre del acuerdo de pago |
| `BLRP` | fecha_regularizacion_pago | DateType | Fecha de regularización del pago |
| `BLSP2` | fecha_suspension_pago | DateType | Fecha de suspensión del pago |
| `BLTP2` | fecha_tercero_pago | DateType | Fecha de pago a tercero |

---


## Hallazgos del Análisis de Datos (Parquet)

### 2.1 Hallazgos del Análisis de Datos (Parquet) — Dos Snapshots

Se analizaron los datos completos de los parquets en `Temporal/` con **dos snapshots**: `Fecha=2026-04-03` y `Fecha=2026-04-04`. Los hallazgos se presentan por tabla, incluyendo la **variación campo a campo entre ambos días**, que es el dato más valioso para el diseño de Satellites.

### 2.1.1 Volumetría Real (Dos Snapshots)

| Tabla | Filas Día 1 (2026-04-03) | Filas Día 2 (2026-04-04) | Diferencia | Filas Documentadas SYSTEM.md |
|-------|-------------------------|-------------------------|------------|------------------------------|
| **CMSTFL** | **75,000** | **75,450** | +450 (0.6%) | 4,000,000 |
| **TRXPFL** | **250,000** | **215,750** | -34,250 (-13.7%) | 7,000,000 |
| **BLNCFL** | **75,000** | **75,450** | +450 (0.6%) | 4,000,000 |

**Observaciones sobre volumetría**:
- CMSTFL y BLNCFL crecen en la misma cantidad (+450), confirmando la relación 1:1.
- TRXPFL varía entre días: no todos los TRXIDs del día 1 aparecen en el día 2 (34,250 se eliminan). Cero TRXIDs nuevos aparecen en día 2 (es un subconjunto del día 1).
- Las cantidades son diferentes a las documentadas en SYSTEM.md (que indica 50K/150K/50K como parámetro default), pero están en el orden de magnitud correcto.

### 2.1.2 CMSTFL — Maestro de Clientes

**Esquema real**: 70 columnas — 1 `int64` (CUSTID) + 41 `string` + 8 `int64` + 18 `date32[day]` + 2 `double` = 70 columnas (coincide con SYSTEM.md)

**Hallazgos**:
- 75,000 CUSTIDs únicos en Día 1, 75,450 en Día 2. **100% única** (PK verdadera).
- **Cero valores nulos** en todas las columnas, ambos días.
- 75,000 CUSTIDs comunes entre ambos días (0 eliminados, +450 nuevos en Día 2).

**Distribución de campos categóricos** (Día 1):

| Campo | Valores | Distribución |
|-------|---------|-------------|
| `CUSSX` | 2 | F: 50.0%, M: 50.0% |
| `CUSTP` | 2 | IND: 50.3%, COR: 49.7% |
| `CUSSG` | 3 | PREM: 33.6%, STD: 33.2%, BAS: 33.1% |
| `CUSRK` | 4 | LOW: 25.2%, CRT: 25.0%, MED: 24.9%, HIG: 24.9% |
| `CUSMS` | 4 | WDW: 25.1%, SNG: 25.0%, DIV: 25.0%, MRD: 24.8% |
| `CUSKT` | 3 | COMP: 33.4%, EXPD: 33.4%, PEND: 33.2% |
| `CUSPF` | 2 | Y: 50.1%, N: 49.9% |
| `CUSAC` | 3 | A: 33.5%, S: 33.4%, I: 33.2% |
| `CUSVP` | 2 | N: 50.2%, Y: 49.8% |
| `CUSLC` | 4 | MOB: 25.1%, ATM: 25.0%, ONL: 24.9%, BRN: 24.9% |
| `CUSCR` | 8 | BBB a A: ~12.5% cada una (distribución uniforme) |
| `CUSTT` | 4 | Mrs: 25.2%, Ms: 25.0%, Mr: 24.9%, Dr: 24.9% |
| `CUSCL` | 5 | CLF01 a CLF05: ~20% cada una |
| `CUSFM` | 2 | N: 50.1%, Y: 49.9% |

**Rangos numéricos** (Día 1):

| Campo | Min | Max | Media | Descripción |
|-------|-----|-----|-------|-------------|
| `CUSAG2` | 19 | 56 | 37.38 | Edad del cliente |
| `CUSSC` | 300 | 1,149 | 723.80 | Score crediticio |
| `CUSIN` | 10.46 | 99,998.85 | 49,907.57 | Ingresos mensuales |
| `CUSBL` | 11.75 | 99,999.68 | 49,876.36 | Saldo maestro |
| `CUSYR` | 1970 | 2007 | 1,988.62 | Año de nacimiento |
| `CUSAC2` | 1 | 5 | 3.01 | Cantidad de cuentas |
| `CUSTX` | 1 | 500 | 250.38 | Total transacciones históricas |
| `CUSLR` | 0 | 9 | 4.52 | Ranking préstamos |
| `CUSRC` | 0 | 49 | 24.54 | Cantidad registros historial |
| `CUSDP2` | 0 | 5 | 2.50 | Cantidad documentos identidad |

**Rangos de fechas**: Todas las fechas operan en el rango `2005-01-01` a `2025-12-30`, excepto `CUSDB` (fecha nacimiento): `1970-01-01` a `2007-12-30`.

#### Comparación CMSTFL entre Día 1 y Día 2 (CLAVE PARA DISEÑO DE SATELLITES)

**15 campos que CAMBIARON** (afectaron ~15-20% de los 75,000 registros comunes):

| Campo | % Cambio | Categoría Funcional |
|-------|----------|---------------------|
| `CUSFN` (nombre_completo) | 20.00% | Datos personales |
| `CUSA2` (dirección_apartamento) | 20.00% | Contacto/Dirección |
| `CUSZP` (código_postal) | 20.00% | Contacto/Dirección |
| `CUSPH` (teléfono_principal) | 20.00% | Contacto |
| `CUSMB` (teléfono_móvil) | 20.00% | Contacto |
| `CUSEM` (correo_electrónico) | 20.00% | Contacto |
| `CUSAD` (dirección_calle) | 20.00% | Contacto/Dirección |
| `CUSMD` (nombre_medio) | 19.93% | Datos personales |
| `CUSNM` (nombre) | 19.91% | Datos personales |
| `CUSLN` (apellido) | 19.91% | Datos personales |
| `CUSCT` (ciudad_residencia) | 18.73% | Contacto/Dirección |
| `CUSOC` (ocupación) | 18.59% | Datos personales |
| `CUSST` (estado_provincia) | 18.04% | Contacto/Dirección |
| `CUSED` (nivel_educativo) | 15.97% | Datos personales |
| `CUSMS` (estado_civil) | 14.98% | Datos personales |

**54 campos que NO cambiaron (0.00% de variación entre días)**:
- **Identidad**: CUSSX, CUSTT
- **Geográficos estables**: CUSCN, CUSNA, CUSRG
- **Clasificación bancaria**: CUSTP, CUSSG, CUSBR, CUSMG, CUSRF, CUSRS, CUSLG, CUSAG, CUSPC, CUSRK, CUSVP, CUSPF, CUSKT, CUSFM, CUSLC, CUSCR, CUSAC, CUSCL, CUSNT
- **Todos los numéricos**: CUSYR, CUSAG2, CUSDP2, CUSAC2, CUSTX, CUSSC, CUSLR, CUSRC, CUSIN, CUSBL
- **Todas las 18 fechas**: CUSDB, CUSOD, CUSCD, CUSLV, CUSUD, CUSKD, CUSRD, CUSXD, CUSFD, CUSLD, CUSMD2, CUSAD2, CUSBD, CUSVD, CUSPD, CUSDD, CUSED2, CUSND

> **Decisión para Satellite**: Los 15 campos que cambian entre snapshots son todos de naturaleza **contacto/datos personales**. Los 54 campos estables permiten una separación NÍTIDA entre Satellites de alta vs. baja variación.

### 2.1.3 TRXPFL — Transaccional de Clientes

**Esquema real**: 60 columnas — `TRXID` es **string** (NO int64 como indica SYSTEM.md), `CUSTID` es `int64`, `TRXSQ` es `int64`, 6 `string`, 19 `date32[day]`, 2 `timestamp[ns]`, 29 `double` = 60 columnas (coincide con SYSTEM.md)

**Hallazgo crítico de tipo de dato**: `TRXID` es **StringType** en el Parquet, pero SYSTEM.md lo documenta como `LongType`. Esto afecta el cálculo de hashes: no se requiere cast a string previo.

**Hallazgos** (Día 1):
- 250,000 transacciones únicas. **100% única** por TRXID (PK verdadera).
- 72,322 CUSTIDs únicos (96.4% de los 75,000 en CMSTFL Día 1).
- **Cero valores nulos** en todas las columnas.
- **TRXDT**: Todas las transacciones tienen la misma fecha: `2026-04-03` (= fecha del snapshot).
- **ATM**: 72,502 transacciones (29.0% del total) — CATM: 37,454 (15.0%) + DATM: 35,048 (14.0%).

**Distribución de campos categóricos** (Día 1):

| Campo | Valores | Distribución |
|-------|---------|-------------|
| `TRXTYP` | 15 | CATM: 15.0%, DATM: 14.0%, CMPR: 13.0%, TINT: 10.0%, DPST: 8.0%, PGSL: 7.0%, TEXT: 6.0%, PGSV: 5.0%, RTRO: 4.9%, NMNA: 4.0%, ADSL: 3.0%, INTR: 3.0%, IMPT: 3.0%, DMCL: 2.0%, CMSN: 2.0% |
| `TRXCUR` | 5 | GBP/EUR/ILS/USD/EGP: ~20% cada una |
| `TRXST` | 4 | APPR/DECL/REVS/PEND: ~25% cada uno |
| `TRXCH` | 5 | ONL/MOB/ATM/POS/BRN: ~20% cada uno |

**Rangos numéricos clave** (Día 1):

| Campo | Min | Max | Media | Nota |
|-------|-----|-----|-------|------|
| `TRXAMT` | 10.05 | 99,999.55 | 49,972.89 | Monto principal |
| `TRXRK` | 0.0004 | 99.9999 | 49.90 | **Escala 0-100**, NO 0-1 como documenta SYSTEM.md |
| `TRXFR` | 0.0004 | 99.9999 | 49.89 | **Escala 0-100**, NO 0-1 como documenta SYSTEM.md |
| `TRXTL` | 134.57 | 101,950.42 | 50,990.38 | Total transacción |
| `TRXNT` | -196,872.58 | 96,796.92 | -50,007.78 | Monto neto (**puede ser negativo**) |
| `TRXMX` | constante | constante | — | Fijo para todos (Día 1 y 2) |
| `TRXMN` | constante | constante | — | Fijo para todos (Día 1 y 2) |
| `TRXSQ` | 1 | 250,000 | 125,000.50 | Secuencia secuencial |

**Hallazgo** (Día 2):
- 215,750 transacciones (34,250 menos que Día 1).
- 71,183 CUSTIDs únicos (94.3% de los 75,450 clientes del Día 2).
- TRXDT: Todas = `2026-04-04`.
- **ATM Día 2**: 62,679 (29.1%) — CATM: 32,406, DATM: 30,273. Proporción ATM estable (~29%).

#### Comparación TRXPFL entre Día 1 y Día 2

- **215,750 TRXIDs comunes** entre ambos días (100% de Día 2 existe en Día 1).
- **34,250 TRXIDs** solo en Día 1 (desaparecen en Día 2). 0 TRXIDs nuevos en Día 2.

**Para los 215,750 TRXIDs comunes**:

| Campo(s) | % Cambio | Interpretación |
|-----------|----------|----------------|
| `TRXDT` | **100.0%** | Cambia del 2026-04-03 al 2026-04-04 (= fecha del snapshot) |
| `CUSTID` | **99.8%** | Los TRXIDs se **reasignan** a diferentes clientes entre snapshots |
| Todos los DoubleType monetarios (TRXAMT, TRXCM, TRXBA, TRXBP, TRXTC, TRXAL, etc.) + TRXRK, TRXFR | **87.5%** cada uno | Montos y riesgos se regeneran |
| `TRXTYP`, `TRXCUR`, `TRXST`, `TRXCH`, `TRXDSC`, `TRXREF` | **0.0%** | Atributos categóricos estables |
| `TRXSQ` | **0.0%** | Secuencia fija |
| Todas las fechas (excepto TRXDT): TRXVD, TRXPD, TRXSD, etc. | **0.0%** | Fechas auxiliares estables |
| `TRXTS`, `TRXUS` | **0.0%** | Timestamps estables |
| `TRXMX`, `TRXMN` | **0.0%** | Montos máximo/mínimo fijos |

> **Decisión para Satellites**: Clara separación entre atributos estables (categóricos, fechas, secuencia = 29 campos, 0% cambio) y atributos variables (montos, riesgos = 30 campos, 87.5-100% cambio).
> **Nota sobre TRXDT**: Siempre coincide con la fecha del snapshot. Esto confirma que es la fecha de "corte" del snapshot, no una fecha de evento individual. Para el hub de transacciones, se debe considerar que TRXDT representa la fecha de extracción, no de ejecución.

### 2.1.4 BLNCFL — Saldos de Clientes

**Esquema real**: 100 columnas — `CUSTID` y `BLSQ` como `int64`, 30 `string`, 34 `double`, 34 `date32[day]` = 100 columnas (coincide con SYSTEM.md)

**Nota sobre esquema**: El campo documentado como `BLCN` en SYSTEM.md aparece como **`BLCU`** en el Parquet real. Se trata de `credito_utilizado`.

**Hallazgos** (Día 1):
- 75,000 registros. **CUSTID 100% único** y **BLSQ 100% único**. Relación **1:1** con CMSTFL confirmada.
- **(CUSTID, BLSQ) combinada**: 75,000 únicos (PK compuesta verdadera).
- **Cero valores nulos** en todas las columnas.
- **100%** de los CUSTIDs de BLNCFL existen en CMSTFL (integridad referencial perfecta).

**Distribución de campos categóricos** (Día 1):

| Campo | Valores | Distribución |
|-------|---------|-------------|
| `BLACT` | 4 | AHRO: 40.1%, CRTE: 29.9%, PRES: 20.0%, INVR: 10.0% |
| `BLST` | 4 | ACTV: 25.4%, CERR: 25.1%, SUSP: 24.8%, INAC: 24.7% |
| `BLCUR` | 5 | GBP/EGP/ILS/EUR/USD: ~20% cada una |
| `BLRK` | 3 | MED: 33.6%, LOW: 33.2%, HIG: 33.2% |
| `BLTP` | 3 | AUT: 33.4%, PRI: 33.4%, SEC: 33.3% |
| `BLCL` | 4 | CLF01/CLF02/CLF03/CLF04: ~25% cada una |
| `BLFR` | 2 | N: 50.4%, Y: 49.6% |
| `BLVP` | 2 | N: 50.2%, Y: 49.8% |
| `BLAU` | 2 | N: 50.2%, Y: 49.8% |

**Rangos numéricos clave** (Día 1):

| Campo | Min | Max | Media | Nota |
|-------|-----|-----|-------|------|
| `BLAV` (saldo disponible) | 10.26 | 99,999.73 | 49,811.75 | — |
| `BLTB` (saldo total) | 14.12 | 99,997.97 | 50,078.60 | — |
| `BLCR` (límite crédito) | 12.49 | 200,006.27 | 100,132.41 | Rango hasta 200K |
| `BLCU` (crédito utilizado) | 10.29 | 100,009.61 | 49,936.05 | — |
| `BLOV` (sobregiro) | 0.10 | 4,999.93 | 2,501.50 | — |
| `BLMN` (saldo mínimo) | **10.00** | **10.00** | **10.00** | **Constante** |
| `BLMX` (saldo máximo) | **100,000** | **100,000** | **100,000** | **Constante** |
| `BLIR` (tasa interés) | 0.00 | 0.25 | 0.12 | Escala 0-0.25 |
| `BLRT` (ratio utilización) | 0.00 | 0.20 | 0.10 | Escala 0-0.20 |
| `BLLO` (marca baja saldo) | **10.00** | **10.00** | **10.00** | **Constante** |
| `BLHI` (marca alta saldo) | 11.77 | 100,009.93 | 50,119.03 | — |
| `BLVR` (varianza) | -4,999.89 | 4,999.88 | 0.91 | Puede ser negativa |
| `BLPM` (multiplicador penalidad) | 0.00 | 0.15 | 0.07 | — |

#### Comparación BLNCFL entre Día 1 y Día 2

- 75,000 CUSTIDs comunes. 0 eliminados. +450 nuevos en Día 2.
- **CERO campos cambiaron** para los 75,000 registros comunes.
- **Las 99 columnas** (excluido CUSTID como índice) son **idénticas** entre ambos días.

> **Decisión para Satellites**: BLNCFL es perfectamente estático entre snapshots. Cualquier separación de Satellites debe basarse en la **naturaleza funcional** de los campos (atributos de cuenta vs. montos vs. fechas), no en la tasa de cambio real observada. Aun así, los montos y saldos son los campos que **en producción real** cambiarían con mayor frecuencia, por lo que la separación por concepto sigue siendo pertinente.

### 2.1.5 Relaciones entre Tablas (Día 2, 2026-04-04)

| Relación | Valor | Interpretación |
|----------|-------|----------------|
| CMSTFL CUSTIDs | 75,450 | Universo total de clientes |
| BLNCFL CUSTIDs | 75,450 **(100% de CMSTFL)** | Relación 1:1 perfecta |
| TRXPFL CUSTIDs | 71,183 **(94.3% de CMSTFL)** | 5.7% de clientes sin transacciones |
| TRXPFL ∩ BLNCFL | 71,183 | Todos los clientes con txns tienen saldo |
| TRXPFL no en CMSTFL | **0** | Integridad referencial: 100% |
| BLNCFL no en CMSTFL | **0** | Integridad referencial: 100% |
| Txns por cliente | min=1, max=13, media=3.0, P95=6 | Distribución moderada |

### 2.1.6 Discrepancias Detectadas entre Documentación y Datos Reales

| # | Discrepancia | En SYSTEM.md | En Datos Reales | Impacto |
|---|-------------|--------------|-----------------|---------|
| 1 | Tipo de `TRXID` | LongType | **StringType** | Hash no requiere cast previo |
| 2 | Escala de `TRXRK` | 0.0–1.0 | **0–100** | Umbrales de campos calculados deben ajustarse |
| 3 | Escala de `TRXFR` | 0.0–1.0 | **0–100** | Umbrales de campos calculados deben ajustarse |
| 4 | Nombre columna BLNCFL | `BLCN` (credito_utilizado) | **`BLCU`** | Código debe usar nombre real del parquet |
| 5 | Cobertura transaccional | Implícita 100% | **94.3%** de clientes | JOINs deben considerar clientes sin txns |
| 6 | Volumetría | 50K/150K/50K | **75K/250K–215K/75K** | Parámetro diferente al default, más los +450 diarios |


### 2.2 Esquema de Tablas de Bronce (Raw Data)

> **Convención de nombres**: Las tablas de Bronce conservan los **nombres originales de los campos del Parquet** (códigos AS400). Bronce es la capa de datos crudos y puros, sin transformaciones de negocio.
>
> Para cada Parquet fuente se crean **dos tablas**:
> - **Streaming Table Temporal** (`{PARQUET}_temp`): Ingesta incremental con AutoLoader (esquema evolutivo). `temporary=True` — no se registra en Unity Catalog. Incluye la columna `_rescued_data` generada automáticamente por AutoLoader para capturar datos que no coincidan con el esquema esperado.
> - **Vista Materializada** (`{PARQUET}`): Snapshot del corte más reciente (`MAX(FechaRegistroParquet)`). Se registra en Unity Catalog bajo `{catalogo}.{esquema}`. Hereda la columna `_rescued_data` de la Streaming Table fuente.
>
> **Columnas exclusivas de Bronce (NO se propagan a Plata ni a Oro)**:
> Los siguientes campos son de uso interno de la capa de Bronce y **no se llevan a la capa de Plata** (y por ende tampoco a Oro): `año`, `mes`, `dia`, `FechaRegistroParquet` y `_rescued_data`. Su propósito es exclusivamente la gestión de la ingesta incremental y la identificación del snapshot más reciente dentro de Bronce.

#### CMSTFL_temp — Streaming Table Temporal (70 columnas + 2 generadas)

| # | Columna | Tipo de Dato (Parquet) | Origen |
|---|---------|----------------------|--------|
| 1 | **`FechaRegistroParquet`** | DateType | **Generada**: `TO_DATE(CONCAT_WS('-', año, mes, dia))` |
| 2 | `CUSTID` | int64 → LongType | Parquet |
| 3 | `CUSNM` | string → StringType | Parquet |
| 4 | `CUSLN` | string → StringType | Parquet |
| 5 | `CUSMD` | string → StringType | Parquet |
| 6 | `CUSFN` | string → StringType | Parquet |
| 7 | `CUSSX` | string → StringType | Parquet |
| 8 | `CUSTT` | string → StringType | Parquet |
| 9 | `CUSAD` | string → StringType | Parquet |
| 10 | `CUSA2` | string → StringType | Parquet |
| 11 | `CUSCT` | string → StringType | Parquet |
| 12 | `CUSST` | string → StringType | Parquet |
| 13 | `CUSZP` | string → StringType | Parquet |
| 14 | `CUSCN` | string → StringType | Parquet |
| 15 | `CUSPH` | string → StringType | Parquet |
| 16 | `CUSMB` | string → StringType | Parquet |
| 17 | `CUSEM` | string → StringType | Parquet |
| 18 | `CUSTP` | string → StringType | Parquet |
| 19 | `CUSSG` | string → StringType | Parquet |
| 20 | `CUSMS` | string → StringType | Parquet |
| 21 | `CUSOC` | string → StringType | Parquet |
| 22 | `CUSED` | string → StringType | Parquet |
| 23 | `CUSNA` | string → StringType | Parquet |
| 24 | `CUSDL` | string → StringType | Parquet |
| 25 | `CUSDP` | string → StringType | Parquet |
| 26 | `CUSRG` | string → StringType | Parquet |
| 27 | `CUSBR` | string → StringType | Parquet |
| 28 | `CUSMG` | string → StringType | Parquet |
| 29 | `CUSRF` | string → StringType | Parquet |
| 30 | `CUSRS` | string → StringType | Parquet |
| 31 | `CUSLG` | string → StringType | Parquet |
| 32 | `CUSNT` | string → StringType | Parquet |
| 33 | `CUSAG` | string → StringType | Parquet |
| 34 | `CUSPC` | string → StringType | Parquet |
| 35 | `CUSRK` | string → StringType | Parquet |
| 36 | `CUSVP` | string → StringType | Parquet |
| 37 | `CUSPF` | string → StringType | Parquet |
| 38 | `CUSKT` | string → StringType | Parquet |
| 39 | `CUSFM` | string → StringType | Parquet |
| 40 | `CUSLC` | string → StringType | Parquet |
| 41 | `CUSCR` | string → StringType | Parquet |
| 42 | `CUSAC` | string → StringType | Parquet |
| 43 | `CUSCL` | string → StringType | Parquet |
| 44 | `CUSDB` | date32[day] → DateType | Parquet |
| 45 | `CUSOD` | date32[day] → DateType | Parquet |
| 46 | `CUSCD` | date32[day] → DateType | Parquet |
| 47 | `CUSLV` | date32[day] → DateType | Parquet |
| 48 | `CUSUD` | date32[day] → DateType | Parquet |
| 49 | `CUSKD` | date32[day] → DateType | Parquet |
| 50 | `CUSRD` | date32[day] → DateType | Parquet |
| 51 | `CUSXD` | date32[day] → DateType | Parquet |
| 52 | `CUSFD` | date32[day] → DateType | Parquet |
| 53 | `CUSLD` | date32[day] → DateType | Parquet |
| 54 | `CUSMD2` | date32[day] → DateType | Parquet |
| 55 | `CUSAD2` | date32[day] → DateType | Parquet |
| 56 | `CUSBD` | date32[day] → DateType | Parquet |
| 57 | `CUSVD` | date32[day] → DateType | Parquet |
| 58 | `CUSPD` | date32[day] → DateType | Parquet |
| 59 | `CUSDD` | date32[day] → DateType | Parquet |
| 60 | `CUSED2` | date32[day] → DateType | Parquet |
| 61 | `CUSND` | date32[day] → DateType | Parquet |
| 62 | `CUSYR` | int64 → LongType | Parquet |
| 63 | `CUSAG2` | int64 → LongType | Parquet |
| 64 | `CUSDP2` | int64 → LongType | Parquet |
| 65 | `CUSAC2` | int64 → LongType | Parquet |
| 66 | `CUSTX` | int64 → LongType | Parquet |
| 67 | `CUSSC` | int64 → LongType | Parquet |
| 68 | `CUSLR` | int64 → LongType | Parquet |
| 69 | `CUSRC` | int64 → LongType | Parquet |
| 70 | `CUSIN` | double → DoubleType | Parquet |
| 71 | `CUSBL` | double → DoubleType | Parquet |
| 72 | `_rescued_data` | StringType | **Generada**: AutoLoader (esquema evolutivo) |
| — | `año` | string → StringType | Partición (inferida por AutoLoader) |
| — | `mes` | string → StringType | Partición (inferida por AutoLoader) |
| — | `dia` | string → StringType | Partición (inferida por AutoLoader) |

#### CMSTFL — Vista Materializada / Snapshot Más Reciente

Mismo esquema que `CMSTFL_temp` (72 columnas). Filtrado por `FechaRegistroParquet = MAX(FechaRegistroParquet)`. Se registra en Unity Catalog como `{catalogo}.{esquema}.CMSTFL`.

**Nombre LSDP**: `@dp.materialized_view(name=f"{catalogo}.{esquema}.CMSTFL", cluster_by=["FechaRegistroParquet"])`

---

#### TRXPFL_temp — Streaming Table Temporal (60 columnas + 2 generadas)

| # | Columna | Tipo de Dato (Parquet) | Origen |
|---|---------|----------------------|--------|
| 1 | **`FechaRegistroParquet`** | DateType | **Generada**: `TO_DATE(CONCAT_WS('-', año, mes, dia))` |
| 2 | `TRXID` | string → StringType | Parquet |
| 3 | `CUSTID` | int64 → LongType | Parquet |
| 4 | `TRXTYP` | string → StringType | Parquet |
| 5 | `TRXAMT` | double → DoubleType | Parquet |
| 6 | `TRXCUR` | string → StringType | Parquet |
| 7 | `TRXST` | string → StringType | Parquet |
| 8 | `TRXCH` | string → StringType | Parquet |
| 9 | `TRXDSC` | string → StringType | Parquet |
| 10 | `TRXREF` | string → StringType | Parquet |
| 11 | `TRXSQ` | int64 → LongType | Parquet |
| 12 | `TRXDT` | date32[day] → DateType | Parquet |
| 13 | `TRXVD` | date32[day] → DateType | Parquet |
| 14 | `TRXPD` | date32[day] → DateType | Parquet |
| 15 | `TRXSD` | date32[day] → DateType | Parquet |
| 16 | `TRXCD` | date32[day] → DateType | Parquet |
| 17 | `TRXED` | date32[day] → DateType | Parquet |
| 18 | `TRXRD` | date32[day] → DateType | Parquet |
| 19 | `TRXAD` | date32[day] → DateType | Parquet |
| 20 | `TRXND` | date32[day] → DateType | Parquet |
| 21 | `TRXXD` | date32[day] → DateType | Parquet |
| 22 | `TRXFD` | date32[day] → DateType | Parquet |
| 23 | `TRXGD` | date32[day] → DateType | Parquet |
| 24 | `TRXHD` | date32[day] → DateType | Parquet |
| 25 | `TRXBD` | date32[day] → DateType | Parquet |
| 26 | `TRXMD` | date32[day] → DateType | Parquet |
| 27 | `TRXLD` | date32[day] → DateType | Parquet |
| 28 | `TRXUD` | date32[day] → DateType | Parquet |
| 29 | `TRXOD` | date32[day] → DateType | Parquet |
| 30 | `TRXKD` | date32[day] → DateType | Parquet |
| 31 | `TRXTS` | timestamp[ns] → TimestampType | Parquet |
| 32 | `TRXUS` | timestamp[ns] → TimestampType | Parquet |
| 33 | `TRXBA` | double → DoubleType | Parquet |
| 34 | `TRXBP` | double → DoubleType | Parquet |
| 35 | `TRXCM` | double → DoubleType | Parquet |
| 36 | `TRXIM` | double → DoubleType | Parquet |
| 37 | `TRXNT` | double → DoubleType | Parquet |
| 38 | `TRXTC` | double → DoubleType | Parquet |
| 39 | `TRXAO` | double → DoubleType | Parquet |
| 40 | `TRXAL` | double → DoubleType | Parquet |
| 41 | `TRXIN` | double → DoubleType | Parquet |
| 42 | `TRXPN` | double → DoubleType | Parquet |
| 43 | `TRXDS` | double → DoubleType | Parquet |
| 44 | `TRXBF` | double → DoubleType | Parquet |
| 45 | `TRXPT` | double → DoubleType | Parquet |
| 46 | `TRXRL` | double → DoubleType | Parquet |
| 47 | `TRXMX` | double → DoubleType | Parquet |
| 48 | `TRXMN` | double → DoubleType | Parquet |
| 49 | `TRXAV` | double → DoubleType | Parquet |
| 50 | `TRXDV` | double → DoubleType | Parquet |
| 51 | `TRXRK` | double → DoubleType | Parquet |
| 52 | `TRXFR` | double → DoubleType | Parquet |
| 53 | `TRXLM` | double → DoubleType | Parquet |
| 54 | `TRXLP` | double → DoubleType | Parquet |
| 55 | `TRXCP` | double → DoubleType | Parquet |
| 56 | `TRXCI` | double → DoubleType | Parquet |
| 57 | `TRXCF` | double → DoubleType | Parquet |
| 58 | `TRXCV` | double → DoubleType | Parquet |
| 59 | `TRXSB` | double → DoubleType | Parquet |
| 60 | `TRXTL` | double → DoubleType | Parquet |
| 61 | `TRXRS` | double → DoubleType | Parquet |
| 62 | `_rescued_data` | StringType | **Generada**: AutoLoader (esquema evolutivo) |
| — | `año` | string → StringType | Partición (inferida por AutoLoader) |
| — | `mes` | string → StringType | Partición (inferida por AutoLoader) |
| — | `dia` | string → StringType | Partición (inferida por AutoLoader) |

#### TRXPFL — Vista Materializada / Snapshot Más Reciente

Mismo esquema que `TRXPFL_temp` (62 columnas). Filtrado por `FechaRegistroParquet = MAX(FechaRegistroParquet)`. Se registra en Unity Catalog como `{catalogo}.{esquema}.TRXPFL`.

**Nombre LSDP**: `@dp.materialized_view(name=f"{catalogo}.{esquema}.TRXPFL", cluster_by=["FechaRegistroParquet"])`

---

#### BLNCFL_temp — Streaming Table Temporal (100 columnas + 2 generadas)

| # | Columna | Tipo de Dato (Parquet) | Origen |
|---|---------|----------------------|--------|
| 1 | **`FechaRegistroParquet`** | DateType | **Generada**: `TO_DATE(CONCAT_WS('-', año, mes, dia))` |
| 2 | `CUSTID` | int64 → LongType | Parquet |
| 3 | `BLSQ` | int64 → LongType | Parquet |
| 4 | `BLACT` | string → StringType | Parquet |
| 5 | `BLACN` | string → StringType | Parquet |
| 6 | `BLCUR` | string → StringType | Parquet |
| 7 | `BLST` | string → StringType | Parquet |
| 8 | `BLBR` | string → StringType | Parquet |
| 9 | `BLPR` | string → StringType | Parquet |
| 10 | `BLSP` | string → StringType | Parquet |
| 11 | `BLNM` | string → StringType | Parquet |
| 12 | `BLCL` | string → StringType | Parquet |
| 13 | `BLRK` | string → StringType | Parquet |
| 14 | `BLTP` | string → StringType | Parquet |
| 15 | `BLMG` | string → StringType | Parquet |
| 16 | `BLRF` | string → StringType | Parquet |
| 17 | `BLCC` | string → StringType | Parquet |
| 18 | `BLAG` | string → StringType | Parquet |
| 19 | `BLPL` | string → StringType | Parquet |
| 20 | `BLRG` | string → StringType | Parquet |
| 21 | `BLSF` | string → StringType | Parquet |
| 22 | `BLNT` | string → StringType | Parquet |
| 23 | `BLLC` | string → StringType | Parquet |
| 24 | `BLPF` | string → StringType | Parquet |
| 25 | `BLAU` | string → StringType | Parquet |
| 26 | `BLTX` | string → StringType | Parquet |
| 27 | `BLGR` | string → StringType | Parquet |
| 28 | `BLEM` | string → StringType | Parquet |
| 29 | `BLFR` | string → StringType | Parquet |
| 30 | `BLKY` | string → StringType | Parquet |
| 31 | `BLVP` | string → StringType | Parquet |
| 32 | `BLFC` | string → StringType | Parquet |
| 33 | `BLAV` | double → DoubleType | Parquet |
| 34 | `BLTB` | double → DoubleType | Parquet |
| 35 | `BLRV` | double → DoubleType | Parquet |
| 36 | `BLBK` | double → DoubleType | Parquet |
| 37 | `BLMN` | double → DoubleType | Parquet |
| 38 | `BLMX` | double → DoubleType | Parquet |
| 39 | `BLIR` | double → DoubleType | Parquet |
| 40 | `BLPM` | double → DoubleType | Parquet |
| 41 | `BLCR` | double → DoubleType | Parquet |
| 42 | `BLCU` | double → DoubleType | Parquet |
| 43 | `BLCD` | double → DoubleType | Parquet |
| 44 | `BLOV` | double → DoubleType | Parquet |
| 45 | `BLOL` | double → DoubleType | Parquet |
| 46 | `BLPD` | double → DoubleType | Parquet |
| 47 | `BLPC` | double → DoubleType | Parquet |
| 48 | `BLPA` | double → DoubleType | Parquet |
| 49 | `BLDI` | double → DoubleType | Parquet |
| 50 | `BLWI` | double → DoubleType | Parquet |
| 51 | `BLTI` | double → DoubleType | Parquet |
| 52 | `BLTC` | double → DoubleType | Parquet |
| 53 | `BLCA` | double → DoubleType | Parquet |
| 54 | `BLIM` | double → DoubleType | Parquet |
| 55 | `BLRF2` | double → DoubleType | Parquet |
| 56 | `BLPN` | double → DoubleType | Parquet |
| 57 | `BLBN` | double → DoubleType | Parquet |
| 58 | `BLAP` | double → DoubleType | Parquet |
| 59 | `BLAM` | double → DoubleType | Parquet |
| 60 | `BLAY` | double → DoubleType | Parquet |
| 61 | `BLHI` | double → DoubleType | Parquet |
| 62 | `BLLO` | double → DoubleType | Parquet |
| 63 | `BLVR` | double → DoubleType | Parquet |
| 64 | `BLRT` | double → DoubleType | Parquet |
| 65 | `BLCP` | double → DoubleType | Parquet |
| 66 | `BLCI` | double → DoubleType | Parquet |
| 67 | `BLOD` | date32[day] → DateType | Parquet |
| 68 | `BLXD` | date32[day] → DateType | Parquet |
| 69 | `BLUD` | date32[day] → DateType | Parquet |
| 70 | `BLLD` | date32[day] → DateType | Parquet |
| 71 | `BLSD` | date32[day] → DateType | Parquet |
| 72 | `BLPD2` | date32[day] → DateType | Parquet |
| 73 | `BLRD` | date32[day] → DateType | Parquet |
| 74 | `BLMD` | date32[day] → DateType | Parquet |
| 75 | `BLCD2` | date32[day] → DateType | Parquet |
| 76 | `BLBD` | date32[day] → DateType | Parquet |
| 77 | `BLFD` | date32[day] → DateType | Parquet |
| 78 | `BLGD` | date32[day] → DateType | Parquet |
| 79 | `BLHD` | date32[day] → DateType | Parquet |
| 80 | `BLID` | date32[day] → DateType | Parquet |
| 81 | `BLJD` | date32[day] → DateType | Parquet |
| 82 | `BLKD` | date32[day] → DateType | Parquet |
| 83 | `BLND` | date32[day] → DateType | Parquet |
| 84 | `BLTD` | date32[day] → DateType | Parquet |
| 85 | `BLVD` | date32[day] → DateType | Parquet |
| 86 | `BLWD` | date32[day] → DateType | Parquet |
| 87 | `BLYD` | date32[day] → DateType | Parquet |
| 88 | `BLZD` | date32[day] → DateType | Parquet |
| 89 | `BLED` | date32[day] → DateType | Parquet |
| 90 | `BLAD2` | date32[day] → DateType | Parquet |
| 91 | `BLDD` | date32[day] → DateType | Parquet |
| 92 | `BLFP` | date32[day] → DateType | Parquet |
| 93 | `BLLP` | date32[day] → DateType | Parquet |
| 94 | `BLMP` | date32[day] → DateType | Parquet |
| 95 | `BLNP` | date32[day] → DateType | Parquet |
| 96 | `BLOP` | date32[day] → DateType | Parquet |
| 97 | `BLPP` | date32[day] → DateType | Parquet |
| 98 | `BLQP` | date32[day] → DateType | Parquet |
| 99 | `BLRP` | date32[day] → DateType | Parquet |
| 100 | `BLSP2` | date32[day] → DateType | Parquet |
| 101 | `BLTP2` | date32[day] → DateType | Parquet |
| 102 | `_rescued_data` | StringType | **Generada**: AutoLoader (esquema evolutivo) |
| — | `año` | string → StringType | Partición (inferida por AutoLoader) |
| — | `mes` | string → StringType | Partición (inferida por AutoLoader) |
| — | `dia` | string → StringType | Partición (inferida por AutoLoader) |

#### BLNCFL — Vista Materializada / Snapshot Más Reciente

Mismo esquema que `BLNCFL_temp` (102 columnas). Filtrado por `FechaRegistroParquet = MAX(FechaRegistroParquet)`. Se registra en Unity Catalog como `{catalogo}.{esquema}.BLNCFL`.

**Nombre LSDP**: `@dp.materialized_view(name=f"{catalogo}.{esquema}.BLNCFL", cluster_by=["FechaRegistroParquet"])`

---

#### Resumen de Tablas de Bronce

| Tabla | Tipo LSDP | Columnas Parquet | + Generadas | Total | Registrada en UC |
|-------|-----------|-----------------|-------------|-------|-----------------|
| `CMSTFL_temp` | Streaming Table (`temporary=True`) | 70 | +2 (`FechaRegistroParquet`, `_rescued_data`) | 72 | No |
| `CMSTFL` | Materialized View | 70 | +2 (`FechaRegistroParquet`, `_rescued_data`) | 72 | Sí |
| `TRXPFL_temp` | Streaming Table (`temporary=True`) | 60 | +2 (`FechaRegistroParquet`, `_rescued_data`) | 62 | No |
| `TRXPFL` | Materialized View | 60 | +2 (`FechaRegistroParquet`, `_rescued_data`) | 62 | Sí |
| `BLNCFL_temp` | Streaming Table (`temporary=True`) | 100 | +2 (`FechaRegistroParquet`, `_rescued_data`) | 102 | No |
| `BLNCFL` | Materialized View | 100 | +2 (`FechaRegistroParquet`, `_rescued_data`) | 102 | Sí |

> **Nota**: Las columnas `año`, `mes`, `dia`, `FechaRegistroParquet` y `_rescued_data` son exclusivas de Bronce. Las capas de Plata y Oro **no** las incluyen en sus esquemas.

---


## Modelo Data Vault 2.0 — Esquemas Detallados (Aprobado)

> **Convención de nombres**: Todos los campos/columnas del modelo de Plata usan nombres en español, claros e intuitivos. Los códigos AS400 se documentan como referencia de mapeo.

### Resumen del Modelo Propuesto

| Tipo | Cantidad | Tablas |
|------|----------|--------|
| **Hubs** | 3 | Hub_Cliente, Hub_Operacion, Hub_Transaccion |
| **Links** | 2 | Link_Cliente_Operacion, Link_Cliente_Transaccion |
| **Satellites** | 10 | 4 de Cliente, 3 de Operación, 3 de Transacción |
| **Total** | **15** | — |

> **Nota**: Se eliminó `Link_Operacion_Transaccion` por decisión del usuario: las transacciones las realizan los clientes, no tienen relación directa con las operaciones (saldos). La relación Operación↔Transacción se resuelve transitivamente a través del Cliente.

### Tablas Hub Propuestas

#### Hub_Cliente
- **Llave de negocio**: `IdentificadorCliente` (origen: CUSTID de CMSTFL)
- **Fuente**: CMSTFL (Bronce)

| Columna | Tipo de Dato | Origen AS400 | Descripción |
|---------|-------------|-------------|-------------|
| `FechaRegistro` | TimestampType | Generado | `current_timestamp()` |
| `Hash_Cliente` | StringType | Calculado | `SHA2(CAST(IdentificadorCliente AS STRING), 256)` |
| `IdentificadorCliente` | LongType | CUSTID | Identificador único del cliente (llave de negocio) |
| `FuenteDatos` | StringType | Generado | Nombre completo de la tabla fuente |

**Liquid Clustering**: `FechaRegistro`, `Hash_Cliente`

**Ejemplo de código LSDP**:

```python
@dp.materialized_view(
    name=f"{catalogo_plata}.{esquema_plata}.Hub_Cliente",
    cluster_by=["FechaRegistro", "Hash_Cliente"]
)
@dp.expect_all_or_fail({
    "id_cliente_no_nulo": "IdentificadorCliente IS NOT NULL",
    "id_cliente_positivo": "IdentificadorCliente > 0",
    "hash_cliente_no_nulo": "Hash_Cliente IS NOT NULL"
})
def hub_cliente():
    snap = spark.read.table(f"{catalogo}.{esquema}.CMSTFL")
    return (
        snap.select(
            F.current_timestamp().alias("FechaRegistro"),
            F.sha2(F.col("CUSTID").cast("string"), HASH_HUB_LINK_BITS).alias("Hash_Cliente"),
            F.col("CUSTID").alias("IdentificadorCliente"),
            F.lit(f"{catalogo}.{esquema}.CMSTFL").alias("FuenteDatos")
        )
        .dropDuplicates(["IdentificadorCliente"])
    )
```

#### Hub_Operacion
- **Llave de negocio compuesta**: `IdentificadorCliente` + `SecuenciaSaldo` (origen: CUSTID + BLSQ de BLNCFL)
- **Fuente**: BLNCFL (Bronce)
- **Justificación**: Confirmado en datos reales: (CUSTID, BLSQ) es 100% única.

| Columna | Tipo de Dato | Origen AS400 | Descripción |
|---------|-------------|-------------|-------------|
| `FechaRegistro` | TimestampType | Generado | `current_timestamp()` |
| `Hash_Operacion` | StringType | Calculado | `SHA2(CONCAT_WS('\|', CAST(IdentificadorCliente), CAST(SecuenciaSaldo)), 256)` |
| `IdentificadorCliente` | LongType | CUSTID | Identificador del cliente |
| `SecuenciaSaldo` | LongType | BLSQ | Secuencia del registro de saldos |
| `FuenteDatos` | StringType | Generado | Nombre completo de la tabla fuente |

**Liquid Clustering**: `FechaRegistro`, `Hash_Operacion`

#### Hub_Transaccion
- **Llave de negocio**: `IdentificadorTransaccion` (origen: TRXID de TRXPFL, StringType nativo)
- **Fuente**: TRXPFL (Bronce)
- **Justificación**: Confirmado: TRXID es 100% único en ambos snapshots. Al ser StringType nativo, no requiere cast previo para el hash.

| Columna | Tipo de Dato | Origen AS400 | Descripción |
|---------|-------------|-------------|-------------|
| `FechaRegistro` | TimestampType | Generado | `current_timestamp()` |
| `Hash_Transaccion` | StringType | Calculado | `SHA2(IdentificadorTransaccion, 256)` — ya es string |
| `IdentificadorTransaccion` | StringType | TRXID | Identificador único de la transacción (llave de negocio) |
| `FuenteDatos` | StringType | Generado | Nombre completo de la tabla fuente |

**Liquid Clustering**: `FechaRegistro`, `Hash_Transaccion`

### Tablas Link Propuestas

#### Link_Cliente_Operacion
- **Relación**: Hub_Cliente ↔ Hub_Operacion
- **Fuente**: BLNCFL (contiene `CUSTID` que conecta con Hub_Cliente)

| Columna | Tipo de Dato | Descripción |
|---------|-------------|-------------|
| `FechaRegistro` | TimestampType | `current_timestamp()` |
| `Hash_Cliente` | StringType | Hash del Hub_Cliente |
| `Hash_Operacion` | StringType | Hash del Hub_Operacion |
| `Hash_Link_Cliente_Operacion` | StringType | `SHA2(CONCAT_WS('\|', Hash_Cliente, Hash_Operacion), 256)` |
| `FuenteDatos` | StringType | Nombre completo de la tabla fuente |

**Liquid Clustering**: `FechaRegistro`, `Hash_Cliente`, `Hash_Operacion`

**Cobertura**: 100% de clientes tienen registro en BLNCFL. Este Link contendrá los 75,450 registros completos.

#### Link_Cliente_Transaccion
- **Relación**: Hub_Cliente ↔ Hub_Transaccion
- **Fuente**: TRXPFL (contiene `CUSTID`)

| Columna | Tipo de Dato | Descripción |
|---------|-------------|-------------|
| `FechaRegistro` | TimestampType | `current_timestamp()` |
| `Hash_Cliente` | StringType | Hash del Hub_Cliente |
| `Hash_Transaccion` | StringType | Hash del Hub_Transaccion |
| `Hash_Link_Cliente_Transaccion` | StringType | `SHA2(CONCAT_WS('\|', Hash_Cliente, Hash_Transaccion), 256)` |
| `FuenteDatos` | StringType | Nombre completo de la tabla fuente |

**Liquid Clustering**: `FechaRegistro`, `Hash_Cliente`, `Hash_Transaccion`

**Cobertura**: 94.3% de clientes tienen transacciones. 5.7% de clientes no generarán registros aquí.

### Tablas Satellite Propuestas

#### Satellites de Hub_Cliente (4 Satellites)

La separación se basa en los resultados REALES del análisis de variación entre snapshots:

**Sat_Cliente_DatosEstables** — Atributos de identidad que nunca cambian (0% variación observada):

| Columna | Tipo de Dato | Origen | Campo AS400 |
|---------|-------------|--------|-------------|
| `Hash_Cliente` | StringType | Hub_Cliente | — |
| `sexo_cliente` | StringType | CMSTFL | CUSSX |
| `tratamiento_cliente` | StringType | CMSTFL | CUSTT |
| `fecha_nacimiento` | DateType | CMSTFL | CUSDB |
| `anio_nacimiento` | LongType | CMSTFL | CUSYR |
| `edad_cliente` | LongType | CMSTFL | CUSAG2 |
| `pais_residencia` | StringType | CMSTFL | CUSCN |
| `nacionalidad_cliente` | StringType | CMSTFL | CUSNA |
| `numero_licencia_conducir` | StringType | CMSTFL | CUSDL |
| `tipo_documento_pasaporte` | StringType | CMSTFL | CUSDP |
| `cantidad_pasaportes` | LongType | CMSTFL | CUSDP2 |
| `idioma_preferido` | StringType | CMSTFL | CUSLG |
| **`RangoEtario`** | StringType | **Calculado** | — |
| **`CategoriaIngresos`** | StringType | **Calculado** | — |
| `Hash_Diferenciador` | StringType | SHA2-512 | — |
| `FechaRegistro` | TimestampType | current_timestamp() | — |
| `FuenteDatos` | StringType | Generado | — |

**Sat_Cliente_Contacto** — Datos de contacto y personales que SÍ cambian (~15–20% variación observada):

| Columna | Tipo de Dato | Origen | Campo AS400 | % Cambio Observado |
|---------|-------------|--------|-------------|-------------------|
| `Hash_Cliente` | StringType | Hub_Cliente | — | — |
| `nombre_cliente` | StringType | CMSTFL | CUSNM | 19.91% |
| `apellido_cliente` | StringType | CMSTFL | CUSLN | 19.91% |
| `nombre_medio_cliente` | StringType | CMSTFL | CUSMD | 19.93% |
| `nombre_completo_cliente` | StringType | CMSTFL | CUSFN | 20.00% |
| `estado_civil` | StringType | CMSTFL | CUSMS | 14.98% |
| `ocupacion_cliente` | StringType | CMSTFL | CUSOC | 18.59% |
| `nivel_educativo` | StringType | CMSTFL | CUSED | 15.97% |
| `direccion_calle` | StringType | CMSTFL | CUSAD | 20.00% |
| `direccion_apartamento` | StringType | CMSTFL | CUSA2 | 20.00% |
| `ciudad_residencia` | StringType | CMSTFL | CUSCT | 18.73% |
| `estado_provincia` | StringType | CMSTFL | CUSST | 18.04% |
| `codigo_postal` | StringType | CMSTFL | CUSZP | 20.00% |
| `telefono_principal` | StringType | CMSTFL | CUSPH | 20.00% |
| `telefono_movil` | StringType | CMSTFL | CUSMB | 20.00% |
| `correo_electronico` | StringType | CMSTFL | CUSEM | 20.00% |
| `Hash_Diferenciador` | StringType | SHA2-512 | — | — |
| `FechaRegistro` | TimestampType | current_timestamp() | — | — |
| `FuenteDatos` | StringType | Generado | — | — |

**Sat_Cliente_Clasificacion** — Variables de clasificación y segmentación bancaria (0% variación pero potencialmente variables en producción):

| Columna | Tipo de Dato | Origen | Campo AS400 |
|---------|-------------|--------|-------------|
| `Hash_Cliente` | StringType | Hub_Cliente | — |
| `tipo_cliente` | StringType | CMSTFL | CUSTP |
| `segmento_cliente` | StringType | CMSTFL | CUSSG |
| `region_geografica` | StringType | CMSTFL | CUSRG |
| `sucursal_principal` | StringType | CMSTFL | CUSBR |
| `gerente_asignado` | StringType | CMSTFL | CUSMG |
| `referencia_interna` | StringType | CMSTFL | CUSRF |
| `fuente_referencia` | StringType | CMSTFL | CUSRS |
| `grupo_afinidad` | StringType | CMSTFL | CUSAG |
| `preferencia_comunicacion` | StringType | CMSTFL | CUSPC |
| `nivel_riesgo` | StringType | CMSTFL | CUSRK |
| `indicador_vip` | StringType | CMSTFL | CUSVP |
| `estado_perfil` | StringType | CMSTFL | CUSPF |
| `estado_kyc` | StringType | CMSTFL | CUSKT |
| `indicador_flags` | StringType | CMSTFL | CUSFM |
| `ultimo_canal` | StringType | CMSTFL | CUSLC |
| `calificacion_crediticia` | StringType | CMSTFL | CUSCR |
| `cuenta_activa` | StringType | CMSTFL | CUSAC |
| `clasificacion_interna` | StringType | CMSTFL | CUSCL |
| `nota_cliente` | StringType | CMSTFL | CUSNT |
| `Hash_Diferenciador` | StringType | SHA2-512 | — |
| `FechaRegistro` | TimestampType | current_timestamp() | — |
| `FuenteDatos` | StringType | Generado | — |

**Sat_Cliente_Financiero** — Valores numéricos y todas las fechas de evento (0% variación observada):

| Columna | Tipo de Dato | Origen | Campo AS400 |
|---------|-------------|--------|-------------|
| `Hash_Cliente` | StringType | Hub_Cliente | — |
| `cantidad_cuentas` | LongType | CMSTFL | CUSAC2 |
| `cantidad_transacciones` | LongType | CMSTFL | CUSTX |
| `score_cliente` | LongType | CMSTFL | CUSSC |
| `ranking_prestamos` | LongType | CMSTFL | CUSLR |
| `cantidad_registros` | LongType | CMSTFL | CUSRC |
| `ingresos_cliente` | DoubleType | CMSTFL | CUSIN |
| `saldo_disponible_maestro` | DoubleType | CMSTFL | CUSBL |
| `fecha_apertura_relacion` | DateType | CMSTFL | CUSOD |
| `fecha_cierre_relacion` | DateType | CMSTFL | CUSCD |
| `fecha_ultima_visita` | DateType | CMSTFL | CUSLV |
| `fecha_ultima_actualizacion` | DateType | CMSTFL | CUSUD |
| `fecha_verificacion_kyc` | DateType | CMSTFL | CUSKD |
| `fecha_renovacion` | DateType | CMSTFL | CUSRD |
| `fecha_expiracion` | DateType | CMSTFL | CUSXD |
| `fecha_primer_producto` | DateType | CMSTFL | CUSFD |
| `fecha_ultimo_producto` | DateType | CMSTFL | CUSLD |
| `fecha_migracion` | DateType | CMSTFL | CUSMD2 |
| `fecha_activacion` | DateType | CMSTFL | CUSAD2 |
| `fecha_bloqueo` | DateType | CMSTFL | CUSBD |
| `fecha_verificacion` | DateType | CMSTFL | CUSVD |
| `fecha_promocion` | DateType | CMSTFL | CUSPD |
| `fecha_desactivacion` | DateType | CMSTFL | CUSDD |
| `fecha_educacion_financiera` | DateType | CMSTFL | CUSED2 |
| `fecha_notificacion` | DateType | CMSTFL | CUSND |
| `Hash_Diferenciador` | StringType | SHA2-512 | — |
| `FechaRegistro` | TimestampType | current_timestamp() | — |
| `FuenteDatos` | StringType | Generado | — |

#### Satellites de Hub_Operacion (3 Satellites)

Se separan por naturaleza funcional. Aunque BLNCFL muestra 0% de variación entre los 2 snapshots analizados, los montos/saldos son por naturaleza los que más cambian en producción bancaria.

**Sat_Operacion_DatosEstables** — Atributos cualitativos de la cuenta (31 campos string):

| Columna | Tipo de Dato | Origen | Campo AS400 |
|---------|-------------|--------|-------------|
| `Hash_Operacion` | StringType | Hub_Operacion | — |
| `tipo_cuenta` | StringType | BLNCFL | BLACT |
| `numero_cuenta` | StringType | BLNCFL | BLACN |
| `moneda_cuenta` | StringType | BLNCFL | BLCUR |
| `estado_cuenta` | StringType | BLNCFL | BLST |
| `sucursal_cuenta` | StringType | BLNCFL | BLBR |
| `producto_cuenta` | StringType | BLNCFL | BLPR |
| `subproducto_cuenta` | StringType | BLNCFL | BLSP |
| `nombre_cuenta` | StringType | BLNCFL | BLNM |
| `clase_cuenta` | StringType | BLNCFL | BLCL |
| `riesgo_cuenta` | StringType | BLNCFL | BLRK |
| `tipo_producto_cuenta` | StringType | BLNCFL | BLTP |
| `gerente_cuenta` | StringType | BLNCFL | BLMG |
| `referencia_cuenta` | StringType | BLNCFL | BLRF |
| `centro_costos_cuenta` | StringType | BLNCFL | BLCC |
| `grupo_afinidad_cuenta` | StringType | BLNCFL | BLAG |
| `plan_cuenta` | StringType | BLNCFL | BLPL |
| `region_cuenta` | StringType | BLNCFL | BLRG |
| `sufijo_cuenta` | StringType | BLNCFL | BLSF |
| `nota_cuenta` | StringType | BLNCFL | BLNT |
| `ultimo_canal_cuenta` | StringType | BLNCFL | BLLC |
| `perfil_cuenta` | StringType | BLNCFL | BLPF |
| `autorizado_cuenta` | StringType | BLNCFL | BLAU |
| `texto_cuenta` | StringType | BLNCFL | BLTX |
| `grupo_cuenta` | StringType | BLNCFL | BLGR |
| `email_cuenta` | StringType | BLNCFL | BLEM |
| `frecuencia_cuenta` | StringType | BLNCFL | BLFR |
| `clave_cuenta` | StringType | BLNCFL | BLKY |
| `vip_cuenta` | StringType | BLNCFL | BLVP |
| `factor_cuenta` | StringType | BLNCFL | BLFC |
| **`CategoriaSaldo`** | StringType | **Calculado** | — |
| **`EstadoUtilizacionCredito`** | StringType | **Calculado** | — |
| **`IndicadorSobregiro`** | StringType | **Calculado** | — |
| `Hash_Diferenciador` | StringType | SHA2-512 | — |
| `FechaRegistro` | TimestampType | current_timestamp() | — |
| `FuenteDatos` | StringType | Generado | — |

**Sat_Operacion_Montos** — Variables monetarias y ratios financieros (34 campos double):

| Columna | Tipo de Dato | Origen | Campo AS400 |
|---------|-------------|--------|-------------|
| `Hash_Operacion` | StringType | Hub_Operacion | — |
| `saldo_disponible` | DoubleType | BLNCFL | BLAV |
| `saldo_total` | DoubleType | BLNCFL | BLTB |
| `saldo_reservado` | DoubleType | BLNCFL | BLRV |
| `saldo_bloqueado` | DoubleType | BLNCFL | BLBK |
| `saldo_minimo` | DoubleType | BLNCFL | BLMN |
| `saldo_maximo` | DoubleType | BLNCFL | BLMX |
| `tasa_interes` | DoubleType | BLNCFL | BLIR |
| `multiplicador_penalidad` | DoubleType | BLNCFL | BLPM |
| `limite_credito` | DoubleType | BLNCFL | BLCR |
| `credito_utilizado` | DoubleType | BLNCFL | BLCU |
| `credito_disponible` | DoubleType | BLNCFL | BLCD |
| `valor_sobregiro` | DoubleType | BLNCFL | BLOV |
| `limite_sobregiro` | DoubleType | BLNCFL | BLOL |
| `depositos_pendientes` | DoubleType | BLNCFL | BLPD |
| `cargos_pendientes` | DoubleType | BLNCFL | BLPC |
| `ajustes_pendientes` | DoubleType | BLNCFL | BLPA |
| `depositos_ingreso` | DoubleType | BLNCFL | BLDI |
| `retenciones_cuenta` | DoubleType | BLNCFL | BLWI |
| `transferencias_ingreso` | DoubleType | BLNCFL | BLTI |
| `cargos_transferencia` | DoubleType | BLNCFL | BLTC |
| `comisiones_anuales` | DoubleType | BLNCFL | BLCA |
| `intereses_mensuales` | DoubleType | BLNCFL | BLIM |
| `reembolsos_cuenta` | DoubleType | BLNCFL | BLRF2 |
| `penalidades_cuenta` | DoubleType | BLNCFL | BLPN |
| `bonificaciones_cuenta` | DoubleType | BLNCFL | BLBN |
| `ajustes_positivos` | DoubleType | BLNCFL | BLAP |
| `ajustes_miscelaneos` | DoubleType | BLNCFL | BLAM |
| `ajustes_anuales` | DoubleType | BLNCFL | BLAY |
| `marca_alta_saldo` | DoubleType | BLNCFL | BLHI |
| `marca_baja_saldo` | DoubleType | BLNCFL | BLLO |
| `varianza_saldo` | DoubleType | BLNCFL | BLVR |
| `ratio_cuenta` | DoubleType | BLNCFL | BLRT |
| `porcentaje_aporte` | DoubleType | BLNCFL | BLCP |
| `ingresos_aporte` | DoubleType | BLNCFL | BLCI |
| `Hash_Diferenciador` | StringType | SHA2-512 | — |
| `FechaRegistro` | TimestampType | current_timestamp() | — |
| `FuenteDatos` | StringType | Generado | — |

**Sat_Operacion_FechasEvento** — Todas las 34 fechas de eventos de la cuenta:

| Columna | Tipo de Dato | Origen | Campo AS400 |
|---------|-------------|--------|-------------|
| `Hash_Operacion` | StringType | Hub_Operacion | — |
| `fecha_apertura_cuenta` | DateType | BLNCFL | BLOD |
| `fecha_expiracion_cuenta` | DateType | BLNCFL | BLXD |
| `fecha_actualizacion_cuenta` | DateType | BLNCFL | BLUD |
| `fecha_ultimo_movimiento` | DateType | BLNCFL | BLLD |
| `fecha_estado_cuenta` | DateType | BLNCFL | BLSD |
| `fecha_penalidad` | DateType | BLNCFL | BLPD2 |
| `fecha_renovacion_cuenta` | DateType | BLNCFL | BLRD |
| `fecha_maduracion` | DateType | BLNCFL | BLMD |
| `fecha_cierre_cuenta` | DateType | BLNCFL | BLCD2 |
| `fecha_bloqueo_cuenta` | DateType | BLNCFL | BLBD |
| `fecha_fondeo` | DateType | BLNCFL | BLFD |
| `fecha_gracia` | DateType | BLNCFL | BLGD |
| `fecha_historica` | DateType | BLNCFL | BLHD |
| `fecha_interes` | DateType | BLNCFL | BLID |
| `fecha_ajuste` | DateType | BLNCFL | BLJD |
| `fecha_kyc_cuenta` | DateType | BLNCFL | BLKD |
| `fecha_notificacion_cuenta` | DateType | BLNCFL | BLND |
| `fecha_transferencia` | DateType | BLNCFL | BLTD |
| `fecha_verificacion_cuenta` | DateType | BLNCFL | BLVD |
| `fecha_retiro` | DateType | BLNCFL | BLWD |
| `fecha_rendimiento` | DateType | BLNCFL | BLYD |
| `fecha_cierre_periodo` | DateType | BLNCFL | BLZD |
| `fecha_evaluacion` | DateType | BLNCFL | BLED |
| `fecha_activacion_cuenta` | DateType | BLNCFL | BLAD2 |
| `fecha_desactivacion_cuenta` | DateType | BLNCFL | BLDD |
| `fecha_primer_pago` | DateType | BLNCFL | BLFP |
| `fecha_ultimo_pago` | DateType | BLNCFL | BLLP |
| `fecha_mora_pago` | DateType | BLNCFL | BLMP |
| `fecha_proximo_pago` | DateType | BLNCFL | BLNP |
| `fecha_origen_pago` | DateType | BLNCFL | BLOP |
| `fecha_programacion_pago` | DateType | BLNCFL | BLPP |
| `fecha_quiebre_pago` | DateType | BLNCFL | BLQP |
| `fecha_regularizacion_pago` | DateType | BLNCFL | BLRP |
| `fecha_suspension_pago` | DateType | BLNCFL | BLSP2 |
| `fecha_tercero_pago` | DateType | BLNCFL | BLTP2 |
| `Hash_Diferenciador` | StringType | SHA2-512 | — |
| `FechaRegistro` | TimestampType | current_timestamp() | — |
| `FuenteDatos` | StringType | Generado | — |

#### Satellites de Hub_Transaccion (2 Satellites)

La separación se basa en la variación observada: 29 campos con 0% cambio vs. 30 campos con 87.5%+ cambio.

**Sat_Transaccion_DatosEstables** — Atributos categóricos, secuencia, fechas auxiliares y timestamps (0% variación):

| Columna | Tipo de Dato | Origen | Campo AS400 | % Cambio |
|---------|-------------|--------|-------------|----------|
| `Hash_Transaccion` | StringType | Hub_Transaccion | — | — |
| `tipo_transaccion` | StringType | TRXPFL | TRXTYP | 0% |
| `moneda_transaccion` | StringType | TRXPFL | TRXCUR | 0% |
| `estado_transaccion` | StringType | TRXPFL | TRXST | 0% |
| `canal_transaccion` | StringType | TRXPFL | TRXCH | 0% |
| `descripcion_transaccion` | StringType | TRXPFL | TRXDSC | 0% |
| `referencia_externa` | StringType | TRXPFL | TRXREF | 0% |
| `secuencia_transaccion` | LongType | TRXPFL | TRXSQ | 0% |
| `monto_maximo` | DoubleType | TRXPFL | TRXMX | 0% |
| `monto_minimo` | DoubleType | TRXPFL | TRXMN | 0% |
| `fecha_valor` | DateType | TRXPFL | TRXVD | 0% |
| `fecha_procesamiento` | DateType | TRXPFL | TRXPD | 0% |
| `fecha_liquidacion` | DateType | TRXPFL | TRXSD | 0% |
| `fecha_compensacion` | DateType | TRXPFL | TRXCD | 0% |
| `fecha_efectiva` | DateType | TRXPFL | TRXED | 0% |
| `fecha_reverso` | DateType | TRXPFL | TRXRD | 0% |
| `fecha_autorizacion` | DateType | TRXPFL | TRXAD | 0% |
| `fecha_notificacion_trx` | DateType | TRXPFL | TRXND | 0% |
| `fecha_expiracion_trx` | DateType | TRXPFL | TRXXD | 0% |
| `fecha_fondeo_trx` | DateType | TRXPFL | TRXFD | 0% |
| `fecha_gracia_trx` | DateType | TRXPFL | TRXGD | 0% |
| `fecha_historica_trx` | DateType | TRXPFL | TRXHD | 0% |
| `fecha_bloqueo_trx` | DateType | TRXPFL | TRXBD | 0% |
| `fecha_maduracion_trx` | DateType | TRXPFL | TRXMD | 0% |
| `fecha_limite_trx` | DateType | TRXPFL | TRXLD | 0% |
| `fecha_actualizacion_trx` | DateType | TRXPFL | TRXUD | 0% |
| `fecha_origen_trx` | DateType | TRXPFL | TRXOD | 0% |
| `fecha_kyc_trx` | DateType | TRXPFL | TRXKD | 0% |
| `timestamp_transaccion` | TimestampType | TRXPFL | TRXTS | 0% |
| `timestamp_actualizacion` | TimestampType | TRXPFL | TRXUS | 0% |
| **`ClasificacionCanalATM`** | StringType | **Calculado** | — | — |
| `Hash_Diferenciador` | StringType | SHA2-512 | — | — |
| `FechaRegistro` | TimestampType | current_timestamp() | — | — |
| `FuenteDatos` | StringType | Generado | — | — |

**Sat_Transaccion_Montos** — Variables monetarias y de riesgo (87.5%+ variación):

| Columna | Tipo de Dato | Origen | Campo AS400 | % Cambio |
|---------|-------------|--------|-------------|----------|
| `Hash_Transaccion` | StringType | Hub_Transaccion | — | — |
| `identificador_cliente` | LongType | TRXPFL | CUSTID | 99.8% |
| `fecha_transaccion` | DateType | TRXPFL | TRXDT | 100% |
| `monto_principal` | DoubleType | TRXPFL | TRXAMT | 87.5% |
| `comision_transaccion` | DoubleType | TRXPFL | TRXCM | 87.5% |
| `saldo_posterior` | DoubleType | TRXPFL | TRXBA | 87.5% |
| `saldo_anterior` | DoubleType | TRXPFL | TRXBP | 87.5% |
| `cargo_fiscal` | DoubleType | TRXPFL | TRXTC | 87.5% |
| `monto_local` | DoubleType | TRXPFL | TRXAL | 87.5% |
| `monto_pago` | DoubleType | TRXPFL | TRXPN | 87.5% |
| `beneficio_transaccion` | DoubleType | TRXPFL | TRXBF | 87.5% |
| `perdida_tasa` | DoubleType | TRXPFL | TRXRL | 87.5% |
| `monto_promedio` | DoubleType | TRXPFL | TRXAV | 87.5% |
| `desviacion_monto` | DoubleType | TRXPFL | TRXDV | 87.5% |
| `riesgo_transaccion` | DoubleType | TRXPFL | TRXRK | 87.5% |
| `riesgo_fraude` | DoubleType | TRXPFL | TRXFR | 87.5% |
| `limite_transaccion` | DoubleType | TRXPFL | TRXLM | 87.5% |
| `porcentaje_limite` | DoubleType | TRXPFL | TRXLP | 87.5% |
| `cargo_plataforma` | DoubleType | TRXPFL | TRXCP | 87.5% |
| `cargo_institucion` | DoubleType | TRXPFL | TRXCI | 87.5% |
| `cargo_extranjero` | DoubleType | TRXPFL | TRXCF | 87.5% |
| `cargo_varianza` | DoubleType | TRXPFL | TRXCV | 87.5% |
| `subtotal_transaccion` | DoubleType | TRXPFL | TRXSB | 87.5% |
| `total_transaccion` | DoubleType | TRXPFL | TRXTL | 87.5% |
| `residuo_transaccion` | DoubleType | TRXPFL | TRXRS | 87.5% |
| `margen_interes` | DoubleType | TRXPFL | TRXIM | 87.5% |
| `monto_neto` | DoubleType | TRXPFL | TRXNT | 87.5% |
| `monto_original` | DoubleType | TRXPFL | TRXAO | 87.5% |
| `monto_inversion` | DoubleType | TRXPFL | TRXIN | 87.5% |
| `descuento_transaccion` | DoubleType | TRXPFL | TRXDS | 87.5% |
| `monto_principal_prestamo` | DoubleType | TRXPFL | TRXPT | 87.5% |
| **`RangoMontoTransaccion`** | StringType | **Calculado** | — | — |
| **`NivelRiesgoFraude`** | StringType | **Calculado** | — | — |
| `Hash_Diferenciador` | StringType | SHA2-512 | — | — |
| `FechaRegistro` | TimestampType | current_timestamp() | — | — |
| `FuenteDatos` | StringType | Generado | — | — |

---


## Modelo Estrella — Esquemas Detallados (Aprobado)

> **Convención**: Las dimensiones de Oro **no almacenan columnas Hash**. Los Hashes se usan en los procesamientos y queries internos de Plata pero no se persisten en las dimensiones. Cada dimensión tiene un **DimId** (llave subrogada numérica persistente) generado a partir de la llave de negocio.

### Patrón de Llave Subrogada Persistente (DimId)

El DimId se asigna **una sola vez** y nunca cambia para un registro existente. Los nuevos registros reciben el siguiente número consecutivo disponible (`MAX(DimId) + ROW_NUMBER()`). Las dimensiones son **SCD Tipo 1** (sobrescritura de atributos, sin historial dimensional).

**Ejemplo del comportamiento esperado**:

| Día | IdentificadorCliente | DimIdCliente | Acción |
|-----|---------------------|-------------|--------|
| 2026-04-03 | 1000 | 1 | Nuevo |
| 2026-04-03 | 1002 | 2 | Nuevo |
| 2026-04-03 | 1004 | 3 | Nuevo |
| 2026-04-04 | 1000 | **1** | Se mantiene |
| 2026-04-04 | 1001 | **4** | Nuevo (MAX previo=3, recibe 3+1=4) |
| 2026-04-04 | 1002 | **2** | Se mantiene |
| 2026-04-04 | 1004 | **3** | Se mantiene |

### Dim_Cliente (SCD Tipo 1 — Sobrescritura con DimId persistente)

Se construye a partir de Hub_Cliente + Satellites de Cliente (registros más recientes). Los Hashes de Plata se usan para los JOINs pero **no se almacenan** en la dimensión.

| Columna | Tipo de Dato | Fuente | Descripción |
|---------|-------------|--------|-------------|
| `DimIdCliente` | LongType | Generado | **PK — Llave subrogada persistente** |
| `IdentificadorCliente` | LongType | Hub_Cliente | Llave de negocio original |
| `NombreCompletoCliente` | StringType | Sat_Cliente_Contacto | Nombre completo |
| `SexoCliente` | StringType | Sat_Cliente_DatosEstables | Sexo (M/F) |
| `EdadCliente` | LongType | Sat_Cliente_DatosEstables | Edad |
| `RangoEtario` | StringType | Sat_Cliente_DatosEstables | Rango calculado |
| `EstadoCivil` | StringType | Sat_Cliente_Contacto | Estado civil |
| `PaisResidencia` | StringType | Sat_Cliente_DatosEstables | País |
| `CiudadResidencia` | StringType | Sat_Cliente_Contacto | Ciudad |
| `TipoCliente` | StringType | Sat_Cliente_Clasificacion | Tipo (IND/COR) |
| `SegmentoCliente` | StringType | Sat_Cliente_Clasificacion | Segmento (PREM/STD/BAS) |
| `NivelRiesgo` | StringType | Sat_Cliente_Clasificacion | Nivel riesgo |
| `IndicadorVip` | StringType | Sat_Cliente_Clasificacion | VIP (S/N) |
| `CalificacionCrediticia` | StringType | Sat_Cliente_Clasificacion | Calificación |
| `ScoreCliente` | LongType | Sat_Cliente_Financiero | Score crediticio |
| `CategoriaIngresos` | StringType | Sat_Cliente_DatosEstables | Categoría calculada |
| `IngresosCliente` | DoubleType | Sat_Cliente_Financiero | Ingresos mensuales |
| `CorreoElectronico` | StringType | Sat_Cliente_Contacto | Email |

**Liquid Clustering**: `DimIdCliente`

**Ejemplo de código LSDP**:

```python
@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.Dim_Cliente",
    cluster_by=["DimIdCliente"]
)
@dp.expect_all_or_fail({
    "dim_id_cliente_no_nulo": "DimIdCliente IS NOT NULL",
    "id_cliente_no_nulo": "IdentificadorCliente IS NOT NULL"
})
def dim_cliente():
    from pyspark.sql.window import Window

    hub = spark.read.table(f"{catalogo_plata}.{esquema_plata}.Hub_Cliente")

    # Último registro de cada Satellite (por Hash_Cliente, más reciente)
    def ultimo_registro(tabla):
        w = Window.partitionBy("Hash_Cliente").orderBy(F.col("FechaRegistro").desc())
        return (
            spark.read.table(f"{catalogo_plata}.{esquema_plata}.{tabla}")
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn", "Hash_Diferenciador", "FechaRegistro", "FuenteDatos")
        )

    sat_estable = ultimo_registro("Sat_Cliente_DatosEstables")
    sat_contacto = ultimo_registro("Sat_Cliente_Contacto")
    sat_clasif = ultimo_registro("Sat_Cliente_Clasificacion")
    sat_fin = ultimo_registro("Sat_Cliente_Financiero")

    # Construir datos actuales (sin Hash en la salida)
    datos_actuales = (
        hub
        .join(sat_estable, "Hash_Cliente", "left")
        .join(sat_contacto, "Hash_Cliente", "left")
        .join(sat_clasif, "Hash_Cliente", "left")
        .join(sat_fin, "Hash_Cliente", "left")
        .select(
            F.col("IdentificadorCliente"),
            F.col("nombre_completo_cliente").alias("NombreCompletoCliente"),
            F.col("sexo_cliente").alias("SexoCliente"),
            F.col("edad_cliente").alias("EdadCliente"),
            F.col("RangoEtario"),
            F.col("estado_civil").alias("EstadoCivil"),
            F.col("pais_residencia").alias("PaisResidencia"),
            F.col("ciudad_residencia").alias("CiudadResidencia"),
            F.col("tipo_cliente").alias("TipoCliente"),
            F.col("segmento_cliente").alias("SegmentoCliente"),
            F.col("nivel_riesgo").alias("NivelRiesgo"),
            F.col("indicador_vip").alias("IndicadorVip"),
            F.col("calificacion_crediticia").alias("CalificacionCrediticia"),
            F.col("score_cliente").alias("ScoreCliente"),
            F.col("CategoriaIngresos"),
            F.col("ingresos_cliente").alias("IngresosCliente"),
            F.col("correo_electronico").alias("CorreoElectronico")
        )
    )

    # Llave subrogada persistente
    try:
        dim_previa = spark.read.table(f"{catalogo_oro}.{esquema_oro}.Dim_Cliente")
        max_id = dim_previa.agg(F.max("DimIdCliente")).first()[0] or 0

        # Preservar DimId existentes
        con_ids = datos_actuales.join(
            dim_previa.select("IdentificadorCliente", "DimIdCliente"),
            "IdentificadorCliente", "left"
        )

        existentes = con_ids.filter(F.col("DimIdCliente").isNotNull())

        # Nuevos registros: asignar IDs consecutivos después del máximo
        w_nuevos = Window.orderBy("IdentificadorCliente")
        nuevos = (
            con_ids.filter(F.col("DimIdCliente").isNull())
            .withColumn("DimIdCliente",
                F.lit(max_id) + F.row_number().over(w_nuevos))
        )

        return existentes.unionByName(nuevos)
    except AnalysisException:
        # Primera ejecución (tabla no existe) — asignar IDs desde 1
        w_inicial = Window.orderBy("IdentificadorCliente")
        return datos_actuales.withColumn("DimIdCliente",
            F.row_number().over(w_inicial))
```

### Dim_Operacion (SCD Tipo 1 — Sobrescritura con DimId persistente)

Se construye a partir de Hub_Operacion + Satellites de Operación (registros más recientes).

| Columna | Tipo de Dato | Fuente | Descripción |
|---------|-------------|--------|-------------|
| `DimIdOperacion` | LongType | Generado | **PK — Llave subrogada persistente** |
| `IdentificadorCliente` | LongType | Hub_Operacion | Identificador del cliente |
| `SecuenciaSaldo` | LongType | Hub_Operacion | Secuencia del saldo |
| `TipoCuenta` | StringType | Sat_Operacion_DatosEstables | Tipo (AHRO/CRTE/PRES/INVR) |
| `MonedaCuenta` | StringType | Sat_Operacion_DatosEstables | Moneda |
| `EstadoCuenta` | StringType | Sat_Operacion_DatosEstables | Estado |
| `RiesgoCuenta` | StringType | Sat_Operacion_DatosEstables | Riesgo |
| `CategoriaSaldo` | StringType | Sat_Operacion_DatosEstables | Calculado |
| `EstadoUtilizacionCredito` | StringType | Sat_Operacion_DatosEstables | Calculado |
| `IndicadorSobregiro` | StringType | Sat_Operacion_DatosEstables | Calculado |
| `SaldoDisponible` | DoubleType | Sat_Operacion_Montos | Saldo disponible |
| `SaldoTotal` | DoubleType | Sat_Operacion_Montos | Saldo total |
| `LimiteCredito` | DoubleType | Sat_Operacion_Montos | Límite crédito |
| `RatioCuenta` | DoubleType | Sat_Operacion_Montos | Ratio utilización |

**Liquid Clustering**: `DimIdOperacion`

**Patrón de DimId**: Mismo patrón que Dim_Cliente. Llave de negocio compuesta: `(IdentificadorCliente, SecuenciaSaldo)`. DimId persistente asignado por orden de primera aparición. Dado que la relación BLNCFL:CMSTFL es 1:1, `IdentificadorCliente` solo podría usarse como llave simple, pero se mantiene la compuesta con `SecuenciaSaldo` por fidelidad al modelo de Plata.

### Dim_Tiempo (Vista Materializada Incremental)

Dimensión de tiempo implementada como **Vista Materializada con refresh incremental** nativo de LSDP. Se alimenta exclusivamente de los valores distintos de `Sat_Transaccion_Montos.fecha_transaccion`. Cada vez que el pipeline detecta nuevas fechas de transacción en Plata, el motor las incorpora automáticamente sin lógica imperativa de fechas.

| Columna | Tipo de Dato | Descripción |
|---------|-------------|-------------|
| `FechaClave` | DateType | **PK — Fecha del calendario** |
| `Anio` | IntegerType | Año (YYYY) |
| `Mes` | IntegerType | Mes (1-12) |
| `Dia` | IntegerType | Día del mes (1-31) |
| `Trimestre` | IntegerType | Trimestre (1-4) |
| `Semestre` | IntegerType | Semestre (1-2) |
| `DiaSemana` | IntegerType | Día de la semana (1=Domingo, 7=Sábado, por Spark) |
| `NombreDia` | StringType | Nombre del día (Lunes, Martes, ...) |
| `NombreMes` | StringType | Nombre del mes (Enero, Febrero, ...) |
| `EsFinSemana` | BooleanType | True si es sábado o domingo (calculado en Oro) |
| `DiaDelAnio` | IntegerType | Día del año (1-366) |
| `SemanaDelAnio` | IntegerType | Semana del año (1-53) |

**Liquid Clustering**: `FechaClave`

**Restricciones de implementación**: solo operadores compatibles con incremental refresh (`select`, `distinct`, `withColumn` con funciones determinísticas, `when/otherwise`). Prohibido: `F.current_date()`, `F.current_timestamp()`, `F.now()`, `F.rand()`, UDFs, joins, Window functions.

**Ejemplo de código LSDP (Patrón MV incremental — patrón aprobado)**:

```python
validaciones_dim_tiempo = {
    "fecha_clave_no_nula": "FechaClave IS NOT NULL",
    "mes_valido": "Mes BETWEEN 1 AND 12",
}

@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.Dim_Tiempo",
    cluster_by=["FechaClave"],
    table_properties={
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days",
    }
)
@dp.expect_all_or_fail(validaciones_dim_tiempo)
@dp.expect("anio_valido", "Anio BETWEEN 1900 AND 2100")
def dim_tiempo():
    return (
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Transaccion_Montos")
        .select(F.col("fecha_transaccion").alias("FechaClave"))
        .distinct()
        .withColumn("Anio", F.year("FechaClave"))
        .withColumn("Mes", F.month("FechaClave"))
        .withColumn("Dia", F.dayofmonth("FechaClave"))
        .withColumn("Trimestre", F.quarter("FechaClave"))
        .withColumn("Semestre",
            F.when(F.quarter("FechaClave") <= 2, 1).otherwise(2))
        .withColumn("DiaSemana", F.dayofweek("FechaClave"))
        .withColumn("NombreDia",
            F.when(F.dayofweek("FechaClave") == 2, "Lunes")
             .when(F.dayofweek("FechaClave") == 3, "Martes")
             .when(F.dayofweek("FechaClave") == 4, "Miércoles")
             .when(F.dayofweek("FechaClave") == 5, "Jueves")
             .when(F.dayofweek("FechaClave") == 6, "Viernes")
             .when(F.dayofweek("FechaClave") == 7, "Sábado")
             .otherwise("Domingo"))
        .withColumn("NombreMes",
            F.when(F.month("FechaClave") == 1, "Enero")
             .when(F.month("FechaClave") == 2, "Febrero")
             .when(F.month("FechaClave") == 3, "Marzo")
             .when(F.month("FechaClave") == 4, "Abril")
             .when(F.month("FechaClave") == 5, "Mayo")
             .when(F.month("FechaClave") == 6, "Junio")
             .when(F.month("FechaClave") == 7, "Julio")
             .when(F.month("FechaClave") == 8, "Agosto")
             .when(F.month("FechaClave") == 9, "Septiembre")
             .when(F.month("FechaClave") == 10, "Octubre")
             .when(F.month("FechaClave") == 11, "Noviembre")
             .otherwise("Diciembre"))
        .withColumn("EsFinSemana",
            F.when(F.dayofweek("FechaClave").isin(1, 7), F.lit(True))
             .otherwise(F.lit(False)))
        .withColumn("DiaDelAnio", F.dayofyear("FechaClave"))
        .withColumn("SemanaDelAnio", F.weekofyear("FechaClave"))
    )
```

### Hec_Transacciones_ATM

Se construye a partir del filtrado de transacciones ATM (DATM, CATM) del Data Vault de Plata. Las FK referencian las dimensiones mediante sus **DimId** (llaves subrogadas). La relación con Dim_Operacion se resuelve **transitivamente a través del cliente** (ya que no existe Link_Operacion_Transaccion).

| Columna | Tipo de Dato | Fuente | Descripción |
|---------|-------------|--------|-------------|
| `FechaClave` | DateType | Sat_Transaccion_Montos | FK dimensión tiempo — nombre alineado con `Dim_Tiempo.FechaClave` |
| `DimIdCliente` | LongType | Dim_Cliente | FK dimensión cliente |
| `IdentificadorTransaccion` | StringType | Hub_Transaccion | PK natural de la transacción |
| `DimIdOperacion` | LongType | Dim_Operacion | FK dimensión operación (vía cliente) |
| `TipoTransaccion` | StringType | Sat_Transaccion_DatosEstables | DATM o CATM |
| `EsRetiro` | BooleanType | Calculado | `TRXTYP == 'DATM'` |
| `EsDeposito` | BooleanType | Calculado | `TRXTYP == 'CATM'` |
| `MontoPrincipal` | DoubleType | Sat_Transaccion_Montos | Monto de la operación |
| `ComisionTransaccion` | DoubleType | Sat_Transaccion_Montos | Comisión cobrada |
| `TotalTransaccion` | DoubleType | Sat_Transaccion_Montos | Monto total |
| `MonedaTransaccion` | StringType | Sat_Transaccion_DatosEstables | Moneda |
| `EstadoTransaccion` | StringType | Sat_Transaccion_DatosEstables | Estado |
| `CanalTransaccion` | StringType | Sat_Transaccion_DatosEstables | Canal |

**Liquid Clustering**: `FechaClave`, `DimIdCliente`

**Métricas derivables** (en capa de consumo/dashboards):
- Cantidad de depósitos (créditos) por cliente: `COUNT(*) WHERE EsDeposito = true`
- Cantidad de retiros (débitos) por cliente: `COUNT(*) WHERE EsRetiro = true`
- Monto promedio de depósitos por cliente: `AVG(MontoPrincipal) WHERE EsDeposito = true`
- Monto promedio de retiros por cliente: `AVG(MontoPrincipal) WHERE EsRetiro = true`
- Total de depósitos por cliente: `SUM(MontoPrincipal) WHERE EsDeposito = true`
- Total de retiros por cliente: `SUM(MontoPrincipal) WHERE EsRetiro = true`

**Ejemplo de código LSDP**:

```python
@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.Hec_Transacciones_ATM",
    cluster_by=["FechaClave", "DimIdCliente"]
)
@dp.expect_all_or_fail({
    "dim_id_cliente_no_nulo": "DimIdCliente IS NOT NULL",
    "fecha_clave_no_nula": "FechaClave IS NOT NULL",
    "tipo_transaccion_atm": "TipoTransaccion IN ('DATM', 'CATM')"
})
def hec_transacciones_atm():
    from pyspark.sql.window import Window

    # --- Datos de Plata ---
    hub_trx = spark.read.table(f"{catalogo_plata}.{esquema_plata}.Hub_Transaccion")
    link_ct = spark.read.table(f"{catalogo_plata}.{esquema_plata}.Link_Cliente_Transaccion")
    hub_cli = spark.read.table(f"{catalogo_plata}.{esquema_plata}.Hub_Cliente")

    w = Window.partitionBy("Hash_Transaccion").orderBy(F.col("FechaRegistro").desc())

    sat_estable = (
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Transaccion_DatosEstables")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1).drop("_rn")
    )

    sat_montos = (
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Transaccion_Montos")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1).drop("_rn")
    )

    # --- Dimensiones de Oro (para obtener DimId) ---
    dim_cliente = spark.read.table(f"{catalogo_oro}.{esquema_oro}.Dim_Cliente")
    dim_operacion = spark.read.table(f"{catalogo_oro}.{esquema_oro}.Dim_Operacion")

    # --- Construir hecho ---
    return (
        hub_trx
        # Atributos estables de la transacción
        .join(sat_estable.select("Hash_Transaccion", "tipo_transaccion",
              "moneda_transaccion", "estado_transaccion", "canal_transaccion"),
              "Hash_Transaccion")
        # Filtrar solo ATM
        .filter(F.col("tipo_transaccion").isin(TIPO_CATM, TIPO_DATM))
        # Montos de la transacción
        .join(sat_montos.select("Hash_Transaccion", "fecha_transaccion",
              "monto_principal", "comision_transaccion", "total_transaccion",
              "identificador_cliente"),
              "Hash_Transaccion")
        # Obtener Hash_Cliente via Link
        .join(link_ct.select("Hash_Transaccion", "Hash_Cliente"),
              "Hash_Transaccion", "left")
        # Obtener IdentificadorCliente desde Hub_Cliente
        .join(hub_cli.select("Hash_Cliente", "IdentificadorCliente"),
              "Hash_Cliente", "left")
        # FK → Dim_Cliente (DimIdCliente via IdentificadorCliente)
        .join(dim_cliente.select("IdentificadorCliente", "DimIdCliente"),
              "IdentificadorCliente", "left")
        # FK → Dim_Operacion (DimIdOperacion transitivamente via IdentificadorCliente)
        # NOTA: Si un cliente tiene múltiples operaciones (BLSQ > 1), este join
        # genera una fila por cada combinación (transacción, operación). Esto es
        # intencional: cada operación del cliente se refleja como contexto dimensional.
        .join(dim_operacion.select("IdentificadorCliente", "DimIdOperacion"),
              "IdentificadorCliente", "left")
        # Campos calculados
        .withColumn("EsRetiro", F.col("tipo_transaccion") == F.lit(TIPO_DATM))
        .withColumn("EsDeposito", F.col("tipo_transaccion") == F.lit(TIPO_CATM))
        # Selección final — sin Hashes
        .select(
            F.col("fecha_transaccion").alias("FechaClave"),
            F.col("DimIdCliente"),
            F.col("IdentificadorTransaccion"),
            F.col("DimIdOperacion"),
            F.col("tipo_transaccion").alias("TipoTransaccion"),
            F.col("EsRetiro"),
            F.col("EsDeposito"),
            F.col("monto_principal").alias("MontoPrincipal"),
            F.col("comision_transaccion").alias("ComisionTransaccion"),
            F.col("total_transaccion").alias("TotalTransaccion"),
            F.col("moneda_transaccion").alias("MonedaTransaccion"),
            F.col("estado_transaccion").alias("EstadoTransaccion"),
            F.col("canal_transaccion").alias("CanalTransaccion")
        )
    )
```

---


## Campos Calculados — Definición Detallada (Aprobados)


### 3.1 Campos Calculados para la Entidad Cliente (Mínimo 2 requeridos)

### 3.1.1 `RangoEtario` — Clasificación por edad

- **Ubicación**: Sat_Cliente_DatosEstables
- **Campo fuente**: `CUSAG2` (edad_cliente)
- **Datos observados**: rango 19–56, media 37.38, distribución uniforme

| Rango CUSAG2 | Clasificación |
|-------------|---------------|
| 18–25 | `JOVEN_ADULTO` |
| 26–35 | `ADULTO` |
| 36–45 | `ADULTO_MEDIO` |
| 46–55 | `ADULTO_MAYOR` |
| ≥56 | `SENIOR` |

```python
F.when(F.col("CUSAG2").between(18, 25), F.lit("JOVEN_ADULTO"))
 .when(F.col("CUSAG2").between(26, 35), F.lit("ADULTO"))
 .when(F.col("CUSAG2").between(36, 45), F.lit("ADULTO_MEDIO"))
 .when(F.col("CUSAG2").between(46, 55), F.lit("ADULTO_MAYOR"))
 .otherwise(F.lit("SENIOR"))
 .alias("RangoEtario")
```

### 3.1.2 `CategoriaIngresos` — Clasificación por ingresos mensuales

- **Ubicación**: Sat_Cliente_DatosEstables
- **Campo fuente**: `CUSIN` (ingresos_cliente)
- **Datos observados**: rango 10.46–99,998.85, media 49,907.57, distribución uniforme

| Rango CUSIN | Clasificación |
|------------|---------------|
| 0–15,000 | `BAJO` |
| 15,001–35,000 | `MEDIO` |
| 35,001–65,000 | `ALTO` |
| 65,001–85,000 | `MUY_ALTO` |
| >85,000 | `PREMIUM` |

```python
F.when(F.col("CUSIN") <= 15000, F.lit("BAJO"))
 .when(F.col("CUSIN") <= 35000, F.lit("MEDIO"))
 .when(F.col("CUSIN") <= 65000, F.lit("ALTO"))
 .when(F.col("CUSIN") <= 85000, F.lit("MUY_ALTO"))
 .otherwise(F.lit("PREMIUM"))
 .alias("CategoriaIngresos")
```

### 3.2 Campos Calculados para la Entidad Operación/Saldos (Mínimo 3 requeridos)

### 3.2.1 `CategoriaSaldo` — Clasificación por saldo disponible

- **Ubicación**: Sat_Operacion_DatosEstables
- **Campo fuente**: `BLAV` (saldo_disponible)
- **Datos observados**: rango 10.26–99,999.73, media 49,811.75

| Rango BLAV | Clasificación |
|-----------|---------------|
| 0–10,000 | `BAJO` |
| 10,001–30,000 | `MEDIO` |
| 30,001–60,000 | `ALTO` |
| 60,001–90,000 | `MUY_ALTO` |
| >90,000 | `PREMIUM` |

```python
F.when(F.col("BLAV") <= 10000, F.lit("BAJO"))
 .when(F.col("BLAV") <= 30000, F.lit("MEDIO"))
 .when(F.col("BLAV") <= 60000, F.lit("ALTO"))
 .when(F.col("BLAV") <= 90000, F.lit("MUY_ALTO"))
 .otherwise(F.lit("PREMIUM"))
 .alias("CategoriaSaldo")
```

### 3.2.2 `EstadoUtilizacionCredito` — Clasificación por ratio de utilización

- **Ubicación**: Sat_Operacion_DatosEstables
- **Campo fuente**: `BLRT` (ratio_cuenta)
- **Datos observados**: rango 0.00–0.20, media 0.10

| Rango BLRT | Clasificación |
|-----------|---------------|
| 0.00 | `SIN_USO` |
| 0.01–0.05 | `USO_BAJO` |
| 0.06–0.10 | `USO_MODERADO` |
| 0.11–0.15 | `USO_ALTO` |
| >0.15 | `SOBRE_UTILIZADO` |

```python
F.when(F.col("BLRT") == 0, F.lit("SIN_USO"))
 .when(F.col("BLRT") <= 0.05, F.lit("USO_BAJO"))
 .when(F.col("BLRT") <= 0.10, F.lit("USO_MODERADO"))
 .when(F.col("BLRT") <= 0.15, F.lit("USO_ALTO"))
 .otherwise(F.lit("SOBRE_UTILIZADO"))
 .alias("EstadoUtilizacionCredito")
```

### 3.2.3 `IndicadorSobregiro` — Clasificación por valor de sobregiro

- **Ubicación**: Sat_Operacion_DatosEstables
- **Campo fuente**: `BLOV` (valor_sobregiro)
- **Datos observados**: rango 0.10–4,999.93, media 2,501.50

| Rango BLOV | Clasificación |
|-----------|---------------|
| 0–100 | `SIN_SOBREGIRO` |
| 101–1,000 | `SOBREGIRO_LEVE` |
| 1,001–3,000 | `SOBREGIRO_MODERADO` |
| >3,000 | `SOBREGIRO_CRITICO` |

```python
F.when(F.col("BLOV") <= 100, F.lit("SIN_SOBREGIRO"))
 .when(F.col("BLOV") <= 1000, F.lit("SOBREGIRO_LEVE"))
 .when(F.col("BLOV") <= 3000, F.lit("SOBREGIRO_MODERADO"))
 .otherwise(F.lit("SOBREGIRO_CRITICO"))
 .alias("IndicadorSobregiro")
```

### 3.3 Campos Calculados para la Entidad Transaccional (Mínimo 3 requeridos)

### 3.3.1 `RangoMontoTransaccion` — Clasificación por monto principal

- **Ubicación**: Sat_Transaccion_Montos
- **Campo fuente**: `TRXAMT` (monto_principal)
- **Datos observados**: rango 10.05–99,999.55, media 49,972.89

| Rango TRXAMT | Clasificación |
|-------------|---------------|
| 0–1,000 | `MICRO` |
| 1,001–10,000 | `PEQUENA` |
| 10,001–50,000 | `MEDIANA` |
| 50,001–90,000 | `GRANDE` |
| >90,000 | `MUY_GRANDE` |

```python
F.when(F.col("TRXAMT") <= 1000, F.lit("MICRO"))
 .when(F.col("TRXAMT") <= 10000, F.lit("PEQUENA"))
 .when(F.col("TRXAMT") <= 50000, F.lit("MEDIANA"))
 .when(F.col("TRXAMT") <= 90000, F.lit("GRANDE"))
 .otherwise(F.lit("MUY_GRANDE"))
 .alias("RangoMontoTransaccion")
```

### 3.3.2 `NivelRiesgoFraude` — Clasificación por probabilidad de fraude

- **Ubicación**: Sat_Transaccion_Montos
- **Campo fuente**: `TRXFR` (riesgo_fraude)
- **Datos observados**: **Escala 0–100** (NO 0–1 como documenta SYSTEM.md), media 49.89

| Rango TRXFR | Clasificación |
|------------|---------------|
| 0–20 | `SIN_RIESGO` |
| 21–40 | `RIESGO_BAJO` |
| 41–60 | `RIESGO_MODERADO` |
| 61–80 | `RIESGO_ALTO` |
| >80 | `RIESGO_CRITICO` |

```python
F.when(F.col("TRXFR") <= 20, F.lit("SIN_RIESGO"))
 .when(F.col("TRXFR") <= 40, F.lit("RIESGO_BAJO"))
 .when(F.col("TRXFR") <= 60, F.lit("RIESGO_MODERADO"))
 .when(F.col("TRXFR") <= 80, F.lit("RIESGO_ALTO"))
 .otherwise(F.lit("RIESGO_CRITICO"))
 .alias("NivelRiesgoFraude")
```

### 3.3.3 `ClasificacionCanalATM` — Clasificación de transacciones por canal/tipo ATM

- **Ubicación**: Sat_Transaccion_DatosEstables
- **Campos fuente**: `TRXTYP` (tipo_transaccion) + `TRXCH` (canal_transaccion)
- **Datos observados**: TRXTYP tiene 15 valores; DATM y CATM representan el 29% del total

| Condición | Clasificación |
|-----------|---------------|
| `TRXTYP == 'DATM'` | `RETIRO_ATM` |
| `TRXTYP == 'CATM'` | `DEPOSITO_ATM` |
| `TRXCH == 'ATM' AND TRXTYP NOT IN ('DATM','CATM')` | `OTRA_OP_ATM` |
| Todos los demás | `NO_ATM` |

```python
F.when(F.col("TRXTYP") == F.lit(TIPO_DATM), F.lit("RETIRO_ATM"))
 .when(F.col("TRXTYP") == F.lit(TIPO_CATM), F.lit("DEPOSITO_ATM"))
 .when(F.col("TRXCH") == F.lit("ATM"), F.lit("OTRA_OP_ATM"))
 .otherwise(F.lit("NO_ATM"))
 .alias("ClasificacionCanalATM")
```

---


## Diagramas del Modelo

### Apéndice A — Diagrama del Modelo Data Vault 2.0 (Plata)

```
┌──────────────────────┐     ┌──────────────────────────────────┐     ┌──────────────────────┐
│   Hub_Cliente        │     │    Link_Cliente_Operacion        │     │   Hub_Operacion      │
│──────────────────────│     │──────────────────────────────────│     │──────────────────────│
│ FechaRegistro        │     │ FechaRegistro                    │     │ FechaRegistro        │
│ Hash_Cliente         │◄───►│ Hash_Cliente                     │◄───►│ Hash_Operacion       │
│ IdentificadorCliente │     │ Hash_Operacion                   │     │ IdentificadorCliente │
│ FuenteDatos          │     │ Hash_Link_Cliente_Operacion      │     │ SecuenciaSaldo       │
└────────┬─────────────┘     │ FuenteDatos                      │     │ FuenteDatos          │
         │                   └──────────────────────────────────┘     └────────┬─────────────┘
         │                                                                      │
         │    ┌──────────────────────────────────┐                             │
         │    │    Link_Cliente_Transaccion      │                             │
         │    │──────────────────────────────────│                             │
         ├───►│ FechaRegistro                    │◄──┐                        │
         │    │ Hash_Cliente                     │   │                        │
         │    │ Hash_Transaccion                 │   │                        │
         │    │ Hash_Link_Cliente_Transaccion    │   │                        │
         │    │ FuenteDatos                      │   │                        │
         │    └──────────────────────────────────┘   │                        │
         │                                            │                        │
         │                               ┌────────────┴───────────────┐       │
         │                               │    Hub_Transaccion         │       │
         │                               │────────────────────────────│       │
         │                               │ FechaRegistro              │       │
         │                               │ Hash_Transaccion           │       │
         │                               │ IdentificadorTransaccion   │       │
         │                               │ FuenteDatos                │       │
         │                               └────────────┬──────────────┘       │
         │                                             │                      │
    ┌────┴──────────────────────────┐            ┌─────┴──────────────────────┤
    │ SATELLITES de CLIENTE (4)     │            │ SATELLITES de TRANSACCION  │
    │───────────────────────────────│            │ (2)                        │
    │ 1. Sat_Cliente_DatosEstables  │            │────────────────────────────│
    │ 2. Sat_Cliente_Contacto       │            │ 1. Sat_Trx_DatosEstables   │
    │ 3. Sat_Cliente_Clasificacion  │            │ 2. Sat_Trx_Montos          │
    │ 4. Sat_Cliente_Financiero     │            │ (+ campos calculados)      │
    │ (+ campos calculados)         │            └────────────────────────────┘
    └───────────────────────────────┘

    ┌───────────────────────────────┐
    │ SATELLITES de OPERACION (3)   │
    │───────────────────────────────│
    │ 1. Sat_Oper_DatosEstables     │
    │ 2. Sat_Oper_Montos            │
    │ 3. Sat_Oper_FechasEvento      │
    │ (+ campos calculados)         │
    └───────────────────────────────┘
```

### Apéndice B — Diagrama del Modelo Estrella (Oro)

```
                    ┌──────────────────────┐
                    │    Dim_Tiempo         │
                    │──────────────────────│
                    │ FechaClave (PK)      │
                    │ Anio, Mes, Dia       │
                    │ Trimestre, Semestre  │
                    │ NombreDia/Mes        │
                    │ EsFinSemana          │
                    └──────────┬───────────┘
                               │
┌──────────────────────┐       │       ┌──────────────────────┐
│  Dim_Cliente         │       │       │  Dim_Operacion       │
│──────────────────────│       │       │──────────────────────│
│ DimIdCliente (PK)    │  ┌────┴────┐  │ DimIdOperacion (PK)  │
│ IdentificadorCliente │  │  Fact   │  │ IdentificadorCliente │
│ NombreCompletoCliente│  │  Trans  │  │ SecuenciaSaldo       │
│ SexoCliente          ├─►│  _ATM   │◄─┤ TipoCuenta           │
│ EdadCliente          │  │─────────│  │ MonedaCuenta         │
│ RangoEtario          │  │ FechaTrx│  │ EstadoCuenta         │
│ SegmentoCliente      │  │ DimIdCl │  │ CategoriaSaldo       │
│ TipoCliente          │  │ IdTrx   │  │ EstadoUtilizCredito  │
│ NivelRiesgo          │  │ DimIdOp │  │ IndicadorSobregiro   │
│ CategoriaIngresos    │  │ TipoTrx │  │ SaldoDisponible      │
│ ScoreCliente         │  │ EsRetiro│  │ SaldoTotal           │
│ ...                  │  │ MontoPr.│  │ LimiteCredito        │
└──────────────────────┘  │ TotalTrx│  └──────────────────────┘
                          │ MonedaTx│
                          │ EstadoTx│
                          │ CanalTx │
                          └─────────┘
```

### Apéndice C — Resumen de Variación entre Snapshots (Base para Decisión de Satellites)

#### CMSTFL — Variación observada entre 2026-04-03 y 2026-04-04

| Grupo | Campos | Cambio | Satellite Propuesto |
|-------|--------|--------|---------------------|
| Datos Personales + Contacto | CUSNM, CUSLN, CUSMD, CUSFN, CUSMS, CUSOC, CUSED, CUSAD, CUSA2, CUSCT, CUSST, CUSZP, CUSPH, CUSMB, CUSEM | **15–20%** | **Sat_Cliente_Contacto** |
| Identidad | CUSSX, CUSTT, CUSDB, CUSYR, CUSAG2, CUSCN, CUSNA, CUSDL, CUSDP, CUSDP2, CUSLG | **0%** | **Sat_Cliente_DatosEstables** |
| Clasificación Bancaria | CUSTP, CUSSG, CUSRK, CUSVP, CUSPF, CUSKT, CUSFM, CUSLC, CUSCR, CUSAC, CUSCL, CUSRG, CUSBR, CUSMG, CUSRF, CUSRS, CUSAG, CUSPC, CUSNT | **0%** | **Sat_Cliente_Clasificacion** |
| Numéricos + Fechas | CUSAC2, CUSTX, CUSSC, CUSLR, CUSRC, CUSIN, CUSBL + 18 fechas | **0%** | **Sat_Cliente_Financiero** |

#### TRXPFL — Variación observada

| Grupo | Campos (cantidad) | Cambio | Satellite Propuesto |
|-------|-------------------|--------|---------------------|
| Categóricos + Secuencia + Fechas aux. + Timestamps + TRXMX/TRXMN | 29 campos | **0%** | **Sat_Transaccion_DatosEstables** |
| Montos + Riesgos + CUSTID + TRXDT | 30 campos | **87.5–100%** | **Sat_Transaccion_Montos** |

#### BLNCFL — Variación observada

| Grupo | Campos (cantidad) | Cambio | Satellite Propuesto |
|-------|-------------------|--------|---------------------|
| Atributos cualitativos cuenta | 30 campos string | **0%** | **Sat_Operacion_DatosEstables** |
| Montos y ratios financieros | 34 campos double | **0%** | **Sat_Operacion_Montos** |
| Fechas de eventos | 34 campos date | **0%** | **Sat_Operacion_FechasEvento** |

> **Nota**: BLNCFL no mostró variación en los 2 snapshots analizados. La separación en 3 Satellites se basa en la **naturaleza funcional** de los campos y en la expectativa de que en producción real los montos/saldos cambien con mayor frecuencia que los atributos cualitativos o las fechas históricas.


---

# Reglas del Modelo de Datos

## Reglas para Tablas Hub (Mínimo 3 Hubs)

1. **Hub_Cliente**: Entidad de negocio Cliente. Llave de negocio: `CUSTID` (identificador_cliente). Fuente: CMSTFL.
2. **Hub_Operacion**: Entidad de negocio Operación/Saldo. Llave de negocio compuesta: `CUSTID` + `BLSQ` (identificador_cliente + secuencia_saldo). Fuente: BLNCFL.
3. **Hub_Transaccion**: Entidad de negocio Transacción. Llave de negocio: `TRXID` (StringType nativo, 100% único en datos reales). Fuente: TRXPFL.

## Reglas para Tablas Satellite (Mínimo 2 por Hub)

Cada Hub debe tener **al menos dos Satellites**:
- **Un Satellite de atributos estables**: Variables que no cambian o cuya tasa de cambio es muy baja.
- **Uno o más Satellites de atributos variables por concepto**: Variables que cambian con frecuencia, agrupadas temáticamente (montos, fechas variantes, clasificaciones, etc.).

## Reglas para Tablas Link (Relaciones binarias exclusivamente)

Cada Link relaciona **exactamente dos Hubs** (alcance del laboratorio):

| Link | Hub 1 | Hub 2 | Justificación |
|------|-------|-------|---------------|
| **Link_Cliente_Operacion** | Hub_Cliente | Hub_Operacion | BLNCFL contiene `CUSTID` que conecta la cuenta/saldo con el cliente propietario. |
| **Link_Cliente_Transaccion** | Hub_Cliente | Hub_Transaccion | TRXPFL contiene `CUSTID` que conecta cada transacción con el cliente que la realizó. |
| ~~Link_Operacion_Transaccion~~ | ~~Hub_Operacion~~ | ~~Hub_Transaccion~~ | **ELIMINADO por decisión aprobada**: Las transacciones las realizan los clientes, no tienen relación directa con las operaciones (saldos). La relación Operación↔Transacción se resuelve transitivamente a través del Cliente en la capa de Oro. |

## Reglas de Campos Calculados en Satellites (Medalla de Plata)

### Entidad Cliente — 2 campos calculados CASE (✅ Aprobados)

- **`RangoEtario`**: Clasificación por `edad_cliente` (`CUSAG2`) → "Joven Adulto" (18–25), "Adulto" (26–35), "Adulto Medio" (36–45), "Adulto Mayor" (46–55), "Senior" (≥56). Ubicación: `Sat_Cliente_DatosEstables`.
- **`CategoriaIngresos`**: Clasificación por `ingresos_cliente` (`CUSIN`) → "Bajo" (0–15,000), "Medio" (15,001–35,000), "Alto" (35,001–65,000), "Muy Alto" (65,001–85,000), "Premium" (>85,000). Ubicación: `Sat_Cliente_DatosEstables`.

### Entidad Operaciones/Saldos — 3 campos calculados CASE (✅ Aprobados)

- **`CategoriaSaldo`**: Clasificación por `saldo_disponible` (`BLAV`) → "Bajo" (0–10,000), "Medio" (10,001–30,000), "Alto" (30,001–60,000), "Muy Alto" (60,001–90,000), "Premium" (>90,000). Ubicación: `Sat_Operacion_DatosEstables`.
- **`EstadoUtilizacionCredito`**: Clasificación por `ratio_cuenta` (`BLRT`) → "Sin uso" (0), "Uso bajo" (0.001–0.05), "Uso moderado" (0.051–0.10), "Uso alto" (0.101–0.15), "Sobre-utilizado" (>0.15). Ubicación: `Sat_Operacion_DatosEstables`.
- **`IndicadorSobregiro`**: Clasificación por `valor_sobregiro` (`BLOV`) → "Sin sobregiro" (0–100), "Sobregiro leve" (101–1,000), "Sobregiro moderado" (1,001–3,000), "Sobregiro crítico" (>3,000). Ubicación: `Sat_Operacion_DatosEstables`.

### Entidad Transaccional — 3 campos calculados CASE (✅ Aprobados)

- **`RangoMontoTransaccion`**: Clasificación por `monto_principal` (`TRXAMT`) → "Micro" (0–1,000), "Pequeña" (1,001–10,000), "Mediana" (10,001–50,000), "Grande" (50,001–90,000), "Muy Grande" (>90,000). Ubicación: `Sat_Transaccion_Montos`.
- **`NivelRiesgoFraude`**: Clasificación por `riesgo_fraude` (`TRXFR`, escala 0–100) → "Sin riesgo" (0–20), "Riesgo bajo" (21–40), "Riesgo moderado" (41–60), "Riesgo alto" (61–80), "Riesgo crítico" (>80). Ubicación: `Sat_Transaccion_Montos`.
- **`ClasificacionCanalATM`**: Clasificación por `tipo_transaccion` (`TRXTYP`) → "Retiro ATM" (DATM), "Depósito ATM" (CATM), "No ATM" (otros). Ubicación: `Sat_Transaccion_DatosEstables`.

## Reglas de Liquid Clustering (Medalla de Plata)

| Tipo de Tabla | Columnas del Liquid Cluster (en orden) |
|---------------|----------------------------------------|
| Hub | `FechaRegistro`, `Hash_{NombreHub}` |
| Link | `FechaRegistro`, `Hash_{NombreHub1}`, `Hash_{NombreHub2}` |
| Satellite | `FechaRegistro`, `Hash_{NombreHubOLink}` |
| Bronce (ST y MV) | `FechaRegistroParquet` |
| Dim_Cliente | `DimIdCliente` |
| Dim_Operacion | `DimIdOperacion` |
| Dim_Tiempo | `FechaClave` |
| Hec_Transacciones_ATM | `FechaClave`, `DimIdCliente` |

> **Regla de ordenamiento de columnas**: Las columnas de Liquid Clustering deben definirse en las **primeras posiciones** del esquema de cada tabla. Según la documentación oficial de Databricks (https://docs.databricks.com/aws/en/delta/clustering), las columnas de LC deben tener estadísticas recopiladas, y por defecto solo las primeras 32 columnas de una tabla Delta tienen estadísticas. Todos los esquemas de este documento ya reflejan este ordenamiento.

## Comportamiento Incremental: Dimensión de Tiempo (Medalla de Oro)

`Dim_Tiempo` se implementa como **Vista Materializada con refresh incremental**. No requiere lógica imperativa de fechas. El motor LSDP:

1. Lee los valores distintos de `Sat_Transaccion_Montos.fecha_transaccion` en cada refresh.
2. Incorpora automáticamente las fechas nuevas que aún no existen en la dimensión.
3. Garantiza la consistencia entre `Dim_Tiempo.FechaClave` y `Hec_Transacciones_ATM.FechaClave` por construcción (mismo origen).

> **Nota sobre llaves subrogadas (mitigación R-03 aprobada)**: Los valores de `DimIdCliente` y `DimIdOperacion` son estables únicamente para el mismo conjunto de hashes de entrada (`dense_rank` sobre orden lexicográfico). Si cambia el conjunto de hashes (alta/baja de entidades), los IDs pueden reasignarse. Las herramientas de consumo BI **no deben referenciar valores literales de `DimId`** ni almacenarlos como constantes externas.
>
> **Nota sobre `DimIdOperacion` (mitigación R-02 aprobada)**: La llave `DimIdOperacion` en `Hec_Transacciones_ATM` se resuelve como la **operación dominante por cliente** (`SecuenciaSaldo desc, Hash_Operacion asc`). No representa la operación de la transacción individual (no existe esa relación en las fuentes actuales). Este supuesto está documentado y aceptado para el alcance del laboratorio.

## Propiedades Delta Obligatorias (Todas las Tablas)

**Sin excepción**, toda tabla Delta del pipeline — incluyendo Materialized Views (`@dp.materialized_view`), Streaming Tables (`dp.create_streaming_table` / `@dp.table`), y Streaming Tables temporales (`temporary=True`) — debe declarar las siguientes `table_properties`:

```python
table_properties={
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.enableChangeDataFeed": "true",
    "delta.logRetentionDuration": "interval 60 days",
}
```

| Propiedad | Valor | Propósito |
|-----------|-------|-----------|
| `delta.autoOptimize.autoCompact` | `"true"` | Compactación automática de archivos pequeños tras cada escritura. |
| `delta.autoOptimize.optimizeWrite` | `"true"` | Optimización del tamaño de archivos durante la escritura. |
| `delta.deletedFileRetentionDuration` | `"interval 30 days"` | Retención de archivos eliminados para time-travel y VACUUM. |
| `delta.enableChangeDataFeed` | `"true"` | Habilita Change Data Feed para captura incremental de cambios. |
| `delta.logRetentionDuration` | `"interval 60 days"` | Retención del log de transacciones Delta para auditoría y time-travel. |

> **Regla**: Estas propiedades aplican a las tres medallas (Bronce, Plata, Oro) y deben incluirse en cada definición de tabla sin omisión. Cualquier tabla que no las declare se considera incompleta.

---

# Reglas sobre los Datos

## Reglas de Integridad y Consistencia

- Todo `CUSTID` presente en TRXPFL o BLNCFL **debe existir** en CMSTFL (integridad referencial del origen).
- Los montos de transacciones (`TRXAMT`) deben ser valores positivos; el signo/tipo de la operación se determina por `TRXTYP`.
- Las fechas de transacción (`TRXDT`) no deben ser futuras respecto a la fecha de procesamiento del pipeline.
- La secuencia de saldo (`BLSQ`) combinada con `CUSTID` debe ser única en BLNCFL.

## Expectations — Calidad de Datos (Aprobadas)


### 4.1 Expectations para Medalla de Bronce

**No se aplican expectations en la Medalla de Bronce.** Los datos se ingestan tal como llegan de la fuente AS400, sin filtrado ni validación en esta capa.

### 4.2 Expectations para Medalla de Plata

| # | Nombre | Condición SQL | Decorador | Severidad | Tabla(s) Aplicable(s) | Justificación |
|---|--------|---------------|-----------|-----------|----------------------|---------------|
| E1 | `id_cliente_positivo` | `IdentificadorCliente > 0` | `@dp.expect_or_drop` | **DROP** | Hub_Cliente, Hub_Operacion, Links | IdentificadorCliente debe ser positivo. Datos muestran min=1. Un 0 o negativo indica dato corrupto → descartar. |
| E2 | `score_cliente_en_rango` | `score_cliente BETWEEN 300 AND 1150` | `@dp.expect("score_rango", ...)` | **WARN** | Sat_Cliente_Financiero | Rango documentado 300-1149. Datos muestran min=300, max=1149. Warn para monitorear desviaciones. |
| E3 | `id_transaccion_no_nulo` | `IdentificadorTransaccion IS NOT NULL` | `@dp.expect_or_fail` | **FAIL** | Hub_Transaccion | IdentificadorTransaccion es PK. Un nulo invalida la integridad del hub completo. |
| E4 | `monto_transaccion_positivo` | `monto_principal > 0` | `@dp.expect_or_drop` | **DROP** | Sat_Transaccion_Montos | Montos deben ser positivos. El tipo débito/crédito se determina por TRXTYP. Datos muestran min=10.05. |
| E5 | `hash_diferenciador_no_nulo` | `Hash_Diferenciador IS NOT NULL` | `@dp.expect_or_fail` | **FAIL** | Todos los Satellites | El hash es esencial para la detección de cambios. Si es nulo, el Satellite no puede funcionar correctamente. |
| E6 | `riesgo_fraude_en_rango` | `riesgo_fraude BETWEEN 0 AND 100` | `@dp.expect("riesgo_fraude_rango", ...)` | **WARN** | Sat_Transaccion_Montos | Escala confirmada 0–100. Warn para detectar outliers futuros. |

### 4.3 Expectations para Medalla de Oro

| # | Nombre | Condición SQL | Decorador | Severidad | Tabla(s) Aplicable(s) | Justificación |
|---|--------|---------------|-----------|-----------|----------------------|---------------|
| E7 | `dim_id_cliente_no_nulo` | `DimIdCliente IS NOT NULL` | `@dp.expect_or_fail` | **FAIL** | Dim_Cliente, Hec_Transacciones_ATM | La llave subrogada es esencial para la integridad referencial del modelo estrella. |
| E8 | `dim_id_operacion_no_nulo` | `DimIdOperacion IS NOT NULL` | `@dp.expect_or_fail` | **FAIL** | Dim_Operacion | Llave subrogada de la dimensión de operaciones. |
| E9 | `fecha_clave_no_nula` | `FechaClave IS NOT NULL` | `@dp.expect_or_fail` | **FAIL** | Hec_Transacciones_ATM | FK obligatoria hacia Dim_Tiempo. |
| E10 | `tipo_transaccion_atm` | `TipoTransaccion IN ('DATM', 'CATM')` | `@dp.expect_or_fail` | **FAIL** | Hec_Transacciones_ATM | La tabla de hechos solo debe contener transacciones ATM. |
| E11 | `monto_principal_positivo_oro` | `MontoPrincipal > 0` | `@dp.expect_or_drop` | **DROP** | Hec_Transacciones_ATM | Montos deben ser positivos en la tabla de hechos. |

### 4.4 Expectations Agrupadas (Patrón Recomendado)

```python
# Expectations para Hub_Cliente (Plata)
validaciones_hub_cliente = {
    "id_cliente_no_nulo": "IdentificadorCliente IS NOT NULL",
    "id_cliente_positivo": "IdentificadorCliente > 0",
    "hash_cliente_no_nulo": "Hash_Cliente IS NOT NULL"
}

# Expectations para Sat_Transaccion_Montos (Plata)
validaciones_sat_trx_montos = {
    "monto_principal_positivo": "monto_principal > 0",
    "riesgo_fraude_en_rango": "riesgo_fraude BETWEEN 0 AND 100",
    "riesgo_transaccion_en_rango": "riesgo_transaccion BETWEEN 0 AND 100",
    "hash_diferenciador_completo": "Hash_Diferenciador IS NOT NULL"
}

# Expectations para Hec_Transacciones_ATM (Oro)
validaciones_fact_atm = {
    "dim_id_cliente_no_nulo": "DimIdCliente IS NOT NULL",
    "fecha_clave_no_nula": "FechaClave IS NOT NULL",
    "tipo_transaccion_atm": "TipoTransaccion IN ('DATM', 'CATM')",
    "monto_principal_positivo": "MontoPrincipal > 0"
}
```

---

---


## Riesgos y Mitigaciones (Aprobados)


| # | Riesgo | Probabilidad | Impacto | Mitigación | Estado |
|---|--------|-------------|---------|------------|--------|
| R1 | **TRXID tipo StringType** vs LongType documentado | Confirmado | Medio | Todo el código de hash refleja StringType. Corregido en SYSTEM.md (incremento `documentacion-consolidada-y-metadata`). | ✅ Aprobada |
| R2 | **TRXRK/TRXFR escala 0-100** vs documentación 0-1 | Confirmado | Alto | Umbrales de campos calculados ajustados a escala 0-100. Corregido en SYSTEM.md. | ✅ Aprobada |
| R3 | **Campo BLCU** en Parquet vs `BLCN` en SYSTEM.md | Confirmado | Medio | Usar `BLCU` en todo el código. Corregido en SYSTEM.md. | ✅ Aprobada |
| R4 | **Volumetría variable** entre snapshots (250K vs 215K en TRXPFL) | Observado | Medio | El procesamiento Append Only del Data Vault maneja variaciones naturalmente. Documentado en SYSTEM.md. | ✅ Aprobada |
| R5 | **TRXDT = fecha del snapshot** (no fecha individual) | Observado | Bajo | Para Hec_Transacciones_ATM, usar TRXDT como fecha dimensional (fecha de corte). Documentado en SYSTEM.md. | ✅ Aprobada |
| R6 | **CUSTID reasignado** entre snapshots para mismo TRXID (99.8% cambio) | Observado | Alto | Los Hubs y Links capturan nuevas combinaciones como registros nuevos (Append Only). Documentado en SYSTEM.md. | ✅ Aprobada |
| R7 | **BLNCFL sin variación** entre 2 snapshots (0% cambio) | Observado | Bajo | Los Satellites no insertarán duplicados gracias al Hash_Diferenciador. Documentado en SYSTEM.md. | ✅ Aprobada |
| R8 | **Concurrencia limitada** por Free Edition (max 5 concurrent tasks) | Diseño | Medio | Se maneja según lo propone LSDP: el framework gestiona automáticamente la secuenciación y paralelización de las tablas del pipeline según sus dependencias. | ✅ Aprobada |
| R9 | **Tamaño del Hash_Diferenciador** para Satellites con muchas columnas | Diseño | Bajo | SHA2-512 es determinístico. `concat_ws` maneja correctamente nulos. Performance aceptable. Documentado en SYSTEM.md. | ✅ Aprobada |
| R10 | **Dimensión Tiempo** requiere refresh incremental nativo | Diseño | Medio | Implementada como `@dp.materialized_view` con refresh incremental (no ST). Fuente: `Sat_Transaccion_Montos.fecha_transaccion` (valores distintos). Documentado en SYSTEM.md. | ✅ Aprobada |
| R11 | **Integridad referencial parcial** — 5.7% de clientes sin transacciones | Observado | Bajo | LEFT JOIN en Links y en Hec_Transacciones_ATM. Clientes sin transacciones existen en dimensiones pero no en la tabla de hechos. Documentado en SYSTEM.md. | ✅ Aprobada |
| R12 | **Nuevos clientes diarios** (+450, 0.6%) podrían crecer | Observado | Bajo | Diseño Append Only maneja crecimiento orgánico. Liquid Clustering optimiza incrementalmente. DimId estable (`xxhash64`) en dimensiones absorbe nuevos registros sin afectar existentes. Documentado en SYSTEM.md. | ✅ Aprobada |

---

---


---

# Estándar de Desarrollo

## Convenciones de Nomenclatura

| Elemento | Convención | Ejemplo |
|----------|-----------|---------|
| Tablas Hub | `Hub_{NombreEntidad}` | `Hub_Cliente`, `Hub_Operacion`, `Hub_Transaccion` |
| Tablas Link | `Link_{NombreHub1}_{NombreHub2}` | `Link_Cliente_Operacion` |
| Tablas Satellite | `Sat_{NombreHubOLink}_{Concepto}` | `Sat_Cliente_DatosEstables`, `Sat_Cliente_Montos` |
| Dimensiones | `Dim_{NombreEntidad}` | `Dim_Cliente`, `Dim_Tiempo`, `Dim_Operacion` |
| Tabla de Hechos | `Hec_{NombreConcepto}` | `Hec_Transacciones_ATM` |
| Streaming Tables Temporal | `{NombreOrigen}_temp` | `CMSTFL_temp` |
| Vistas Materializadas Bronce | `{NombreOrigen}` (nombre directo del Parquet) | `CMSTFL`, `TRXPFL`, `BLNCFL` |
| Columnas Hash | `Hash_{NombreEntidad}` o `Hash_Diferenciador` | `Hash_Cliente`, `Hash_Diferenciador` |
| Columnas de auditoría | `FechaRegistro`, `FuenteDatos` | — |
| Notebooks de carpeta transformations de LSDP | `LSDP{Medalla}{Nombre}` | `LSDPBronceCMSTFL`, `LSDPPlataHubCliente` |
| Archivos Python puro de carpeta utilities de LSDP | `LSDP{NombreUtilidad}.py` | `LSDPUtilidadPrincipal.py`, `LSDPUtilidadesDataFrame.py` |
| Nombre de objetos, clases, funciones, metodos, variables y constantes en idioma españo | `{nombreVariable}`, `{nombreObjeto}`, `{nombreFuncion}` | `dfFinal`, `constanteRegla1` |
| Todo el Código ampliamente detallado y explicado por comentarios | `"""Comentarios"""` | - |
| Uso de Bloques Markdown para ampliar la documentación | `# Databricks notebook source # MAGIC %md # MAGIC # Comentario # COMMAND ----------` | - |
| Todas las tablas delta / vista metarializadas deben tener pruebas funcionales de los datos, tanto de su comportamiento como correcto procesamiento y carga. Por ejemplo: los Hubs no deben tener llaves de negocio repetidas, los satelites solo deben aceptar cambios cuando el Hash_Diferenciador sea diferentes, etc | - | `SELECT {LlaveNegocio}, COUNT(*) AS Q FROM Hub_Cliente GROUP BY {LlaveNegocio} HAVING COUNT(*) >= 2` |

## Principios de Parametrización

La solución **no debe contener valores hard-coded**. Toda configuración variable debe externalizarse mediante:

- **Parámetros del pipeline LSDP**: Nombres de catálogo, esquema, volumen, rutas de landing zone.
- **Variables de configuración**: Nombres de tablas, umbrales de clasificación para campos calculados, algoritmos de hash.
- **Constantes nombradas**: Valores fijos con significado de dominio (ej: tipos de transacción ATM: `"DATM"`, `"CATM"`) definidos como constantes en el notebook de configuración, no dispersos en el código.

## Estructura de Notebooks Propuesta

| Notebook | Medalla | Responsabilidad |
|----------|---------|-----------------|
| `00_Configuracion` | Transversal | Parámetros, constantes, funciones helper compartidas |
| `01_Bronce_Ingesta` | Bronce | Streaming Tables + Snapshot Tables para CMSTFL, TRXPFL, BLNCFL |
| `02_Plata_Hubs` | Plata | Tablas Hub_Cliente, Hub_Operacion, Hub_Transaccion |
| `03_Plata_Links` | Plata | Tablas Link_Cliente_Operacion, Link_Cliente_Transaccion |
| `04_Plata_Satellites` | Plata | Todos los Satellites con sus campos calculados y expectations |
| `05_Oro_Dimensiones` | Oro | Dim_Cliente, Dim_Operacion, Dim_Tiempo |
| `06_Oro_Hechos` | Oro | Hec_Transacciones_ATM |

> **Nota**: La estructura definitiva de notebooks se refinará en la fase de Design del SDD.

## Principios de Calidad de Código

- Cada función decorada con `@dp.table` o `@dp.materialized_view` debe tener una única responsabilidad.
- Las transformaciones complejas deben descomponerse en funciones auxiliares reutilizables.
- Los hashes (SHA2-256 para Hubs/Links, SHA2-512 para Satellites) se calculan mediante funciones helper centralizadas para evitar inconsistencias.
- Las expectations de calidad de datos se definen en los decoradores LSDP donde corresponda.

---

---

# Qué NO Debe Hacer la Solución

## Restricciones de Implementación

1. **No utilizar APIs incompatibles con Serverless**: Ver la tabla completa de prohibiciones en la sección [Restricciones Críticas del Entorno Serverless](#restricciones-críticas-del-entorno-serverless). Esto incluye `.cache()`, `.persist()`, `spark.sparkContext`, operaciones RDD, broadcast variables, accumulators, threading/multiprocessing.
2. **No hard-codear valores**: Ningún nombre de catálogo, esquema, tabla, ruta, umbral de clasificación o constante de dominio debe estar escrito directamente en el código de transformación. Todo debe referenciarse desde el notebook de configuración o desde parámetros del pipeline. Solamente las constantes definidas en la sección [constantes de negocio notebook de configuración](#constantes-de-negocio-notebook-de-configuración) son las que entran en la excepción de esta regla.
3. **No modificar configuraciones protegidas de Spark**: No cambiar `spark.sql.ansi.enabled` ni otras configuraciones fuera del rango permitido (`spark.sql.shuffle.partitions` y `spark.sql.adaptive.*`).
4. **No usar el import incorrecto de LSDP**: Nunca `import databricks.sdk.pipelines as dp`. Siempre `from pyspark import pipelines as dp`.
5. **No crear tablas con parámetros de decorador no soportados**: No usar `catalog=` ni `schema=` como kwargs separados en `@dp.materialized_view()`. Usar siempre el nombre de tres partes en `name=`.
6. **No acumular historia en las Snapshot Tables de Bronce**: Las tablas de instantánea actual deben contener **solo** los datos de la fecha más reciente.
7. **No eliminar ni actualizar registros en tablas de Plata**: El Data Vault 2.0 Raw Vault es estrictamente Append Only. Nunca se ejecutan UPDATE ni DELETE sobre Hubs, Links o Satellites.
8. **No mezclar responsabilidades entre medallas**: La lógica de negocio de Data Vault pertenece a Plata; la lógica dimensional pertenece a Oro. Bronce solo se encarga de ingesta y snapshot.
9. **No ignorar ANSI mode**: El modo ANSI está habilitado por defecto. Todo cast y operación aritmética debe considerar las reglas de overflow documentadas.
10. **No usar UDFs (User Defined Functions)**: Preferir siempre las funciones nativas de PySpark (`pyspark.sql.functions.*`). Las UDFs no están optimizadas en Serverless Compute ni en Photon.
11. **El Modelo de IA NO debe alucinar**: Debe mantener el alcance definido en cada ejecución del slash command /kiro-spec-init y no agregar nada adicional.
12. **El Modelo de IA NO debe tomar decisiones, solo propone**: Cualquier propuesta o idea de mejora debe ser propuesta al usuario y es el usuario quien toma la decisón final. En cualquier etapa del cc-sdd(Spec-Driven Development inspirado en Kiro) como por ejemplo el research o cualquier acción que implica investigación y descubrimiento para luego tomar decisiones, debe de estar generar una propuesta y esperar a que el usuario tome la decisión o suministre la solución, mientras la decisión no sea tomada, no debes permitir continuar con el proceso.



---

---

---

---

# Historial de Cambios

| Fecha | Incremento SDD | Cambio | Referencia |
|-------|---------------|--------|------------|
| 2026-05-01 | `documentacion-consolidada-y-metadata` | Corrección: `Fact_Transacciones_ATM` → `Hec_Transacciones_ATM` (nombre real implementado) | [tasks.md#2.1](/.kiro/specs/documentacion-consolidada-y-metadata/tasks.md) |
| 2026-05-01 | `documentacion-consolidada-y-metadata` | Corrección: `SYSTEM2.md` → `SYSTEM.md` en sección de estructura del repositorio | [tasks.md#2.1](/.kiro/specs/documentacion-consolidada-y-metadata/tasks.md) |
| 2026-05-01 | `documentacion-consolidada-y-metadata` | Corrección: parámetros del pipeline — 13 parámetros reales (`volumen` + rutas individuales por fuente) en lugar de `ruta_base` + `ruta_base_autoloader` (no implementados) | [tasks.md#2.1](/.kiro/specs/documentacion-consolidada-y-metadata/tasks.md) |
| 2026-05-01 | `documentacion-consolidada-y-metadata` | Corrección: estrategia Sat_Transaccion_* — `@dp.append_flow()` puro (sin `procesar_satellite_transaccional()`). La función existe en LSDPUtilidadPrincipal pero no es invocada; la deduplicación natural la da CDF + unicidad de TRXID | [tasks.md#2.1](/.kiro/specs/documentacion-consolidada-y-metadata/tasks.md) |
| 2026-05-01 | `documentacion-consolidada-y-metadata` | Corrección: Tabla de convenciones `Fact_` → `Hec_` para tabla de hechos | [tasks.md#2.1](/.kiro/specs/documentacion-consolidada-y-metadata/tasks.md) |
| 2026-05-01 | `documentacion-consolidada-y-metadata` | Corrección: referencias a `SYSTEM2.md` en tabla de riesgos R1-R12 actualizadas a "SYSTEM.md" | [tasks.md#2.1](/.kiro/specs/documentacion-consolidada-y-metadata/tasks.md) |
| 2026-05-01 | `documentacion-consolidada-y-metadata` | Corrección: R10 (Dim_Tiempo) — implementada como `@dp.materialized_view` incremental, NO como ST acumulativa | [tasks.md#2.1](/.kiro/specs/documentacion-consolidada-y-metadata/tasks.md) |
| 2026-05-01 | `documentacion-consolidada-y-metadata` | Corrección: R12 — `DimIdCliente`/`DimIdOperacion` estables usando `xxhash64(hash_col).cast("long")` (no `dense_rank`) | [tasks.md#2.1](/.kiro/specs/documentacion-consolidada-y-metadata/tasks.md) |

## Glosario de Términos

| Término | Definición |
|---------|-----------|
| **LSDP** | Lakeflow Spark Declarative Pipelines — Framework declarativo de Databricks para definir pipelines de datos usando decoradores Python. |
| **Data Vault 2.0** | Metodología de modelado de datos para Data Warehouses que separa las entidades de negocio (Hubs), sus relaciones (Links) y sus atributos (Satellites). |
| **Raw Vault** | Primera capa del Data Vault 2.0 donde se almacenan los datos tal como llegan del origen, con las estructuras Hub/Link/Satellite pero sin transformaciones de negocio complejas. |
| **Hub** | Tabla del Data Vault que almacena las llaves de negocio únicas de una entidad. |
| **Link** | Tabla del Data Vault que materializa la relación entre dos o más Hubs. |
| **Satellite** | Tabla del Data Vault que almacena los atributos descriptivos de un Hub o Link, con historial de cambios. |
| **Modelo Estrella** | Esquema dimensional (Star Schema) compuesto por tablas de dimensiones y tablas de hechos, optimizado para consultas analíticas. |
| **Liquid Clustering** | Mecanismo de Delta Lake que reorganiza automáticamente los datos para optimizar las consultas basándose en columnas especificadas. |
| **Append Only** | Patrón de escritura donde nunca se modifican ni eliminan registros existentes; solo se agregan nuevos. |
| **AutoLoader** | Componente de Databricks (cloudFiles) que detecta y procesa incrementalmente archivos nuevos en un directorio de almacenamiento. |
| **Expectations** | Reglas de calidad de datos definidas en LSDP que validan condiciones sobre los registros durante el procesamiento del pipeline. |
| **Streaming Table** | Tabla gestionada por LSDP que soporta procesamiento incremental continuo de datos. |
| **Materialized View** | Vista persistida en LSDP cuyos resultados se almacenan físicamente y se actualizan automáticamente cuando cambian los datos fuente. |
| **EARS** | Easy Approach to Requirements Syntax — Formato estructurado para escribir requisitos verificables y testables. |
| **SDD** | Spec-Driven Development — Metodología donde las especificaciones formales guían todo el desarrollo (requirements → design → tasks → implementation). |
| **AI-DLC** | AI-Driven Development Lifecycle — Ciclo de vida de desarrollo impulsado por IA, estructurado en fases con compuertas de aprobación humana. |
| **cc-sdd** | Herramienta open-source (https://github.com/gotalab/cc-sdd) que implementa SDD para múltiples agentes de IA (Claude, Cursor, Copilot, etc.). |
| **Unity Catalog** | Sistema de gobernanza de datos de Databricks que proporciona gestión centralizada de metadatos, permisos y linaje. |
| **Landing Zone** | Directorio de almacenamiento donde se depositan los archivos de datos crudos (Parquets) antes de ser procesados por el pipeline. |

