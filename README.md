# LSDP Lab — Data Vault DWH on Databricks Free Edition

> Laboratorio de ingeniería de datos de extremo a extremo que demuestra la construcción
> de un Data Warehouse moderno sobre Databricks Free Edition con cómputo Serverless,
> combinando **Data Vault 2.0** como capa de integración y un **Modelo Estrella** como
> capa analítica, orquestado mediante **Lakeflow Spark Declarative Pipelines (LSDP)**.

![Platform](https://img.shields.io/badge/Platform-Databricks%20Free%20Edition-FF3621?style=flat-square)
![Compute](https://img.shields.io/badge/Compute-Serverless-0078D4?style=flat-square)
![Catalog](https://img.shields.io/badge/Catalog-Unity%20Catalog-1B6AC6?style=flat-square)
![Language](https://img.shields.io/badge/Language-PySpark%20%2F%20Python%203-3776AB?style=flat-square)
![Format](https://img.shields.io/badge/Storage-Delta%20Lake-00ADD8?style=flat-square)
![Methodology](https://img.shields.io/badge/Methodology-Data%20Vault%202.0-4CAF50?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)

---

## Tabla de contenido

- [Vision general](#vision-general)
- [Caso de uso](#caso-de-uso)
- [Arquitectura](#arquitectura)
  - [Medalla de Bronce — Ingesta incremental](#medalla-de-bronce--ingesta-incremental)
  - [Medalla de Plata — Data Vault 2.0 Raw Vault](#medalla-de-plata--data-vault-20-raw-vault)
  - [Medalla de Oro — Modelo Estrella](#medalla-de-oro--modelo-estrella)
- [Fuentes de datos](#fuentes-de-datos)
- [Stack tecnologico](#stack-tecnologico)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Requisitos previos](#requisitos-previos)
- [Inicio rapido](#inicio-rapido)
- [Configuracion del pipeline](#configuracion-del-pipeline)
- [Ejecucion](#ejecucion)
- [Pruebas](#pruebas)
- [Documentacion](#documentacion)
- [Decisiones de diseno](#decisiones-de-diseno)
- [Demo](#demo)
- [Contribuir](#contribuir)
- [Licencia](#licencia)

---

## Vision general

Este repositorio es un laboratorio de referencia para equipos de ingeniería de datos que
quieran implementar un Data Warehouse de producción sobre la plataforma Databricks Free
Edition sin incurrir en costos de cómputo dedicado. El proyecto resuelve los tres desafíos
centrales de cualquier pipeline de datos moderno:

**Ingesta confiable y escalable**: AutoLoader detecta y procesa automáticamente los archivos
Parquet nuevos depositados en un Volume de Unity Catalog, sin reprocesar datos históricos y
con inferencia evolutiva de esquema.

**Integridad histórica y auditabilidad**: Data Vault 2.0 preserva la historia completa de
cada entidad de negocio mediante tablas Hub, Link y Satellite con hashing deterministico
(SHA2-256 para llaves de negocio, SHA2-512 para detección de cambio). Ningún registro
existente es modificado ni eliminado — todo es Append-Only.

**Consumo analítico sin friccion**: El Modelo Estrella de la capa Oro expone dimensiones
Tipo 1 y una tabla de hechos lista para consultas SQL directas, dashboards o integración
con herramientas de BI, sin que el analista deba comprender la complejidad subyacente del
Raw Vault.

El pipeline completo opera en modo **Serverless** — no requiere la creación ni gestión de
clústeres dedicados, lo que lo hace totalmente reproducible en cualquier workspace
Databricks Free Edition.

---

## Caso de uso

El área de negocio de Clientes de una entidad bancaria necesita un producto de datos
analítico para monitorear el comportamiento transaccional de sus clientes en cajeros
automáticos (ATMs). Las preguntas de negocio que este laboratorio responde son:

- ¿Cuántos retiros (DATM) y depósitos (CATM) realiza cada cliente en ATMs por período?
- ¿Cuál es el monto promedio y total de transacciones ATM por cliente, segmento y región?
- ¿Cómo evoluciona el saldo de las cuentas a lo largo del tiempo?
- ¿Qué clientes presentan patrones atípicos en su uso de cajeros automáticos?

El pipeline procesa tres fuentes del sistema AS400 bancario (Maestro de Clientes, Saldos y
Transacciones), las integra a través de Data Vault 2.0 y expone el resultado como un
Modelo Estrella consultable desde Databricks SQL o cualquier herramienta conectada a
Unity Catalog.

---

## Arquitectura

El pipeline sigue la **Arquitectura Medallón** en tres capas, implementada como un único
pipeline declarativo LSDP (Lakeflow Spark Declarative Pipelines):

```
                    Landing Zone
                  (Volume UC — Parquet)
                         |
          +--------------+--------------+
          |              |              |
        CMSTFL         TRXPFL        BLNCFL
     (4M clientes)  (7M transac.)  (4M saldos)
          |              |              |
          +--------------+--------------+
                         |
                         v
          +-----------------------------+
          |         BRONCE              |
          |   Streaming Tables (x3)     |
          |   Ingesta incremental       |
          |   AutoLoader + Delta Lake   |
          |   Liquid Clustering         |
          +-----------------------------+
                         |
                         v
          +-----------------------------+
          |          PLATA              |
          |   Data Vault 2.0 Raw Vault  |
          |                             |
          |   Hubs (x3)                 |
          |   Hub_Cliente               |
          |   Hub_Operacion             |
          |   Hub_Transaccion           |
          |                             |
          |   Links (x2)                |
          |   Link_Cliente_Operacion    |
          |   Link_Cliente_Transaccion  |
          |                             |
          |   Satellites (x9)           |
          |   Sat_Cliente_* (x3)        |
          |   Sat_Operacion_* (x3)      |
          |   Sat_Transaccion_* (x3)    |
          +-----------------------------+
                         |
                         v
          +-----------------------------+
          |           ORO               |
          |   Modelo Estrella           |
          |                             |
          |   Dim_Cliente               |
          |   Dim_Operacion             |
          |   Dim_Tiempo                |
          |   Hec_Transacciones_ATM     |
          +-----------------------------+
                         |
                         v
              Unity Catalog / Databricks SQL
              Dashboards / Herramientas BI
```

El pipeline se declara en `src/LSDP_Lab_DataVault_DWH/transformations/` y se ejecuta como
un pipeline Lakeflow Spark Declarative Pipelines en modo **Triggered** (recomendado para laboratorio) o
**Continuous**. Todos los parámetros de catálogo, esquema, rutas y ubicaciones de schema
se inyectan vía `Advanced → Configuration` del pipeline — el código no contiene valores
hard-coded.

---

### Medalla de Bronce — Ingesta incremental

Tres **Streaming Tables persistentes** (una por fuente de datos), creadas con `@dp.table`
sin `temporary=True`, que acumulan la historia completa de forma incremental:

| Tabla | Fuente AS400 | Descripcion | Columnas | Registros aprox. |
|-------|-------------|-------------|----------|------------------|
| `CMSTFL` | Maestro de Clientes | Datos demográficos y financieros del cliente | 70 (41 String, 18 Date, 9 Long, 2 Double) | 4,000,000 |
| `TRXPFL` | Transacciones | Historial de movimientos bancarios | 60 (7 String, 19 Date, 2 Timestamp, 2 Long, 30 Double) | 7,000,000 |
| `BLNCFL` | Saldos/Operaciones | Cuentas y saldos por cliente | 100 (2 Long, 29 String, 34 Double, 35 Date) | 4,000,000 |

**Caracteristicas tecnicas**:

- Mecanismo: AutoLoader (`cloudFiles`) con inferencia evolutiva de esquema
- Particionamiento fisico: `año=YYYY/mes=MM/dia=DD/` — Spark infiere las columnas de particion via lazy evaluation
- Columna derivada: `FechaRegistroParquet` (DATE desde las columnas de particion)
- Columna `_rescued_data`: captura automatica de campos que no coinciden con el esquema
- Liquid Clustering: exclusivamente sobre `FechaRegistroParquet`
- Las columnas `año`, `mes`, `dia`, `FechaRegistroParquet` y `_rescued_data` son exclusivas de Bronce y **no se propagan a capas superiores**

---

### Medalla de Plata — Data Vault 2.0 Raw Vault

14 entidades del Raw Vault implementadas como Streaming Tables Acumulativas con estrategias
diferenciadas de deduplicacion segun la cardinalidad y mutabilidad de cada entidad:

#### Hubs — Llaves de negocio

| Tabla | Llave de negocio | Hash | Estrategia LSDP |
|-------|-----------------|------|-----------------|
| `Hub_Cliente` | `CUSTID` | SHA2-256 | `dp.create_auto_cdc_flow(scd_type=1)` — OPT-001 |
| `Hub_Operacion` | `CUSTID` + `BLSQ` | SHA2-256 llave compuesta | `dp.create_auto_cdc_flow(scd_type=1)` — OPT-001 |
| `Hub_Transaccion` | `TRXID` | SHA2-256 | `@dp.append_flow` + `procesar_hub()` LEFT ANTI JOIN |

**OPT-001**: Hub_Cliente y Hub_Operacion usan `dp.create_auto_cdc_flow(stored_as_scd_type=1)`
alimentado por un `@dp.view`. El motor LSDP gestiona un MERGE cross-batch con coste
O(delta del microbatch) — sin full scan del Hub historico. `FechaRegistro` se actualiza
en cada MERGE (semantica "ultima vez vista").

#### Links — Relaciones entre entidades

| Tabla | Entidades relacionadas | Estrategia LSDP |
|-------|------------------------|-----------------|
| `Link_Cliente_Operacion` | Hub_Cliente ↔ Hub_Operacion | `dp.create_auto_cdc_flow(scd_type=1)` — OPT-001 |
| `Link_Cliente_Transaccion` | Hub_Cliente ↔ Hub_Transaccion | `@dp.append_flow` + `procesar_link()` LEFT ANTI JOIN |

#### Satellites — Atributos por tasa de cambio

Los Satellites se organizan siguiendo el principio de **separacion por tasa de cambio**:
atributos estables en un Satellite y atributos variables agrupados por concepto funcional
(montos, fechas variables) en Satellites separados. Cada Satellite incluye `Hash_Diferenciador`
(SHA2-512) para deteccion de cambio fila a fila.

| Satellite | Hub/Link padre | Descripcion |
|-----------|---------------|-------------|
| `Sat_Cliente_DatosEstables` | Hub_Cliente | Atributos demograficos de baja frecuencia de cambio |
| `Sat_Cliente_Montos` | Hub_Cliente | Valores monetarios y limites del cliente |
| `Sat_Cliente_FechasVariantes` | Hub_Cliente | Fechas de actualizacion frecuente |
| `Sat_Operacion_DatosEstables` | Hub_Operacion | Atributos de cuenta de baja variabilidad |
| `Sat_Operacion_Montos` | Hub_Operacion | Saldos, limites y valores financieros de la cuenta |
| `Sat_Operacion_FechasVariantes` | Hub_Operacion | Fechas operativas de actualizacion frecuente |
| `Sat_Transaccion_DatosEstables` | Hub_Transaccion | Tipo de transaccion, canal, estado |
| `Sat_Transaccion_Montos` | Hub_Transaccion | Importes, cargos y saldos post-transaccion |
| `Sat_Transaccion_FechasVariantes` | Hub_Transaccion | Timestamps de autorizacion y liquidacion |

**Estrategia de deduplicacion en Satellites**:

- Satellites de estado (Cliente, Operacion): `procesar_satellite()` con LEFT JOIN sobre
  `Hash_Diferenciador` — solo inserta registros donde el hash cambio o la entidad no existe.
  ROW_NUMBER para obtener el estado actual por entidad.
- Satellites transaccionales (Transaccion): `@dp.append_flow` puro — LEFT ANTI JOIN por
  `(hash_col, fecha_transaccion)`. TRXID es globalmente unico entre ejecuciones por diseno;
  no se compara `Hash_Diferenciador` en el join.

**Estructura estandar de columnas** (todas las tablas Plata):

| Columna | Tipo | Descripcion |
|---------|------|-------------|
| `Hash_{Entidad}` | STRING | SHA2-256 sobre llave(s) de negocio |
| `{Atributos de negocio}` | Tipo original | Variables del origen |
| `Hash_Diferenciador` | STRING | SHA2-512 sobre todos los atributos — detecta cambios |
| `FechaRegistro` | TIMESTAMP | Load Date en Data Vault 2.0 |
| `FuenteDatos` | STRING | Nombre de tres partes `catalogo.esquema.tabla` |

Liquid Clustering en todas las entidades: `FechaRegistro`, `Hash_{Entidad}`.

---

### Medalla de Oro — Modelo Estrella

Cuatro entidades analiticas construidas a partir del Raw Vault de Plata:

| Entidad | Tipo LSDP | Tipo dimensional | Fuente (Plata) |
|---------|-----------|------------------|----------------|
| `Dim_Cliente` | Materialized View | Dimension Tipo 1 | Hub_Cliente + Sat_Cliente_* |
| `Dim_Operacion` | Materialized View | Dimension Tipo 1 | Hub_Operacion + Sat_Operacion_* |
| `Dim_Tiempo` | Materialized View incremental | Dimension de fecha — granularidad diaria | `Sat_Transaccion_Montos.fecha_transaccion` |
| `Hec_Transacciones_ATM` | Materialized View | Tabla de hechos | `Trx_ATM_Stream` (ST `temporary=True`) + `Map_Cliente_Operacion_Dominante` (MV `temporary=True`) |

`Dim_Tiempo` se construye exclusivamente a partir de las fechas de transaccion presentes en
`Sat_Transaccion_Montos`. Cada vez que aparecen nuevas fechas, el motor las incorpora en el
siguiente refresh — sin logica imperativa de fechas.

`Hec_Transacciones_ATM` registra unicamente transacciones `DATM` (retiro ATM) y `CATM`
(deposito ATM). Las FK `DimIdCliente` y `DimIdOperacion` se pre-resuelven en la Streaming
Table temporal `Trx_ATM_Stream`, liberando al plan de la Materialized View de joins y
habilitando refresh incremental via CDF.

---

## Fuentes de datos

Los datos de origen son archivos Parquet generados sinteticamente con el mismo esquema
del sistema AS400 bancario. El proyecto incluye notebooks generadores de datos:

| Fuente | Entidad | Llave primaria | Registros sinteticos |
|--------|---------|----------------|----------------------|
| `CMSTFL` | Maestro de Clientes | `CUSTID` | Configurable (default 50,000) |
| `TRXPFL` | Transacciones bancarias | `TRXID` | Configurable (default 150,000) |
| `BLNCFL` | Saldos y operaciones | `CUSTID` + `BLSQ` | 1:1 con CMSTFL |

Los archivos se depositan en un Volume de Unity Catalog con particionamiento fisico por
fecha (`año/mes/dia`) y son detectados automaticamente por AutoLoader en la siguiente
ejecucion del pipeline.

> Para entornos de produccion, reemplaza los notebooks generadores con el proceso de
> extraccion de los sistemas AS400 reales y deposita los Parquet en las mismas rutas.

---

## Stack tecnologico

| Componente | Tecnologia | Notas |
|------------|------------|-------|
| Plataforma | Databricks Free Edition | Sin costo de infraestructura dedicada |
| Computo | Serverless Compute | Sin gestion de clusters |
| Catalogo de datos | Unity Catalog | Metadatos, permisos y linaje centralizados |
| Motor de pipeline | Lakeflow Spark Declarative Pipelines (LSDP) | Motor declarativo nativo de Databricks |
| Ingesta | AutoLoader (`cloudFiles`) | Deteccion incremental, inferencia evolutiva de esquema |
| Formato de almacenamiento | Delta Lake | ACID, time travel, liquid clustering |
| Lenguaje | Python 3 / PySpark | Sin UDFs — solo funciones nativas `pyspark.sql.functions` |
| Modelado | Data Vault 2.0 + Modelo Estrella | Hash SHA2-256 (llaves) · SHA2-512 (diferenciador) |
| Orquestacion | Lakeflow Jobs | Scheduling del pipeline |
| Metodologia de desarrollo | Kiro-style Spec-Driven Development (cc-sdd) | Ciclo Requirements → Design → Tasks → Implementation |
| Tests | pytest + PySpark local | Sin necesidad de cluster Databricks para correr tests |

---

## Estructura del repositorio

```
DbsFreeLakeflowSparkDeclarativePipelineDataVaultDwh/
|
+-- src/
|   +-- LSDP_Lab_DataVault_DWH/
|       |
|       +-- transformations/              # Pipeline LSDP — todos los notebooks del pipeline
|       |   +-- LSDPBronceCMSTFL.py       # ST Bronce: Maestro de Clientes (AutoLoader)
|       |   +-- LSDPBronceTRXPFL.py       # ST Bronce: Transacciones (AutoLoader)
|       |   +-- LSDPBronceBLNCFL.py       # ST Bronce: Saldos (AutoLoader)
|       |   +-- LSDPPlataHubCliente.py    # Hub_Cliente (OPT-001: auto_cdc_flow)
|       |   +-- LSDPPlataHubOperacion.py  # Hub_Operacion (OPT-001: auto_cdc_flow)
|       |   +-- LSDPPlataHubTransaccion.py # Hub_Transaccion (append_flow)
|       |   +-- LSDPPlataLinkClienteOperacion.py   # Link (OPT-001: auto_cdc_flow)
|       |   +-- LSDPPlataLinkClienteTransaccion.py # Link (append_flow)
|       |   +-- LSDPPlataSatCliente.py     # Satellites de Cliente (x3)
|       |   +-- LSDPPlataSatOperacion.py   # Satellites de Operacion (x3)
|       |   +-- LSDPPlataSatTransaccion.py # Satellites de Transaccion (x3)
|       |   +-- LSDPPlataVistaTRXPFLCDF.py # Vista CDF sobre TRXPFL
|       |   +-- LSDPOroDimCliente.py       # Dimension Cliente (MV Tipo 1)
|       |   +-- LSDPOroDimOperacion.py     # Dimension Operacion (MV Tipo 1)
|       |   +-- LSDPOroDimTiempo.py        # Dimension Tiempo (MV incremental)
|       |   +-- LSDPOroHecTransaccionesATM.py    # Hechos ATM (MV)
|       |   +-- LSDPOroTrxATMEnriquecida.py      # ST temporal pre-calculo FK
|       |   +-- LSDPOroMapClienteOperacionDominante.py # MV temporal mapa FK
|       |
|       +-- explorations/
|       |   +-- GenerarParquets/           # Notebooks generadores de datos sinteticos
|       |   |   +-- NbConfiguracionInicial.py     # Crea catalogos, esquemas y Volume UC
|       |   |   +-- NbGenerarMaestroCliente.py    # Genera CMSTFL (primera ejecucion + mutacion)
|       |   |   +-- NbGenerarSaldosCliente.py     # Genera BLNCFL (1:1 con CMSTFL)
|       |   |   +-- NbGenerarTransaccionalCliente.py # Genera TRXPFL
|       |   +-- Metadata/
|       |       +-- NbComentariosTablas.py # Aplica COMMENT en Unity Catalog (idempotente)
|       |
|       +-- utilities/                    # Modulos Python reutilizables (no notebooks)
|           +-- LSDPConfiguracion.py      # Parametros y constantes del pipeline
|           +-- LSDPUtilidadPrincipal.py  # procesar_hub(), procesar_link(), procesar_satellite()
|           +-- LSDPUtilidadOro.py        # Funciones helper para la capa Oro
|
+-- tests/                               # Tests unitarios con PySpark local
|   +-- test_configuracion.py
|   +-- test_documentacion.py
|   +-- test_notebooks_bronce.py
|   +-- test_notebooks_exploracion.py
|   +-- test_notebooks_oro.py
|   +-- test_notebooks_plata.py
|   +-- test_utilidad_oro.py
|   +-- test_utilidad_principal.py
|   +-- test_utilidades_plata.py
|
+-- docs/                                # Documentacion tecnica y operativa
|   +-- Quickstart.md                   # Guia paso a paso para reproducir el laboratorio
|   +-- ManualTecnico.md                # Patrones LSDP, restricciones Serverless, hashing
|   +-- ModeloDatos.md                  # Catalogo exhaustivo de tablas y columnas + Mermaid
|
+-- SYSTEM.md                           # Fuente de verdad centralizada (alimenta cc-sdd)
+-- AGENTS.md                           # Configuracion del flujo AI-DLC (Kiro)
+-- README.md                           # Este archivo
|
+-- .kiro/
    +-- steering/
    |   +-- product.md                  # Vision, capacidades y caso de uso del producto
    |   +-- tech.md                     # Stack tecnico, restricciones Serverless, patrones
    |   +-- structure.md                # Convenios de nombrado y organizacion del proyecto
    +-- specs/
        +-- documentacion-consolidada-y-metadata/  # Spec activo
        +-- correccion-arquitectura-bronce-plata/  # Spec historico
        +-- oro-modelo-estrella-mv-tiempo/          # Spec historico
```

---

## Requisitos previos

Antes de comenzar, verifica que tienes acceso a los siguientes recursos:

| Requisito | Detalle |
|-----------|---------|
| Workspace Databricks Free Edition | Con Serverless habilitado |
| Unity Catalog | Metastore configurado en el workspace |
| Tres catalogos UC | Uno para Bronce, uno para Plata y uno para Oro |
| Un Volume UC | Para la Landing Zone de archivos Parquet |
| Cuenta Git | GitHub, GitLab o Bitbucket con acceso al repositorio |
| Permisos UC | `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME` |

No se requiere instalar ningun software local. Todo el codigo se ejecuta en el workspace
de Databricks Free Edition desde Git Folders.

---

## Inicio rapido

La guia completa con todos los parametros documentados esta en
[docs/Quickstart.md](docs/Quickstart.md). A continuacion el flujo minimo:

**Paso 1 — Clonar el repositorio como Git Folder**

En el workspace de Databricks: `Workspace → Add → Git Folder`

```
https://github.com/andresmorera04/DbsFreeLakeflowSparkDeclarativePipelineDataVaultDwh.git
```

Selecciona la rama `main` y confirma con **Create Git Folder**.

**Paso 2 — Crear la infraestructura base**

Desde un cluster interactivo, ejecuta:

```
src/LSDP_Lab_DataVault_DWH/explorations/GenerarParquets/NbConfiguracionInicial.py
```

Configura los widgets: `catalogoParametro=control`, `esquemaParametro=lab1`,
`tablaParametros=Parametros`.

**Paso 3 — Generar datos sinteticos** (si no tienes datos reales)

Ejecuta en secuencia — los dos ultimos pueden ejecutarse en paralelo:

```
NbGenerarMaestroCliente.py      # Genera CMSTFL
NbGenerarSaldosCliente.py       # Genera BLNCFL  \  en paralelo
NbGenerarTransaccionalCliente.py # Genera TRXPFL  /
```

**Paso 4 — Crear el pipeline LSDP**

En Databricks: `Workflows → Lakeflow Spark Declarative Pipelines → Create Pipeline`

- Pipeline name: `LSDP_Lab_DataVault_DWH`
- Product edition: Core
- Serverless: habilitado
- Source code: todos los archivos en `src/LSDP_Lab_DataVault_DWH/transformations/*.py`

**Paso 5 — Configurar los 13 parametros del pipeline**

En `Advanced → Configuration`, agrega los 13 pares `clave = valor` descritos en la
seccion siguiente.

**Paso 6 — Ejecutar el pipeline**

Primera carga: `Start → Full Refresh`. Ejecuciones posteriores: `Start → Triggered`.

---

## Configuracion del pipeline

Todos los parametros se configuran en `Advanced → Configuration` del pipeline LSDP.
Ninguno tiene valor hard-coded en el codigo:

| Parametro | Valor ejemplo | Descripcion |
|-----------|--------------|-------------|
| `pipeline.catalogo` | `lsdp_bronce` | Catalogo Unity Catalog para la capa Bronce |
| `pipeline.esquema` | `lab_dwh` | Esquema de la capa Bronce |
| `pipeline.volumen` | `landing_zone` | Nombre del Volume UC para la Landing Zone |
| `pipeline.catalogo_plata` | `lsdp_plata` | Catalogo Unity Catalog para la capa Plata |
| `pipeline.esquema_plata` | `lab_dwh` | Esquema de la capa Plata |
| `pipeline.catalogo_oro` | `lsdp_oro` | Catalogo Unity Catalog para la capa Oro |
| `pipeline.esquema_oro` | `lab_dwh` | Esquema de la capa Oro |
| `pipeline.ruta_cmstfl` | `origenes/cmstfl` | Ruta relativa al Volume para archivos CMSTFL |
| `pipeline.ruta_trxpfl` | `origenes/trxpfl` | Ruta relativa al Volume para archivos TRXPFL |
| `pipeline.ruta_blncfl` | `origenes/blncfl` | Ruta relativa al Volume para archivos BLNCFL |
| `pipeline.schema_location_cmstfl` | `_schema/cmstfl` | Directorio AutoLoader para inferencia de schema CMSTFL |
| `pipeline.schema_location_trxpfl` | `_schema/trxpfl` | Directorio AutoLoader para inferencia de schema TRXPFL |
| `pipeline.schema_location_blncfl` | `_schema/blncfl` | Directorio AutoLoader para inferencia de schema BLNCFL |

Las rutas `ruta_*` y `schema_location_*` son relativas al Volume UC. La ruta absoluta
construida por el codigo es:

```
/Volumes/{pipeline.catalogo}/{pipeline.esquema}/{pipeline.volumen}/{ruta_*}/
```

---

## Ejecucion

### Primera ejecucion (Full Refresh)

Carga todos los datos historicos disponibles en la Landing Zone:

```
Panel del pipeline → Start → Full Refresh
```

El DAG mostrara las 21 entidades en orden de dependencia. Tiempo estimado: 15–45 minutos
segun el volumen de datos y la capacidad Serverless disponible.

### Ejecuciones incrementales

Una vez completada la primera carga, cada ejecucion subsiguiente procesa unicamente los
archivos nuevos depositados en el Volume UC desde la ultima ejecucion:

1. Deposita los nuevos Parquet en las rutas configuradas
2. `Start → Triggered`

AutoLoader detecta los archivos nuevos mediante su mecanismo de checkpoint — no
reprocesa archivos ya cargados.

### Criterios de verificacion de exito

Tras una ejecucion exitosa, valida con las siguientes consultas en Databricks SQL:

```sql
-- Bronce: 3 tablas con datos
SELECT COUNT(*) FROM lsdp_bronce.lab_dwh.CMSTFL;       -- debe ser > 0
SELECT COUNT(*) FROM lsdp_bronce.lab_dwh.TRXPFL;       -- debe ser > 0
SELECT COUNT(*) FROM lsdp_bronce.lab_dwh.BLNCFL;       -- debe ser > 0

-- Plata: integridad del Raw Vault
SELECT COUNT(*) FROM lsdp_plata.lab_dwh.Hub_Cliente;   -- debe ser > 0

-- Oro: modelo estrella sin FK nulas
SELECT COUNT(*) FROM lsdp_oro.lab_dwh.Hec_Transacciones_ATM
WHERE DimIdCliente IS NULL;                            -- debe ser 0

-- Oro: solo transacciones ATM
SELECT DISTINCT TipoTransaccion
FROM lsdp_oro.lab_dwh.Hec_Transacciones_ATM;          -- solo DATM y CATM

-- Oro: consulta analitica de referencia
SELECT
    d.NombreMes, d.Anio,
    COUNT(*)                AS TotalTransacciones,
    SUM(h.MontoPrincipal)   AS MontoTotal,
    COUNT(DISTINCT h.DimIdCliente) AS ClientesUnicos
FROM lsdp_oro.lab_dwh.Hec_Transacciones_ATM h
JOIN lsdp_oro.lab_dwh.Dim_Tiempo d ON h.FechaClave = d.FechaClave
GROUP BY d.NombreMes, d.Anio
ORDER BY d.Anio, MIN(d.Mes);
```

---

## Pruebas

El proyecto incluye una suite de tests unitarios que se ejecutan con PySpark local, sin
necesidad de un cluster Databricks. Los tests validan la logica de transformacion de las
utilidades y la estructura de los notebooks:

```bash
# Instalar dependencias
pip install pytest pyspark

# Ejecutar toda la suite
pytest tests/ -v

# Ejecutar por modulo
pytest tests/test_utilidad_principal.py -v   # Funciones procesar_hub, link, satellite
pytest tests/test_utilidad_oro.py -v          # Funciones helper de la capa Oro
pytest tests/test_notebooks_bronce.py -v      # Validacion estructura Bronce
pytest tests/test_notebooks_plata.py -v       # Validacion estructura Plata
pytest tests/test_notebooks_oro.py -v         # Validacion estructura Oro
```

Los tests de notebooks validan: presencia de todos los widgets declarados, nombre de tablas
en formato de tres partes, ausencia de valores hard-coded y cumplimiento de las restricciones
Serverless (sin `.cache()`, sin RDD, sin UDFs).

---

## Documentacion

| Documento | Proposito |
|-----------|-----------|
| [SYSTEM.md](SYSTEM.md) | Fuente de verdad centralizada del proyecto. Define la arquitectura completa, las reglas de modelado Data Vault 2.0, las decisiones de diseno con justificacion tecnica, las restricciones de Serverless y los patrones LSDP aplicados. Es el documento primario que alimenta el flujo Spec-Driven Development: los comandos `/kiro-steering` y `/kiro-spec-init` lo consumen como entrada obligatoria para generar artefactos de alta precision. |
| [docs/Quickstart.md](docs/Quickstart.md) | Guia paso a paso para reproducir el laboratorio desde cero en Databricks Free Edition. Cubre prerequisitos, clonado como Git Folder, generacion de datos sinteticos con todos los widgets documentados (nombre, valor por defecto y descripcion de proposito), creacion y configuracion del pipeline con los 13 parametros, ejecucion y criterios de verificacion de exito. Incluye seccion de videos de demo por etapa. |
| [docs/ManualTecnico.md](docs/ManualTecnico.md) | Manual de referencia para ingenieros que mantienen o extienden el pipeline. Explica en profundidad el uso de `dp.create_auto_cdc_flow`, `@dp.append_flow`, Streaming Tables temporales, Materialized Views, patrones de hash SHA2-256/512 con separador pipe, restricciones de Serverless (sin `cache`, sin RDD, sin UDFs, sin threading) y reglas ANSI Mode (`cast` a `long` antes de `abs`, `F.concat_ws` en lugar de `+` para strings). |
| [docs/ModeloDatos.md](docs/ModeloDatos.md) | Catalogo exhaustivo de todas las tablas y columnas del pipeline organizado por medalla. Por cada entidad incluye: tipo de tabla LSDP, diagrama relacional Mermaid con cardinalidades, catalogo completo de columnas con tipo de dato, descripcion de negocio, origen y clasificacion (PK, FK, hash, columna tecnica Data Vault). Incluye el diagrama de linaje macro Bronce → Plata → Oro. |

---

## Decisiones de diseno

Las decisiones tecnicas mas relevantes adoptadas en este proyecto:

**OPT-001 — AUTO CDC en lugar de full scan para Hub_Cliente, Hub_Operacion y
Link_Cliente_Operacion**: Las entidades de alta cardinalidad que requieren deduplicacion
cross-batch utilizan `dp.create_auto_cdc_flow(stored_as_scd_type=1)` con un `@dp.view`
como fuente. El motor LSDP gestiona un MERGE con coste O(delta del microbatch), eliminando
el full scan O(historico) que la funcion `procesar_hub()` requeria en cada ejecucion.
`except_column_list` **no se usa** — en esta API el parametro excluye columnas del esquema
del target (no las protege de actualizaciones), lo que causa `DELTA_COLUMN_NOT_FOUND_IN_SCHEMA`
al referenciarlas en `cluster_by`.

**Arquitectura Bronce de una sola capa**: Se elimino el patron anterior de dos capas
(Streaming Table temporal + Materialized View con snapshot del ultimo dia). La nueva
arquitectura usa una ST persistente unica que acumula toda la historia. Plata consume
directamente via `dp.read_stream()` aplicando la logica de seleccion dentro de sus
propias transformaciones. Esto reduce el numero de tablas intermedias y elimina la
dependencia del snapshot diario.

**Hash_Diferenciador con SHA2-512**: Los Satellites usan SHA2-512 (en lugar de SHA2-256
usado en Hubs/Links) para el campo de deteccion de cambio. El algoritmo de mayor longitud
minimiza colisiones en el espacio combinado de todos los atributos del registro.

**Dim_Tiempo como Materialized View incremental**: La dimension de tiempo se construye
exclusivamente a partir de las fechas de transaccion presentes en `Sat_Transaccion_Montos`,
sin logica imperativa de fechas. Esto garantiza que la dimension contenga exactamente las
fechas del negocio sin depender de generadores de rangos ni tablas de calendario externas.

**Pre-resolucion de FK en ST temporal**: Las llaves foraneas `DimIdCliente` y
`DimIdOperacion` de la tabla de hechos se calculan en `Trx_ATM_Stream` (ST `temporary=True`),
no en la Materialized View final. Esto libera el plan de `Hec_Transacciones_ATM` de joins
y habilita el refresh incremental via CDF.

**Separacion de Satellites por tasa de cambio**: Los atributos de cada entidad se
distribuyen en tres Satellites (datos estables, montos, fechas variantes) siguiendo el
principio Data Vault 2.0 de tasa de cambio. Esto permite cargar solo el Satellite afectado
ante un cambio parcial, sin reescribir atributos inmutables.

---

## Demo

Videos de demostracion del laboratorio organizados por etapa de ejecucion:

| Etapa | Descripcion | Enlace |
|-------|-------------|--------|
| Configuracion inicial — Parte 1 | Clonado del repositorio, creacion de catalogos y Volume UC con `NbConfiguracionInicial.py` | [Ver video](https://drive.google.com/file/d/11XptallIxQa2tYLAypPOaOuHpSLdbMyL/view?usp=sharing) |
| Configuracion inicial — Parte 2 | Generacion de datos sinteticos (CMSTFL, BLNCFL, TRXPFL) con los notebooks generadores | [Ver video](https://drive.google.com/file/d/1FDpXZOHSmc2QeqsFKdkxO6_uHuh1-jQE/view?usp=sharing) |
| Ejecucion Bronce | Creacion del pipeline, configuracion de los 13 parametros y primera ejecucion — ingesta incremental AutoLoader sobre las tres Streaming Tables | [Ver video](https://drive.google.com/file/d/1ha3O5cH_z1v5u5qFhKHGp2h4V-o9QFQP/view?usp=sharing) |
| Ejecucion Plata | Transformacion del Raw Vault Data Vault 2.0 — Hubs, Links y Satellites con estrategias AUTO CDC y append flow; verificacion de integridad referencial y hash | [Ver video](https://drive.google.com/file/d/157gM0VXBWOq1WxiB_YAj2nIYyYfbwq_H/view?usp=sharing) |
| Ejecucion Oro | Materializacion del Modelo Estrella — dimensiones y tabla de hechos ATM con validacion de calidad (Expectations) y consulta analitica final | [Ver video](https://drive.google.com/file/d/101NrWq82VC0kXqKXBmUirBQPsx5p_Fa4/view?usp=sharing) |
| Segunda Ejecucion (Incremental) | Deposito de nuevos Parquets en el Volume UC y ejecucion incremental del pipeline — verificacion de deteccion de cambios en Satellites y propagacion al Modelo Estrella | [Ver video](https://drive.google.com/file/d/1Cun5SaIBa8VU-8-YJfkx5svH5t7Z1RwB/view?usp=sharing) |

---

## Contribuir

Las contribuciones son bienvenidas. Para proponer cambios:

1. Abre un issue describiendo el problema o la mejora propuesta
2. Haz fork del repositorio y crea una rama descriptiva (`feat/nombre-feature` o `fix/nombre-bug`)
3. Asegurate de que los tests pasen: `pytest tests/ -v`
4. Verifica que el codigo no introduce valores hard-coded ni viola las restricciones Serverless
5. Abre un Pull Request con una descripcion clara del cambio y su motivacion

Para cambios de arquitectura o modelo de datos, considera abrir un issue de discusion
primero antes de implementar.

---

## Licencia

Este proyecto se distribuye bajo licencia MIT. Consulta el archivo [LICENSE](LICENSE) para
los terminos completos.

---

_Mantenido por [andresmorera04](https://github.com/andresmorera04)_  
_Ultima actualizacion: 2026-05-02_


---

## Descripción del proyecto

El laboratorio abarca el ciclo completo de ingeniería de datos estructurado en tres capas
(Arquitectura Medallón):

**Bronce — Ingesta incremental**  
AutoLoader consume archivos Parquet depositados en un Volume de Unity Catalog y los
materializa como Streaming Tables persistentes. Cada tabla acumula la historia completa
con liquid clustering sobre `FechaRegistroParquet`.

**Plata — Data Vault 2.0 Raw Vault**  
Los datos de Bronce se transforman en 14 entidades del Raw Vault: 3 Hubs, 2 Links y
9 Satellites. Las entidades de alta cardinalidad (Hub_Cliente, Hub_Operacion,
Link_Cliente_Operacion) usan `dp.create_auto_cdc_flow(stored_as_scd_type=1)` para
garantizar deduplicación cross-batch O(delta) sin full scan. Los Satellites implementan
detección de cambio vía `Hash_Diferenciador` (SHA2-512).

**Oro — Modelo Estrella**  
Cuatro entidades analíticas: `Dim_Cliente`, `Dim_Operacion`, `Dim_Tiempo` y
`Hec_Transacciones_ATM`. La tabla de hechos se materializa como Materialized View filtrada
exclusivamente sobre transacciones ATM (DATM y CATM) con llaves subrogadas e integridad
referencial declarativa.

---

## Stack tecnológico

| Componente | Tecnología |
|------------|------------|
| Plataforma | Databricks Free Edition |
| Cómputo | Serverless Compute (sin clúster dedicado) |
| Catálogo | Unity Catalog |
| Motor de pipeline | Lakeflow Spark Declarative Pipelines (LSDP) |
| Ingesta | AutoLoader (`cloudFiles`) |
| Formato de almacenamiento | Delta Lake |
| Lenguaje | Python 3 / PySpark |
| Modelado de datos | Data Vault 2.0 + Modelo Estrella Dimensional |
| Hash de llaves | SHA2-256 (Hubs/Links) · SHA2-512 (Hash_Diferenciador) |
| CI / SDD | Kiro-style Spec-Driven Development (cc-sdd) |

---

## Estructura del repositorio

```
.
├── src/
│   └── LSDP_Lab_DataVault_DWH/
│       ├── explorations/
│       │   ├── GenerarParquets/          # Notebooks generadores de datos sintéticos
│       │   │   ├── NbConfiguracionInicial.py
│       │   │   ├── NbGenerarMaestroCliente.py
│       │   │   ├── NbGenerarSaldosCliente.py
│       │   │   └── NbGenerarTransaccionalCliente.py
│       │   └── Metadata/
│       │       └── NbComentariosTablas.py  # Aplica COMMENT en Unity Catalog
│       ├── transformations/              # Pipeline LSDP (Bronce -> Plata -> Oro)
│       │   ├── LSDPBronce*.py            # Ingesta AutoLoader
│       │   ├── LSDPPlata*.py             # Data Vault 2.0 Raw Vault
│       │   └── LSDPOro*.py               # Modelo Estrella
│       └── utilities/                   # Funciones compartidas del pipeline
├── tests/                               # Tests unitarios con PySpark local
├── docs/                                # Documentación técnica y operativa
│   ├── Quickstart.md
│   ├── ManualTecnico.md
│   └── ModeloDatos.md
├── SYSTEM.md                            # Fuente de verdad centralizada
└── .kiro/                               # Artefactos del flujo Spec-Driven Development
    ├── steering/
    └── specs/
```

---

## Arquitectura del pipeline

```
Landing Zone (Volume UC — Parquet particionado por fecha)
         |
         v
  [ BRONCE ]  Streaming Tables persistentes (AutoLoader)
         CMSTFL          TRXPFL          BLNCFL
         |
         v
  [ PLATA ]   Data Vault 2.0 — Raw Vault
    Hubs       Hub_Cliente · Hub_Operacion · Hub_Transaccion
    Links      Link_Cliente_Operacion · Link_Cliente_Transaccion
    Satellites Sat_Cliente_* (x3) · Sat_Operacion_* (x3) · Sat_Transaccion_* (x3)
         |
         v
  [ ORO ]     Modelo Estrella Dimensional
    Dimensiones  Dim_Cliente · Dim_Operacion · Dim_Tiempo
    Hechos       Hec_Transacciones_ATM
```

El pipeline se declara íntegramente en `src/LSDP_Lab_DataVault_DWH/transformations/` y se
ejecuta en Databricks como un pipeline Lakeflow Spark Declarative Pipelines en modo Triggered o Continuous.
Todos los parámetros de catálogo, esquema, rutas y ubicaciones de schema se inyectan
mediante la sección `Advanced → Configuration` del pipeline — sin valores hard-coded en
el código.

---

## Documentación

| Documento | Propósito |
|-----------|-----------|
| [SYSTEM.md](SYSTEM.md) | Fuente de verdad centralizada del proyecto. Describe la arquitectura completa, las reglas de modelado, las decisiones técnicas, las restricciones de Serverless y los patrones LSDP. Es el documento primario que alimenta el flujo Spec-Driven Development (cc-sdd): los comandos `/kiro-steering` y `/kiro-spec-init` lo consumen como entrada obligatoria para generar artefactos de alta precisión. |
| [docs/Quickstart.md](docs/Quickstart.md) | Guía paso a paso para reproducir el laboratorio desde cero en Databricks Free Edition. Cubre: prerequisitos, clonado como Git Folder, generación de datos sintéticos (con todos los widgets documentados), creación y configuración del pipeline, primera ejecución y criterios de verificación de éxito. Incluye una sección de vídeos de demo por etapa. |
| [docs/ManualTecnico.md](docs/ManualTecnico.md) | Manual de referencia para ingenieros que mantienen o extienden el pipeline. Explica en profundidad el uso de `dp.create_auto_cdc_flow`, `@dp.append_flow`, las Streaming Tables temporales, las Materialized Views, los patrones de hash SHA2-256/512, las restricciones de Serverless (sin `cache`, sin RDD, sin UDFs, sin threading) y las reglas ANSI Mode que aplican en el entorno de ejecución. |
| [docs/ModeloDatos.md](docs/ModeloDatos.md) | Catálogo exhaustivo de todas las tablas y columnas del pipeline, organizado por medalla (Bronce, Plata, Oro). Por cada entidad incluye: tipo de tabla LSDP, diagrama relacional Mermaid con cardinalidades, catálogo completo de columnas con tipo de dato, descripción de negocio, origen y clasificación (PK, FK, hash, columna técnica Data Vault). Incluye también el diagrama de linaje macro Bronce → Plata → Oro. |

---

## Inicio rápido

Consulta [docs/Quickstart.md](docs/Quickstart.md) para la guía completa. El flujo mínimo es:

1. Clonar el repositorio como Git Folder en tu workspace de Databricks
2. Ejecutar `NbConfiguracionInicial.py` para crear catálogos, esquemas y el Volume UC
3. Ejecutar los notebooks generadores de datos sintéticos (CMSTFL → BLNCFL + TRXPFL en paralelo)
4. Crear el pipeline LSDP apuntando a `src/LSDP_Lab_DataVault_DWH/transformations/*.py`
5. Configurar los 13 parámetros del pipeline en `Advanced → Configuration`
6. Ejecutar el pipeline con **Full Refresh** en la primera carga

---

## Requisitos de plataforma

- Workspace Databricks Free Edition con Serverless habilitado
- Unity Catalog activo con metastore configurado
- Tres catálogos creados: Bronce, Plata y Oro
- Un Volume UC para Landing Zone

No se requiere clúster dedicado. Toda la ejecución del pipeline corre sobre Serverless
Compute. Los notebooks de exploración y generación de datos sintéticos requieren un
clúster interactivo independiente al pipeline.

---

## Desarrollo y especificaciones

El proyecto sigue el framework **Spec-Driven Development (cc-sdd)** con un ciclo de
aprobación en tres fases: Requirements → Design → Tasks → Implementation.

Los artefactos de especificación se encuentran en `.kiro/specs/`. Los archivos de steering
(`product.md`, `tech.md`, `structure.md`) en `.kiro/steering/` actúan como contexto
persistente para todos los incrementos.

---

## Licencia

Repositorio de uso educativo y de laboratorio. Consulta el archivo `LICENSE` para los
términos de uso aplicables.
