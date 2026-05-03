# Quickstart — LSDP Lab DataVault DWH

**Proyecto**: LSDP Lab DataVault DWH  
**Autor**: LSDP Lab DataVault DWH  
**Fecha**: 2026-05-01  
**Spec activo**: [documentacion-consolidada-y-metadata](../.kiro/specs/documentacion-consolidada-y-metadata/spec.json)  
**Plataforma**: Databricks Free Edition · Serverless Compute · Unity Catalog

---

## Índice

1. [Prerequisitos](#1-prerequisitos)
2. [Clonar el repositorio como Git Folder](#2-clonar-el-repositorio-como-git-folder)
3. [Preparar el Landing Zone (Volume UC)](#3-preparar-el-landing-zone-volume-uc)
4. [Generar los datos sintéticos](#4-generar-los-datos-sintéticos)
5. [Crear el pipeline LSDP en Databricks](#5-crear-el-pipeline-lsdp-en-databricks)
6. [Configurar los 13 parámetros del pipeline](#6-configurar-los-13-parámetros-del-pipeline)
7. [Ejecutar el pipeline](#7-ejecutar-el-pipeline)
8. [Verificar ejecución exitosa](#8-verificar-ejecución-exitosa)

---

## 1. Prerequisitos

Antes de comenzar, asegúrate de contar con:

| Requisito | Detalle |
|-----------|---------|
| Workspace Databricks Free Edition | Serverless habilitado |
| Unity Catalog activo | Metastore configurado en el workspace |
| Tres catálogos creados | `{catalogo}` (Bronce), `{catalogo_plata}` (Plata), `{catalogo_oro}` (Oro) |
| Un Volume UC creado | `{catalogo}.{esquema}.{volumen}` — para Landing Zone |
| Acceso a Git Folder | Cuenta en GitHub/GitLab/Bitbucket con acceso al repositorio |

---

## 2. Clonar el repositorio como Git Folder

En Databricks Free Edition, el código se integra mediante **Git Folders** (sección Workspace del menú lateral).

**Pasos**:

1. Abrir el menú lateral → **Workspace**
2. Navegar al directorio donde quieras clonar (por ejemplo, `/Users/{tu_correo}/`)
3. Clic en **Add** → **Git Folder**
4. Ingresar la URL del repositorio:
   ```
   https://github.com/andresmorera04/DbsFreeLakeflowSparkDeclarativePipelineDataVaultDwh.git
   ```
5. Seleccionar rama `main`
6. Confirmar con **Create Git Folder**

La estructura del proyecto quedará disponible en el path del workspace seleccionado.

---

## 3. Preparar el Landing Zone (Volume UC)

Los Parquets de origen deben depositarse en el Volume UC con la siguiente estructura de
directorios (particionados por fecha):

```
/Volumes/{catalogo}/{esquema}/{volumen}/
├── {ruta_cmstfl}/
│   └── 2024/
│       └── 01/
│           └── 15/
│               └── cmstfl_20240115.parquet
├── {ruta_trxpfl}/
│   └── 2024/01/15/
│       └── trxpfl_20240115.parquet
└── {ruta_blncfl}/
    └── 2024/01/15/
        └── blncfl_20240115.parquet
```

> Si no tienes datos reales, usa los notebooks generadores en el paso siguiente.

---

## 4. Generar los datos sintéticos

El proyecto incluye notebooks generadores de datos sintéticos con volúmenes representativos.
Ejecutar en el siguiente orden desde un clúster interactivo (no el pipeline):

### 4.1 Inicialización del catálogo

```
src/LSDP_Lab_DataVault_DWH/explorations/GenerarParquets/NbConfiguracionInicial.py
```

Este notebook crea los catálogos, esquemas y el Volume UC si no existen, de forma
idempotente. Configura los widgets antes de ejecutarlo:

| Widget | Valor por defecto | Descripción |
|--------|-------------------|-------------|
| `catalogoParametro` | `control` | Catálogo de Unity Catalog donde se almacena la tabla centralizada de parámetros del proyecto |
| `esquemaParametro` | `lab1` | Esquema dentro del catálogo de control donde reside la tabla de parámetros |
| `tablaParametros` | `Parametros` | Nombre de la tabla Delta centralizada que contiene los 15 registros de configuración del proyecto |

> **Nota**: Los tres parámetros son obligatorios y no pueden dejarse vacíos. Al re-ejecutarse,
> el notebook produce el mismo resultado sin errores (comportamiento idempotente).

### 4.2 Generar Maestro de Clientes (CMSTFL)

```
src/LSDP_Lab_DataVault_DWH/explorations/GenerarParquets/NbGenerarMaestroCliente.py
```

Genera registros de clientes sintéticos (CMSTFL — 70 columnas: 41 String, 18 Date, 9 Long,
2 Double) con nombres hebreos, egipcios e ingleses y distribución demográfica realista.
Soporta primera ejecución (generación limpia) y re-ejecución (mutación + nuevos registros).

| Widget | Valor por defecto | Descripción |
|--------|-------------------|-------------|
| `catalogoParametro` | `control` | Catálogo donde reside la tabla de parámetros del proyecto |
| `esquemaParametro` | `lab1` | Esquema dentro del catálogo de control |
| `tablaParametros` | `Parametros` | Nombre de la tabla Delta de parámetros |
| `cantidadClientes` | `50000` | Número de registros de clientes a generar en la primera ejecución |
| `rutaRelativaMaestroCliente` | `LSDP_Base/As400/MaestroCliente/` | Ruta relativa dentro del Volume UC donde se escribe el Parquet de salida CMSTFL |
| `rutaMaestroClienteExistente` | *(vacío)* | Ruta del Parquet CMSTFL ya generado. Vacío indica primera ejecución; con valor activa el modo mutación |
| `porcentajeMutacion` | `0.20` | Fracción de registros a mutar en re-ejecución (0.0–1.0). Solo aplica cuando `rutaMaestroClienteExistente` no está vacío |
| `porcentajeNuevos` | `0.006` | Fracción de registros nuevos a agregar en re-ejecución respecto al total existente |
| `camposMutacion` | `CUSNM,CUSLN,CUSMD,CUSFN,CUSAD,CUSA2,CUSCT,CUSST,CUSZP,CUSPH,CUSMB,CUSEM,CUSMS,CUSOC,CUSED` | Lista separada por comas de los campos demográficos que se modifican durante la mutación |
| `montoMinimo` | `10` | Valor mínimo generado para columnas de tipo DoubleType (importes y valores financieros) |
| `montoMaximo` | `100000` | Valor máximo generado para columnas de tipo DoubleType |
| `numeroParticiones` | `8` | Número de particiones usadas en `coalesce()` al escribir el Parquet (afecta tamaño de archivos de salida) |
| `shufflePartitions` | `8` | Valor de `spark.sql.shuffle.partitions`; reduce overhead en clústeres Serverless de tamaño laboratorio |

### 4.3 Generar Saldos (BLNCFL)

```
src/LSDP_Lab_DataVault_DWH/explorations/GenerarParquets/NbGenerarSaldosCliente.py
```

Genera saldos bancarios sintéticos (BLNCFL — 100 columnas: 2 Long, 29 String, 34 Double,
35 Date) con relación 1:1 respecto al Maestro de Clientes. Requiere que el CMSTFL haya
sido generado previamente.

Distribución de tipos de cuenta: AHRO 40 %, CRTE 30 %, PRES 20 %, INVR 10 %.

| Widget | Valor por defecto | Descripción |
|--------|-------------------|-------------|
| `catalogoParametro` | `control` | Catálogo donde reside la tabla de parámetros del proyecto |
| `esquemaParametro` | `lab1` | Esquema dentro del catálogo de control |
| `tablaParametros` | `Parametros` | Nombre de la tabla Delta de parámetros |
| `rutaRelativaSaldoCliente` | `LSDP_Base/As400/SaldoCliente/` | Ruta relativa dentro del Volume UC donde se escribe el Parquet de salida BLNCFL |
| `rutaRelativaMaestroCliente` | `LSDP_Base/As400/MaestroCliente/` | Ruta relativa del Parquet CMSTFL existente del que se leen los CUSTID para garantizar integridad referencial |
| `montoMinimo` | `10` | Valor mínimo generado para columnas de tipo DoubleType (saldos, límites, tasas) |
| `montoMaximo` | `100000` | Valor máximo generado para columnas de tipo DoubleType |
| `numeroParticiones` | `8` | Número de particiones usadas en `coalesce()` al escribir el Parquet |
| `shufflePartitions` | `8` | Valor de `spark.sql.shuffle.partitions` para entornos Serverless de laboratorio |

### 4.4 Generar Transacciones (TRXPFL)

```
src/LSDP_Lab_DataVault_DWH/explorations/GenerarParquets/NbGenerarTransaccionalCliente.py
```

Genera transacciones bancarias sintéticas (TRXPFL — 60 columnas: 7 String, 19 Date,
2 Timestamp, 2 Long, 30 Double). Requiere el CMSTFL para garantizar integridad referencial
de CUSTID.

Distribución de tipos: alta (~60 %): CATM, DATM, CMPR, TINT, DPST · media (~30 %): PGSL,
TEXT, RTRO, PGSV, NMNA, INTR · baja (~10 %): ADSL, IMPT, DMCL, CMSN.

| Widget | Valor por defecto | Descripción |
|--------|-------------------|-------------|
| `catalogoParametro` | `control` | Catálogo donde reside la tabla de parámetros del proyecto |
| `esquemaParametro` | `lab1` | Esquema dentro del catálogo de control |
| `tablaParametros` | `Parametros` | Nombre de la tabla Delta de parámetros |
| `cantidadTransacciones` | `150000` | Número de transacciones a generar en la ejecución |
| `fechaTransaccion` | *(obligatorio, sin valor por defecto)* | Fecha de las transacciones en formato `YYYY-MM-DD`. Determina la partición física del Parquet de salida |
| `rutaRelativaTransaccional` | `LSDP_Base/As400/Transaccional/` | Ruta relativa dentro del Volume UC donde se escribe el Parquet de salida TRXPFL |
| `rutaRelativaMaestroCliente` | `LSDP_Base/As400/MaestroCliente/` | Ruta relativa del Parquet CMSTFL existente del que se leen los CUSTID para integridad referencial |
| `rutaRelativaParquetsExistentes` | *(vacío)* | Ruta de Parquets TRXPFL ya existentes para continuar la secuencia de IDs de transacción. Vacío indica que es el primer archivo (la secuencia empieza desde 1) |
| `montoMinimo` | `10` | Valor mínimo generado para columnas de tipo DoubleType (importes de transacción) |
| `montoMaximo` | `100000` | Valor máximo generado para columnas de tipo DoubleType |
| `numeroParticiones` | `8` | Número de particiones usadas en `coalesce()` al escribir el Parquet |
| `shufflePartitions` | `8` | Valor de `spark.sql.shuffle.partitions` para entornos Serverless de laboratorio |

> **Paralelismo**: los notebooks 4.3 y 4.4 son independientes entre sí y pueden ejecutarse
> en paralelo en dos pestañas distintas del workspace.

---

## 5. Crear el pipeline LSDP en Databricks

1. Ir a **Workflows** → **Delta Live Tables** → **Create Pipeline**
2. Configurar:
   - **Pipeline name**: `LSDP_Lab_DataVault_DWH`
   - **Product edition**: Core (incluido en Free Edition)
   - **Pipeline mode**: Triggered (recomendado para laboratorio)
   - **Serverless**: **Habilitado** ✓
   - **Source code**: Seleccionar todos los archivos en
     `src/LSDP_Lab_DataVault_DWH/transformations/*.py`
   - **Storage location**: vacío (Managed Storage por defecto en Unity Catalog)
   - **Target schema**: vacío (se configura por parámetros en el código)

---

## 6. Configurar los 13 parámetros del pipeline

En la configuración del pipeline, sección **Advanced → Configuration**, agregar cada par
`clave = valor`. Los valores ejemplo asumen catálogos `lsdp_bronce`, `lsdp_plata`, `lsdp_oro`:

| Parámetro | Valor ejemplo | Descripción |
|-----------|--------------|-------------|
| `pipeline.catalogo` | `lsdp_bronce` | Catálogo de Bronce en Unity Catalog |
| `pipeline.esquema` | `lab_dwh` | Esquema de Bronce |
| `pipeline.volumen` | `landing_zone` | Nombre del Volume UC para Landing Zone |
| `pipeline.catalogo_plata` | `lsdp_plata` | Catálogo de Plata (Data Vault 2.0) |
| `pipeline.esquema_plata` | `lab_dwh` | Esquema de Plata |
| `pipeline.catalogo_oro` | `lsdp_oro` | Catálogo de Oro (Modelo Estrella) |
| `pipeline.esquema_oro` | `lab_dwh` | Esquema de Oro |
| `pipeline.ruta_cmstfl` | `origenes/cmstfl` | Ruta relativa al Volume para CMSTFL |
| `pipeline.ruta_trxpfl` | `origenes/trxpfl` | Ruta relativa al Volume para TRXPFL |
| `pipeline.ruta_blncfl` | `origenes/blncfl` | Ruta relativa al Volume para BLNCFL |
| `pipeline.schema_location_cmstfl` | `_schema/cmstfl` | Directorio de inferencia de schema AutoLoader |
| `pipeline.schema_location_trxpfl` | `_schema/trxpfl` | Directorio de inferencia de schema AutoLoader |
| `pipeline.schema_location_blncfl` | `_schema/blncfl` | Directorio de inferencia de schema AutoLoader |

> **Nota**: Las rutas `ruta_*` y `schema_location_*` son relativas al Volume UC. La ruta
> completa que construye el código es:
> `/Volumes/{catalogo}/{esquema}/{volumen}/{ruta_*}/`

---

## 7. Ejecutar el pipeline

### Primera ejecución (Full Refresh)

La primera ejecución carga todos los datos históricos de la Landing Zone:

1. En la pantalla del pipeline → **Start** → **Full Refresh**
2. El DAG mostrará las 21 entidades en orden de dependencia
3. Tiempo estimado primera carga: 15–45 minutos (depende del volumen y Serverless capacity)

### Ejecuciones incrementales

Una vez completada la primera carga, las siguientes ejecuciones procesan solo los datos
nuevos en el Volume UC:

1. Depositar nuevos Parquets en las rutas configuradas
2. → **Start** → **Triggered** (el modo por defecto)

---

## 8. Verificar ejecución exitosa

### Criterios de verificación

| Check | Cómo verificar | Estado esperado |
|-------|---------------|-----------------|
| Pipeline completado | Panel Pipeline → estado `COMPLETED` | ✅ Verde |
| Data Quality sin FAIL | Tab "Data Quality" en el pipeline | 0 registros en FAIL |
| Bronce: 3 tablas | `SELECT COUNT(*) FROM lsdp_bronce.lab_dwh.CMSTFL` | > 0 |
| Plata: 14 entidades | `SELECT COUNT(*) FROM lsdp_plata.lab_dwh.Hub_Cliente` | > 0 |
| Oro: 4 tablas | `SELECT COUNT(*) FROM lsdp_oro.lab_dwh.Hec_Transacciones_ATM` | > 0 |
| Solo ATM en Hechos | `SELECT DISTINCT TipoTransaccion FROM lsdp_oro.lab_dwh.Hec_Transacciones_ATM` | Solo `DATM` y `CATM` |
| Dim_Tiempo generada | `SELECT MIN(FechaClave), MAX(FechaClave) FROM lsdp_oro.lab_dwh.Dim_Tiempo` | Rango de fechas coherente |
| DimIdCliente no nulo | `SELECT COUNT(*) FROM lsdp_oro.lab_dwh.Hec_Transacciones_ATM WHERE DimIdCliente IS NULL` | 0 |

### Consulta de validación rápida (Oro)

```sql
-- Resumen ejecutivo del modelo estrella
SELECT
    d.NombreMes,
    d.Anio,
    COUNT(*)          AS TotalTransacciones,
    SUM(h.MontoPrincipal) AS MontoTotal,
    COUNT(DISTINCT h.DimIdCliente) AS ClientesUnicos
FROM lsdp_oro.lab_dwh.Hec_Transacciones_ATM h
JOIN lsdp_oro.lab_dwh.Dim_Tiempo d ON h.FechaClave = d.FechaClave
GROUP BY d.NombreMes, d.Anio
ORDER BY d.Anio, MIN(d.Mes);
```

### Aplicar comentarios de Unity Catalog (opcional)

Ejecutar el notebook de metadatos para poblar los comentarios de tablas y columnas en
Unity Catalog:

```
src/LSDP_Lab_DataVault_DWH/explorations/Metadata/NbComentariosTablas.py
```

Configurar los widgets con los mismos valores de catálogo/esquema usados en el pipeline.
El notebook valida paridad con el modelo de datos antes de aplicar cualquier comentario.

---

## 9. Demo

A continuación se presentan los vídeos de demostración del laboratorio, organizados por
etapa de ejecución. Cada vídeo cubre en detalle la configuración, la ejecución y la
verificación de resultados de su medalla correspondiente.

| # | Etapa | Descripción | Enlace |
|---|-------|-------------|--------|
| 1 | Configuración inicial — Parte 1 | Clonado del repositorio como Git Folder, creación de catálogos y Volume UC con `NbConfiguracionInicial.py` | [Ver vídeo](https://drive.google.com/file/d/11XptallIxQa2tYLAypPOaOuHpSLdbMyL/view?usp=sharing) |
| 2 | Configuración inicial — Parte 2 | Generación de datos sintéticos con los notebooks generadores (CMSTFL, BLNCFL, TRXPFL) | [Ver vídeo](https://drive.google.com/file/d/1FDpXZOHSmc2QeqsFKdkxO6_uHuh1-jQE/view?usp=sharing) |
| 3 | Ejecución Bronce | Creación del pipeline LSDP, configuración de los 13 parámetros y primera ejecución del pipeline — ingesta incremental AutoLoader sobre las tres Streaming Tables de Bronce | [Ver vídeo](https://drive.google.com/file/d/1ha3O5cH_z1v5u5qFhKHGp2h4V-o9QFQP/view?usp=sharing) |
| 4 | Ejecución Plata | Transformación del Raw Vault Data Vault 2.0 — Hubs, Links y Satellites — con estrategias AUTO CDC SCD=1 y append flow; verificación de integridad referencial y hash | [Ver vídeo](https://drive.google.com/file/d/157gM0VXBWOq1WxiB_YAj2nIYyYfbwq_H/view?usp=sharing) |
| 5 | Ejecución Oro | Materialización del Modelo Estrella — Dim_Cliente, Dim_Operacion, Dim_Tiempo y Hec_Transacciones_ATM — con validación de calidad de datos (Expectations) y consulta analítica final | [Ver vídeo](https://drive.google.com/file/d/101NrWq82VC0kXqKXBmUirBQPsx5p_Fa4/view?usp=sharing) |
| 6 | Segunda Ejecución (Incremental) | Depósito de nuevos Parquets en el Volume UC y ejecución incremental del pipeline — verificación de detección de cambios en Satellites y propagación al Modelo Estrella | [Ver vídeo](https://drive.google.com/file/d/1Cun5SaIBa8VU-8-YJfkx5svH5t7Z1RwB/view?usp=sharing) |

---

## Documentación relacionada

- [Modelo de Datos](./ModeloDatos.md) — Catálogo exhaustivo de tablas y columnas
- [Manual Técnico](./ManualTecnico.md) — Arquitectura, patrones y decisiones técnicas
- [SYSTEM.md](../SYSTEM.md) — Fuente de verdad centralizada del proyecto

---

_Documento generado durante el incremento `documentacion-consolidada-y-metadata` · 2026-05-01_  
_Mantenido en: [docs/Quickstart.md](./Quickstart.md)_
