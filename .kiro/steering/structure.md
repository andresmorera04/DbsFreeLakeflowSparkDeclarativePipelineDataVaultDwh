# Estructura del Proyecto

## Filosofía de Organización

El proyecto sigue una organización **por medallas** (Bronce → Plata → Oro), alineada con la Arquitectura Medallón de Databricks. Cada notebook en `transformations/` pertenece a una medalla y encapsula una unidad funcional del pipeline LSDP.

## Patrones de Directorios

### Transformaciones (Pipeline LSDP)
**Ubicación**: `src/LSDP_Lab_DataVault_DWH/transformations/`  
**Propósito**: Notebooks de producción que definen las tablas del pipeline declarativo.  
**Ejemplo**: `LSDPBronceCMSTFL.py`, `LSDPPlataHubCliente.py`, `LSDPOroDimCliente.py`

### Exploraciones
**Ubicación**: `src/LSDP_Lab_DataVault_DWH/explorations/`  
**Propósito**: Notebooks auxiliares de pruebas, validación SQL y generación de Parquets para la Landing Zone. **No son parte del pipeline LSDP.**  
**Ejemplo**: Consultas de validación sobre tablas, generadores de archivos Parquet.

### Utilidades
**Ubicación**: `src/LSDP_Lab_DataVault_DWH/utilities/`  
**Propósito**: Módulos Python puro reutilizables (no notebooks). Contienen parámetros, constantes y funciones helper.  
**Ejemplo**: `LSDPConfiguracion.py`, `LSDPUtilidadPrincipal.py`

### Documentación
**Ubicación**: `docs/`  
**Propósito**: Documentación técnica, modelo de datos final y guía de demostración.  
**Archivos**: `ManualTecnico.md`, `ModeloDatosFinal.md`, `Demo.md`

### Especificaciones SDD
**Ubicación**: `.kiro/specs/<feature-name>/`  
**Propósito**: Artefactos del flujo Spec-Driven Development (requirements, design, tasks por feature).

## Convenciones de Nombrado

### Archivos de Código

| Tipo | Patrón | Ejemplo |
|------|--------|---------|
| Notebooks de transformación | `LSDP{Medalla}{Nombre}` | `LSDPBronceCMSTFL`, `LSDPPlataHubCliente`, `LSDPOroFactTransaccionesATM` |
| Utilidades Python | `LSDP{NombreUtilidad}.py` | `LSDPConfiguracion.py` |
| Notebooks de exploración | `Nb{DescripcionOAccion}` | NbValidaciones, NbGeneradores |

### Objetos de Base de Datos (tablas en Unity Catalog)

| Medalla | Tipo | Patrón | Tipo LSDP | Ejemplo |
|---------|------|--------|-----------|---------|
| Bronce | Streaming Table persistente | `{Origen}` | `@dp.table()` | `CMSTFL`, `TRXPFL`, `BLNCFL` |
| Plata | Hub (OPT-001) | `Hub_{Entidad}` | `dp.create_streaming_table()` + `@dp.view` + `dp.create_auto_cdc_flow()` | `Hub_Cliente`, `Hub_Operacion` |
| Plata | Hub | `Hub_{Entidad}` | `dp.create_streaming_table()` + `@dp.append_flow()` | `Hub_Transaccion` |
| Plata | Link (OPT-001) | `Link_{Entidad1}_{Entidad2}` | `dp.create_streaming_table()` + `@dp.view` + `dp.create_auto_cdc_flow()` | `Link_Cliente_Operacion` |
| Plata | Link | `Link_{Entidad1}_{Entidad2}` | `dp.create_streaming_table()` + `@dp.append_flow()` | `Link_Cliente_Transaccion` |
| Plata | Satellite | `Sat_{Entidad}_{Concepto}` | `dp.create_streaming_table()` + `@dp.append_flow()` | `Sat_Cliente_DatosEstables`, `Sat_Cliente_Montos` |
| Oro | Dimensión | `Dim_{Nombre}` | Varies | `Dim_Cliente`, `Dim_Operacion`, `Dim_Tiempo` |
| Oro | Hecho | `Hec_{Nombre}` | Varies | `Hec_Transacciones_ATM` |

### Columnas Estándar por Tipo de Tabla

| Tabla | Columnas obligatorias |
|-------|-----------------------|
| Hub | `{LlaveNegocio}`, `Hash_{Hub}`, `FechaRegistro`, `FuenteDatos` |
| Link | `Hash_{Link}`, `Hash_{Hub1}`, `Hash_{Hub2}`, `FechaRegistro`, `FuenteDatos` |
| Satellite | `Hash_{HubOLink}`, `{Campos}`, `Hash_Diferenciador`, `FechaRegistro`, `FuenteDatos` |

## Organización de Imports

```python
# 1. LSDP framework
from pyspark import pipelines as dp

# 2. PySpark nativo
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 3. Parámetros del pipeline
catalogo = spark.conf.get("pipeline.catalogo")
```

## Principios de Organización del Código

- **Un notebook por unidad funcional**: Cada notebook de transformación encapsula una medalla + fuente/entidad.
- **Configuración centralizada**: Todos los parámetros, constantes y funciones helper viven en `utilities/LSDPConfiguracion.py`.
- **Sin valores hard-coded**: Catálogos, esquemas, rutas y umbrales de negocio se obtienen de parámetros del pipeline o constantes centralizadas.
- **Columnas de Bronce no se propagan**: `año`, `mes`, `dia`, `FechaRegistroParquet` y `_rescued_data` son exclusivas de Bronce.
- **Liquid Clustering**: Las columnas LC van en las primeras posiciones del esquema (helper `reordenar_columnas_lc()`).

---
_Documenta patrones, no árboles de archivos. Código nuevo que siga estos patrones no debería requerir actualizar este documento._
