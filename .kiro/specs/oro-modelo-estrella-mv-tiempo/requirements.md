# Requirements Document

## Introduction

Este documento define los requisitos para construir la **Medalla de Oro** (Modelo Estrella / Data Warehouse dimensional) del laboratorio LSDP, consumiendo el Data Vault 2.0 de la Medalla de Plata. Introduce un **cambio arquitectónico obligatorio** respecto a la dimensión de tiempo: `Dim_Tiempo` deja de ser una Streaming Table acumulativa con `@dp.append_flow()` y pasa a ser una **Vista Materializada incremental** (`@dp.materialized_view`) cuya fuente de datos son los valores **distintos** del campo `FechaTransaccion` del Satellite `Sat_Transaccion_Montos`.

El alcance incluye:

- **Actualización previa y obligatoria de la documentación** (`SYSTEM.md` y archivos de steering/specs que referencien la arquitectura previa de `Dim_Tiempo`) para dejar el spec sin ambigüedades antes de iniciar el desarrollo.
- Construcción de las dimensiones `Dim_Cliente` (Tipo 1), `Dim_Operacion` (Tipo 1) y `Dim_Tiempo` (MV incremental).
- Construcción de la tabla de hechos `Hec_Transacciones_ATM` filtrada por transacciones `DATM` (retiros) y `CATM` (depósitos), con FKs a las tres dimensiones.
- Cumplimiento estricto de las restricciones de Databricks Free Edition Serverless Compute y de las recomendaciones de LSDP para refresh incremental de Vistas Materializadas.

Idioma del spec: `es` (todos los artefactos Markdown se redactan en español).

## Requirements

### Requirement 1: Actualización Obligatoria de SYSTEM.md y Documentación Asociada

**Objective:** As a Equipo de Spec-Driven Development, I want que `SYSTEM.md` y demás documentación del proyecto reflejen la nueva arquitectura de `Dim_Tiempo` como Vista Materializada incremental antes de iniciar el desarrollo, so that el spec quede limpio y sin ambigüedades para todas las fases posteriores del flujo cc-sdd.

#### Acceptance Criteria

1. The Documentation Update Process shall actualizar `SYSTEM.md` reemplazando toda mención de `Dim_Tiempo` como "Streaming Table acumulativa con Append Flow" por "Vista Materializada incremental basada en valores distintos de `Sat_Transaccion_Montos.FechaTransaccion`".
2. The Documentation Update Process shall reescribir el ejemplo de código LSDP de `Dim_Tiempo` en `SYSTEM.md` para usar `@dp.materialized_view` con `spark.read.table()` sobre `Sat_Transaccion_Montos`, eliminando `dp.create_streaming_table()`, `@dp.append_flow()`, `spark.range(0, 2)`, `F.current_date()` y `F.date_sub()`.
3. The Documentation Update Process shall actualizar la tabla "Compatibilidad con Databricks Free Edition" en `SYSTEM.md` removiendo la mención de `Dim_Tiempo` dentro de la fila de Streaming Tables y agregando una nota explícita de que en Oro `Dim_Tiempo` se implementa como Vista Materializada incremental.
4. The Documentation Update Process shall reescribir la "Regla especial para Dim_Tiempo" en `SYSTEM.md` para describir el nuevo comportamiento: la dimensión se materializa por LSDP a partir de los `FechaTransaccion` distintos del Satellite y se refresca incrementalmente sin lógica explícita de "ayer/hoy".
5. When existan archivos en `.kiro/steering/`, `docs/` u otros specs que mencionen la arquitectura previa de `Dim_Tiempo`, the Documentation Update Process shall actualizarlos para mantener coherencia con la nueva decisión arquitectónica.
6. If una sección de `SYSTEM.md` describe `Dim_Tiempo` con campos derivados (`Anio`, `Mes`, `Trimestre`, etc.), then the Documentation Update Process shall preservar dichos atributos en la nueva definición de la Vista Materializada.
7. The Documentation Update Process shall completarse y aprobarse antes de generar cualquier código de la Medalla de Oro.

### Requirement 2: Dim_Tiempo como Vista Materializada Incremental

**Objective:** As a Pipeline LSDP de la Medalla de Oro, I want exponer `Dim_Tiempo` como Vista Materializada cuyo refresh sea incremental, so that solo se procesen las fechas nuevas que aparecen en `Sat_Transaccion_Montos` sin reprocesar todo el histórico ni introducir lógica imperativa de control de fechas.

#### Acceptance Criteria

1. The Oro Pipeline shall declarar `Dim_Tiempo` con el decorador `@dp.materialized_view(name=f"{catalogo_oro}.{esquema_oro}.Dim_Tiempo", ...)` usando nombre de 3 partes en `name`.
2. The Oro Pipeline shall construir el conjunto base de fechas de `Dim_Tiempo` mediante `spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Transaccion_Montos").select("FechaTransaccion").distinct()`.
3. The Oro Pipeline shall renombrar la columna fuente `FechaTransaccion` a `FechaClave` (DateType) como llave primaria de la dimensión.
4. The Oro Pipeline shall derivar todos los atributos calendario (`Anio`, `Mes`, `Dia`, `Trimestre`, `Semestre`, `DiaSemana`, `NombreDia`, `NombreMes`, `EsFinSemana`, `DiaDelAnio`, `SemanaDelAnio`) usando exclusivamente funciones determinísticas nativas de `pyspark.sql.functions` aplicadas sobre `FechaClave`.
5. If el flujo necesita generar atributos derivados, then the Oro Pipeline shall NO usar funciones no determinísticas (`F.current_date()`, `F.current_timestamp()`, `F.now()`, `F.rand()`) ni UDFs en la definición de la Vista Materializada.
6. The Oro Pipeline shall configurar `cluster_by=["FechaClave"]` en la Vista Materializada `Dim_Tiempo`.
7. The Oro Pipeline shall garantizar que la Vista Materializada cumpla las recomendaciones de LSDP para incremental refresh: lectura batch (`spark.read.table`) de una tabla Delta soportada (`Sat_Transaccion_Montos`), uso únicamente de operadores compatibles (`select`, `distinct`, `withColumn` con expresiones determinísticas, `when/otherwise`), sin joins con tablas externas no soportadas y sin operaciones de streaming.
8. When LSDP refresque `Dim_Tiempo` y aparezcan nuevas fechas en `Sat_Transaccion_Montos`, the Oro Pipeline shall permitir que el motor recalcule incrementalmente solo las filas afectadas, sin requerir lógica adicional de "ayer/hoy" ni control manual de duplicados.
9. The Oro Pipeline shall validar la calidad de `Dim_Tiempo` con expectations: `FechaClave IS NOT NULL` (fail), `Mes BETWEEN 1 AND 12` (fail) y `Anio BETWEEN 1900 AND 2100` (warn — `expect`, no aborta el pipeline ante valores fuera de rango).

### Requirement 3: Dim_Cliente como Vista Materializada Tipo 1

**Objective:** As a Pipeline LSDP de la Medalla de Oro, I want exponer `Dim_Cliente` con los atributos vigentes de cada cliente, so that el modelo estrella ofrezca una visión actual y consolidada del cliente para el análisis dimensional.

#### Acceptance Criteria

1. The Oro Pipeline shall declarar `Dim_Cliente` como `@dp.materialized_view(name=f"{catalogo_oro}.{esquema_oro}.Dim_Cliente", ...)`.
2. The Oro Pipeline shall obtener el último estado de cada cliente combinando `Hub_Cliente` con los Satellites de Cliente (`Sat_Cliente_DatosEstables`, `Sat_Cliente_Contacto`, `Sat_Cliente_Clasificacion`, `Sat_Cliente_Financiero`) tomando el registro más reciente por `Hash_Cliente` (`ROW_NUMBER() OVER (PARTITION BY Hash_Cliente ORDER BY FechaRegistro DESC) = 1`).
3. The Oro Pipeline shall asignar la llave subrogada `DimIdCliente` (LongType) por orden de primera aparición del `Hash_Cliente`, garantizando estabilidad entre ejecuciones.
4. The Oro Pipeline shall incluir los atributos de negocio definidos en la sección de Oro de `SYSTEM.md` para `Dim_Cliente` (identidad, contacto, clasificación, financieros y campos calculados como `RangoEtario`, `CategoriaIngresos`).
5. The Oro Pipeline shall configurar `cluster_by=["DimIdCliente"]` en `Dim_Cliente`.
6. The Oro Pipeline shall validar con expectations: `DimIdCliente IS NOT NULL` (fail) y `Hash_Cliente IS NOT NULL` (fail).

### Requirement 4: Dim_Operacion como Vista Materializada Tipo 1

**Objective:** As a Pipeline LSDP de la Medalla de Oro, I want exponer `Dim_Operacion` con los atributos vigentes de cada operación/cuenta, so that el modelo estrella permita análisis por tipo de cuenta, moneda, producto y categoría de saldo.

#### Acceptance Criteria

1. The Oro Pipeline shall declarar `Dim_Operacion` como `@dp.materialized_view(name=f"{catalogo_oro}.{esquema_oro}.Dim_Operacion", ...)`.
2. The Oro Pipeline shall obtener el último estado de cada operación combinando `Hub_Operacion` con los Satellites de Operación (`Sat_Operacion_DatosEstables`, `Sat_Operacion_Montos`, `Sat_Operacion_FechasEvento`) tomando el registro más reciente por `Hash_Operacion`.
3. The Oro Pipeline shall asignar la llave subrogada `DimIdOperacion` (LongType) por orden de primera aparición del `Hash_Operacion`, manteniendo estabilidad entre ejecuciones.
4. The Oro Pipeline shall incluir los atributos definidos en la sección de Oro de `SYSTEM.md` para `Dim_Operacion` (tipo cuenta, moneda, estado, producto, riesgo, saldos, ratio, y campos calculados `CategoriaSaldo`, `EstadoUtilizacionCredito`, `IndicadorSobregiro`).
5. The Oro Pipeline shall configurar `cluster_by=["DimIdOperacion"]` en `Dim_Operacion`.
6. The Oro Pipeline shall validar con expectations: `DimIdOperacion IS NOT NULL` (fail) y `Hash_Operacion IS NOT NULL` (fail).

### Requirement 5: Hec_Transacciones_ATM como Tabla de Hechos

**Objective:** As a Pipeline LSDP de la Medalla de Oro, I want exponer `Hec_Transacciones_ATM` con las transacciones ATM (DATM y CATM) integradas con las tres dimensiones, so that el área de negocio pueda responder las preguntas analíticas sobre cantidad, monto y comportamiento transaccional en cajeros automáticos.

#### Acceptance Criteria

1. The Oro Pipeline shall declarar `Hec_Transacciones_ATM` como `@dp.materialized_view(name=f"{catalogo_oro}.{esquema_oro}.Hec_Transacciones_ATM", ...)`.
2. The Oro Pipeline shall filtrar las transacciones de `Sat_Transaccion_DatosEstables` para incluir únicamente registros con `tipo_transaccion IN (TIPO_DATM, TIPO_CATM)` usando las constantes centralizadas del notebook de configuración.
3. The Oro Pipeline shall integrar las medidas monetarias (`MontoPrincipal`, `ComisionTransaccion`, `TotalTransaccion`) desde `Sat_Transaccion_Montos` y los atributos categóricos (`MonedaTransaccion`, `EstadoTransaccion`, `CanalTransaccion`, `TipoTransaccion`) desde `Sat_Transaccion_DatosEstables`, tomando el último registro por `Hash_Transaccion` cuando aplique. _Nota de implementación (aprobada 2026-04-25): los Sats transaccionales son únicos por `Hash_Transaccion` por construcción de Plata (`procesar_satellite_transaccional`); la salvaguarda contractual de unicidad se implementa como `expect_or_drop` sobre la columna marcador `_marca_duplicado` en el hecho de Oro, NO como reducción al último registro vía `obtener_ultimo_por_hash` (ver `design.md` §NotebookHecTransaccionesATM)._
4. The Oro Pipeline shall obtener `FechaTransaccion` directamente de `Sat_Transaccion_Montos`, garantizando que cada valor exista en `Dim_Tiempo.FechaClave`.
5. The Oro Pipeline shall resolver `DimIdCliente` mediante join entre `Link_Cliente_Transaccion` y `Dim_Cliente` por `Hash_Cliente`.
6. The Oro Pipeline shall resolver `DimIdOperacion` transitivamente: a partir del `Hash_Cliente` de la transacción se busca la operación asociada vía `Link_Cliente_Operacion` y se obtiene `DimIdOperacion` desde `Dim_Operacion` por `Hash_Operacion`.
7. The Oro Pipeline shall derivar las banderas booleanas `EsRetiro = (TipoTransaccion == TIPO_DATM)` y `EsDeposito = (TipoTransaccion == TIPO_CATM)`.
8. The Oro Pipeline shall configurar `cluster_by=["FechaTransaccion", "DimIdCliente"]` en `Hec_Transacciones_ATM`.
9. The Oro Pipeline shall validar con expectations (fail): `DimIdCliente IS NOT NULL`, `IdentificadorTransaccion IS NOT NULL`, `FechaTransaccion IS NOT NULL`, `TipoTransaccion IN ('DATM','CATM')`.

### Requirement 6: Cumplimiento Estricto de Restricciones de Serverless y LSDP

**Objective:** As a Pipeline LSDP de la Medalla de Oro, I want que todo el código generado sea 100% compatible con Databricks Free Edition Serverless Compute y con los lineamientos LSDP, so that el pipeline ejecute sin errores de runtime y aproveche correctamente la incremental refresh de Vistas Materializadas.

#### Acceptance Criteria

1. The Oro Pipeline shall importar LSDP exclusivamente con `from pyspark import pipelines as dp` y NO usar `databricks.sdk.pipelines`.
2. The Oro Pipeline shall NO usar `.cache()`, `.persist()`, `spark.sparkContext`, `.rdd`, `.parallelize()`, `.mapPartitions()`, `.foreachPartition()`, `.toLocalIterator()`, `sc.broadcast()`, `sc.accumulator()`, UDFs, threading ni multiprocessing.
3. The Oro Pipeline shall usar exclusivamente funciones nativas de `pyspark.sql.functions` para todas las transformaciones, hashes y campos calculados.
4. If se requiere broadcast de un DataFrame para optimización de joins, then the Oro Pipeline shall usar `F.broadcast(df)` como hint y nunca `sc.broadcast()`.
5. The Oro Pipeline shall pasar el nombre completo de tres partes (`catalogo.esquema.tabla`) en el parámetro `name` de `@dp.materialized_view` y NO usar parámetros separados `catalog=` ni `schema=`.
6. The Oro Pipeline shall obtener todos los catálogos, esquemas y constantes vía parámetros del pipeline (`spark.conf.get("pipeline.*")`) y constantes centralizadas en `LSDPConfiguracion.py`, sin valores hard-coded.
7. The Oro Pipeline shall NO modificar `spark.sql.ansi.enabled` y deberá manejar correctamente las reglas ANSI (cast a `long` antes de `F.abs(F.hash(...))`, uso de `F.concat_ws` para concatenación de strings).
8. While LSDP analiza la Vista Materializada `Dim_Tiempo` para determinar elegibilidad de incremental refresh, the Oro Pipeline shall mantener su definición exclusivamente con operadores soportados (lectura batch de Delta, `select`, `distinct`, `withColumn` con expresiones determinísticas, `when/otherwise`).

### Requirement 7: Estructura del Código y Convenciones del Proyecto

**Objective:** As a Equipo de Mantenimiento, I want que los notebooks y utilidades de la Medalla de Oro respeten la organización y convenciones del proyecto, so that el código sea consistente, descubrible y alineado con la guía estructural definida en `.kiro/steering/structure.md`.

#### Acceptance Criteria

1. The Oro Pipeline shall ubicar todos los notebooks de transformación en `src/LSDP_Lab_DataVault_DWH/transformations/` siguiendo el patrón de nombre `LSDPOro{Nombre}` (ej: `LSDPOroDimCliente.py`, `LSDPOroDimOperacion.py`, `LSDPOroDimTiempo.py`, `LSDPOroHecTransaccionesATM.py`).
2. The Oro Pipeline shall ubicar las funciones helper reutilizables (resolución de `DimId` persistente, lectura del último Satellite, etc.) en `src/LSDP_Lab_DataVault_DWH/utilities/` siguiendo el patrón `LSDP{NombreUtilidad}.py`.
3. The Oro Pipeline shall consumir las constantes de negocio (`TIPO_DATM`, `TIPO_CATM`, umbrales, separadores de hash) desde `LSDPConfiguracion.py`.
4. The Oro Pipeline shall declarar las tablas de Oro en Unity Catalog usando los parámetros `pipeline.catalogo_oro` y `pipeline.esquema_oro`.
5. The Oro Pipeline shall NO propagar a Oro las columnas exclusivas de Bronce (`año`, `mes`, `dia`, `FechaRegistroParquet`, `_rescued_data`).
6. Where existan campos calculados nuevos en dimensiones u hechos, the Oro Pipeline shall implementarlos con `F.when().otherwise()` siguiendo la convención del proyecto.

### Requirement 8: Pruebas Unitarias y Validación

**Objective:** As a Equipo de QA, I want pruebas unitarias automatizadas para los notebooks de Oro y sus utilidades, so that podamos verificar el comportamiento de las dimensiones y la tabla de hechos sin ejecutar el pipeline completo en Databricks.

#### Acceptance Criteria

1. The Test Suite shall incluir un archivo `tests/test_notebooks_oro.py` que valide la estructura sintáctica y los esquemas esperados de cada notebook de Oro.
2. The Test Suite shall validar que `Dim_Tiempo` se construya a partir de `Sat_Transaccion_Montos.FechaTransaccion` y que sus atributos calendario coincidan con los esperados para un conjunto de fechas de prueba.
3. The Test Suite shall validar que `Hec_Transacciones_ATM` filtre exclusivamente transacciones `DATM` y `CATM` y que las banderas `EsRetiro`/`EsDeposito` se calculen correctamente.
4. The Test Suite shall validar que las llaves subrogadas `DimIdCliente` y `DimIdOperacion` sean estables entre ejecuciones para el mismo conjunto de hashes de entrada.
5. If una utilidad de Oro modifica el comportamiento esperado, then the Test Suite shall fallar de manera explícita indicando el campo/regla afectado.
