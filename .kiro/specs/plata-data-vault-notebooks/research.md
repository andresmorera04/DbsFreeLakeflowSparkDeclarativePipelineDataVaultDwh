# Investigación y Decisiones de Diseño — plata-data-vault-notebooks

## Resumen

- **Feature**: `plata-data-vault-notebooks`
- **Alcance de Descubrimiento**: Extensión (brownfield) — se extiende el pipeline LSDP existente del Incremento 1 (Bronce + Utilities) con la capa de Plata (Data Vault 2.0)
- **Hallazgos Clave**:
  1. Las 3 funciones helper existentes en `LSDPUtilidadPrincipal.py` (`calcular_hash_hub`, `calcular_hash_diferenciador`, `reordenar_columnas_lc`) cubren el 70% de la lógica de Plata; se necesitan 2 funciones nuevas (detección de cambios + clasificación por umbrales).
  2. El patrón `procesar_satellite` documentado en SYSTEM.md usa `Window + ROW_NUMBER + left join` para detección de cambios — es 100% compatible con Serverless (sin RDD ni cache).
  3. Discrepancias verificadas en datos reales: TRXID es StringType (no LongType), TRXRK/TRXFR escala 0-100 (no 0-1), Sat_Operacion_FechasEvento tiene 19 campos DateType (BLNCFL tiene 19 fechas mapeables, no 34).
  4. **CRÍTICO**: Los Satellites NO pueden ser Materialized Views — MV recalcula todo en cada ejecución, destruyendo la semántica Append-Only de Data Vault 2.0. Se adopta el patrón `dp.create_streaming_table()` + `@dp.append_flow()` (probado en Dim_Tiempo de SYSTEM.md) que preserva registros existentes y solo agrega cambios detectados.

## Registro de Investigación

### Patrón de Detección de Cambios para Satellites

- **Contexto**: Todos los Satellites requieren procesamiento Append-Only con detección de cambios vía `Hash_Diferenciador`.
- **Fuentes Consultadas**: SYSTEM.md (sección "Patrón de Detección de Cambios"), patrones de Bronce existentes.
- **Hallazgos**:
  - SYSTEM.md define una función `procesar_satellite(spark, nombre_sat, hash_col, datos_nuevos)` que:
    1. Usa `Window.partitionBy(hash_col).orderBy(FechaRegistro.desc())` + `ROW_NUMBER() == 1` para obtener último registro por entidad.
    2. Hace `left join` entre datos nuevos y último existente.
    3. Filtra registros donde `Hash_Existente IS NULL` (nuevo) o `Hash_Diferenciador != Hash_Existente` (cambio).
    4. En primera ejecución (tabla no existe), captura `AnalysisException` y retorna todos los registros.
  - Este patrón NO usa `.cache()`, `.persist()`, RDDs ni UDFs — 100% compatible con Serverless.
  - La función debe recibir `spark` como parámetro (consistente con `obtener_configuracion(spark)` en Incremento 1).
  - La función necesita `catalogo_plata` y `esquema_plata` para construir el nombre de 3 partes de la tabla existente.
- **Implicaciones**: Se implementará como función nueva en `LSDPUtilidadPrincipal.py`. Debe recibir `spark`, nombre del satellite, columna hash de la entidad y DataFrame con datos nuevos. También necesita los parámetros de catálogo/esquema.

### Materialized Views vs Streaming Tables para Plata

- **Contexto**: Definir el tipo de tabla LSDP correcto para Hubs, Links y Satellites.
- **Fuentes Consultadas**: SYSTEM.md (secciones "API de Decoradores", "Patrón de Detección de Cambios", "Dim_Tiempo acumulativa"), steering tech.md, documentación oficial Databricks LSDP (`ldp-python-ref-streaming-table`, `ldp-python-ref-append-flow`).
- **Hallazgos**:
  - **Hubs y Links**: `@dp.materialized_view()` — son tablas de referencia idempotentes que se recalculan completamente con cada ejecución (deduplicación de llaves de negocio). No acumulan historial, por lo que el recálculo completo es correcto y eficiente.
  - **Satellites con MV (❌ DESCARTADO)**: Usar `@dp.materialized_view()` para Satellites implica que en CADA ejecución se recalcula la tabla completa. Esto viola el principio fundamental de Data Vault 2.0 donde los Satellites son estrictamente Append-Only y acumulan historial. A medida que crece el volumen (registros de cambios acumulados), el recálculo completo deteriora gravemente el rendimiento haciendo el pipeline inviable.
  - **Satellites con ST + Append Flow (✅ SELECCIONADO)**: El patrón `dp.create_streaming_table()` + `@dp.append_flow()` ya está probado y aprobado en SYSTEM.md para la `Dim_Tiempo` (Oro). Permite:
    1. Persistencia permanente: los registros existentes del Satellite nunca se tocan.
    2. Solo se agregan registros nuevos/cambiados en cada ejecución.
    3. La función `procesar_satellite()` lee el Satellite existente vía `spark.read.table()` y retorna SOLO los cambios detectados.
    4. El `@dp.append_flow()` inserta únicamente esos cambios en la Streaming Table.
  - **`@dp.append_flow()` acepta DataFrames batch**: La documentación oficial de Databricks indica que por defecto `@dp.append_flow()` espera un streaming DataFrame, pero con `once=True` acepta batch. Sin embargo, el patrón aprobado de `Dim_Tiempo` en SYSTEM.md usa batch sin `once=True` y se ejecuta en cada pipeline run — este es el comportamiento requerido para Satellites.
  - **`dp.create_streaming_table()` soporta expectations**: Confirmado en la API oficial — acepta `expect_all`, `expect_all_or_drop`, `expect_all_or_fail` como parámetros directos (no como decoradores sobre el append_flow).
  - **Liquid Clustering soportado**: `dp.create_streaming_table()` acepta `cluster_by` y `table_properties`.
- **Implicaciones**:
  - Hubs y Links (5 tablas): `@dp.materialized_view()` con `spark.read.table()`.
  - Satellites (9 tablas): `dp.create_streaming_table()` + `@dp.append_flow()` con `spark.read.table()` + `procesar_satellite()`.
  - La función `procesar_satellite()` retorna SOLO registros nuevos/cambiados (sin `unionByName` con existentes — el framework LSDP maneja el append).
  - El patrón elimina la Window + ROW_NUMBER de la ruta crítica de recarga (solo se ejecuta para comparación de hashes, no para reconstruir la tabla completa).

### Función Genérica de Clasificación por Umbrales

- **Contexto**: 7 campos calculados en 4 Satellites diferentes usan la misma lógica: clasificar un valor numérico según rangos definidos en diccionarios `UMBRAL_*`.
- **Fuentes Consultadas**: `LSDPConfiguracion.py` (constantes existentes), SYSTEM.md (definiciones de campos calculados).
- **Hallazgos**:
  - Todos los diccionarios `UMBRAL_*` tienen la misma estructura: `dict[str, tuple[min, max]]`.
  - La lógica es siempre: cadena de `F.when(col.between(min, max), lit(nombre))...otherwise(lit("DESCONOCIDO"))`.
  - `ClasificacionCanalATM` NO sigue este patrón — es una lógica condicional basada en `tipo_transaccion` y `canal_transaccion`, no en umbrales numéricos.
- **Implicaciones**: Se creará una función genérica `clasificar_por_umbral(columna, diccionario_umbrales)` en `LSDPUtilidadPrincipal.py`. `ClasificacionCanalATM` se implementará directamente inline en el notebook (decisión aprobada).

### Organización de Notebooks de Plata

- **Contexto**: Decidir cuántos notebooks se crean y qué entidades contiene cada uno.
- **Fuentes Consultadas**: steering structure.md (convención `LSDPPlata{Entidad}`), patrones de Bronce (1 notebook = 1 fuente).
- **Hallazgos**:
  - Bronce: 1 notebook por fuente (3 notebooks para 6 tablas — 2 tablas por notebook).
  - Para Plata, la granularidad natural es:
    - 1 notebook por Hub (3 notebooks: `LSDPPlataHubCliente`, `LSDPPlataHubOperacion`, `LSDPPlataHubTransaccion`).
    - 1 notebook por Link (2 notebooks: `LSDPPlataLinkClienteOperacion`, `LSDPPlataLinkClienteTransaccion`).
    - 1 notebook por grupo de Satellites del mismo Hub (3 notebooks: `LSDPPlataSatCliente`, `LSDPPlataSatOperacion`, `LSDPPlataSatTransaccion`).
  - Total: **8 notebooks** de transformación de Plata.
  - Cada notebook de Satellites agrupa las MV del mismo Hub en un solo archivo para compartir la lectura de la tabla fuente de Bronce.
- **Implicaciones**: 8 notebooks + 2 funciones nuevas en utilidades = 10 artefactos de código.

### Discrepancias de Datos Verificadas

- **Contexto**: Validar que la documentación SYSTEM.md coincida con los datos reales.
- **Fuentes Consultadas**: SYSTEM.md, research previo del Incremento 1.
- **Hallazgos Confirmados**:
  - `TRXID` es **StringType** nativo en Parquet → no requiere `.cast("string")` para SHA2.
  - `TRXRK` y `TRXFR` escala **0-100** (no 0.0-1.0) → los umbrales de `UMBRAL_RIESGO_FRAUDE` ya están en escala 0-100 en `LSDPConfiguracion.py`.
  - Sat_Operacion_FechasEvento: BLNCFL tiene **19 campos DateType** mapeables según la tabla de SYSTEM.md (BLOD, BLXD, BLUD, BLLD, BLSD, BLPD2, BLRD, BLMD, BLCD2, BLBD, BLFD, BLGD, BLHD, BLID, BLJD, BLKD, BLND, BLTD, BLVD).
  - 94.3% de clientes tienen transacciones en TRXPFL → 5.7% sin transacciones no generan registros en Link_Cliente_Transaccion ni Sat_Transaccion (comportamiento correcto).
  - BLNCFL 0% variación entre snapshots → los Satellites de Operación no insertarán registros duplicados gracias al Hash_Diferenciador.
- **Implicaciones**: El diseño debe considerar estas discrepancias documentadas para evitar errores en implementación.

## Evaluación de Patrones Arquitectónicos

| Opción | Descripción | Fortalezas | Riesgos / Limitaciones | Notas |
|--------|-------------|-----------|------------------------|-------|
| MV para todos (Hubs, Links, Satellites) | Todas las tablas como `@dp.materialized_view()` | Consistencia de patrón; LSDP gestiona dependencias | **FATAL**: MV recalcula completo en cada ejecución → destruye historial acumulado de Satellites; rendimiento inviable a escala | **❌ Descartado** — viola principio Append-Only de DV2.0 |
| MV para Hubs/Links + ST+AppendFlow para Satellites | Hubs/Links como MV; Satellites como `dp.create_streaming_table()` + `@dp.append_flow()` | Append-Only real; registros existentes nunca se tocan; solo cambios detectados se agregan; patrón probado en Dim_Tiempo (SYSTEM.md) | Dos patrones de tabla distintos en Plata; la detección de cambios vía `procesar_satellite()` requiere leer el Satellite existente en cada ejecución | **✅ Seleccionado** — respeta DV2.0; escalable; patrón aprobado en SYSTEM.md |
| ST con `@dp.table()` + `spark.readStream` para Satellites | Satellites como `@dp.table()` con `spark.readStream.table()` desde MV de Bronce | Streaming incremental nativo; checkpoints automáticos | La MV de Bronce se recalcula completo (snapshot) → `readStream` puede tratar TODO como nuevo en cada ejecución; no hay control fino de detección de cambios por `Hash_Diferenciador` | **❌ Descartado** — la fuente MV Bronce no es un stream incremental real |

## Decisiones de Diseño

### Decisión: MV para Hubs/Links + Streaming Table Acumulativa con Append Flow para Satellites ✅ APROBADA

- **Estado**: **APROBADA** (2026-04-13)
- **Contexto**: Elegir el tipo de tabla LSDP para cada entidad Data Vault 2.0, respetando que los Satellites son estrictamente Append-Only y acumulan historial indefinidamente.
- **Alternativas Consideradas**:
  1. MV para todos — `@dp.materialized_view()` con lógica batch que recalcula completamente.
  2. MV para Hubs/Links + ST con AppendFlow para Satellites — patrón mixto.
  3. ST con `@dp.table()` + `spark.readStream` para Satellites — streaming desde MV Bronce.
- **Enfoque Seleccionado**: (2) MV para Hubs/Links + ST con AppendFlow para Satellites.
- **Justificación**:
  - **Hubs y Links**: Son conjuntos de llaves de negocio únicos y deduplicados. Recalcular con MV es correcto e idempotente — el `dropDuplicates` produce siempre el mismo resultado.
  - **Satellites**: Data Vault 2.0 requiere Append-Only real. Los registros existentes no deben ser tocados ni reprocesados. El patrón `dp.create_streaming_table()` + `@dp.append_flow()`:
    - Preserva permanentemente todos los registros existentes.
    - Solo agrega registros con cambios detectados (`Hash_Diferenciador` diferente).
    - Es el mismo patrón aprobado para `Dim_Tiempo` en SYSTEM.md (R10).
    - La función `procesar_satellite()` lee el Satellite via `spark.read.table()`, obtiene el último `Hash_Diferenciador` por entidad (Window + ROW_NUMBER), y retorna SOLO los registros nuevos/cambiados.
    - El `@dp.append_flow()` inserta exclusivamente esos cambios — NO recarga la tabla completa.
  - **Opción 3 descartada**: Las MV de Bronce se recalculan completamente (snapshot del último día), por lo que `spark.readStream.table()` sobre ellas no ofrece incrementalidad real — podría inyectar todos los registros como "nuevos" en cada ejecución.
- **Trade-offs**: Dos patrones de tabla en Plata (MV + ST) añaden complejidad conceptual, pero cada uno está correctamente alineado con la semántica de su entidad DV2.0.
- **Seguimiento**: Monitorear rendimiento de `procesar_satellite()` con historial acumulado grande — la Window sobre el Satellite existente puede ser costosa a largo plazo. Liquid Clustering en `Hash_Entidad` + `FechaRegistro` mitiga esto.

### Decisión: Función genérica `clasificar_por_umbral` centralizada ✅ APROBADA

- **Estado**: **APROBADA** (2026-04-13)
- **Contexto**: 7 campos calculados usan la misma lógica de clasificación por rangos numéricos.
- **Alternativas Consideradas**:
  1. Lógica inline en cada notebook — `F.when().when()...otherwise()` repetido.
  2. Función genérica en utilidades que reciba columna + diccionario de umbrales.
- **Enfoque Seleccionado**: Función genérica centralizada en `LSDPUtilidadPrincipal.py`.
- **Justificación**: Elimina duplicación de código; los umbrales ya están definidos en `LSDPConfiguracion.py` como diccionarios con estructura uniforme.
- **Trade-offs**: Una función más en utilidades; pero es mínima complejidad.
- **Seguimiento**: Validar que la función maneje correctamente tipos DoubleType (umbrales de crédito) e IntegerType (umbrales de edad/monto).

### Decisión: Agrupación de Satellites por Hub en un solo notebook ✅ APROBADA

- **Estado**: **APROBADA** (2026-04-13)
- **Contexto**: Organizar 9 Satellites en notebooks de transformación.
- **Alternativas Consideradas**:
  1. 1 notebook por Satellite (9 notebooks) — máxima granularidad.
  2. 1 notebook por grupo de Hub (3 notebooks de Satellites) — comparten lectura de tabla Bronce.
  3. 1 notebook único para todos los Satellites — mínimo número de archivos.
- **Enfoque Seleccionado**: 1 notebook por grupo de Hub (3 notebooks de Satellites).
- **Justificación**: Los Satellites del mismo Hub leen de la misma tabla Bronce; agruparlos evita 9 lecturas redundantes. Un solo notebook es demasiado largo para mantenimiento.
- **Trade-offs**: Notebooks de Satellites serán más extensos (~200-400 líneas) pero agrupan lógica cohesiva.

### Decisión: ClasificacionCanalATM implementada inline en el notebook ✅ APROBADA

- **Estado**: **APROBADA** (2026-04-13)
- **Contexto**: El campo calculado `ClasificacionCanalATM` no sigue el patrón de umbrales numéricos — es una lógica condicional basada en `tipo_transaccion` y `canal_transaccion`.
- **Alternativas Consideradas**:
  1. Función `clasificar_canal_atm(col_tipo, col_canal)` centralizada en utilidades.
  2. Lógica inline directamente en el notebook de Satellites de Transacción.
- **Enfoque Seleccionado**: Lógica inline en el notebook `LSDPPlataSatTransaccion`.
- **Justificación**: Es la única clasificación con esta lógica (no se reutiliza en otros notebooks). Extraerla a una función centralizada añadiría complejidad innecesaria para un uso único. La lógica `F.when()...otherwise()` basada en `tipo_transaccion` e `canal_transaccion` es suficientemente simple y autoexplicativa en contexto.
- **Trade-offs**: Si en el futuro se requiere esta clasificación en Oro, deberá duplicarse o refactorizarse a utilidades.

## Riesgos y Mitigaciones

- **Riesgo 1**: Performance de `procesar_satellite` con `Window + ROW_NUMBER` sobre la tabla completa del Satellite en ejecuciones posteriores con historial acumulado. — **Mitigación**: Liquid Clustering en `Hash_Entidad` + `FechaRegistro` asegura que las consultas de ventana aprovechen el particionamiento lógico de Delta. Además, solo se necesita el ÚLTIMO registro por entidad, lo que permite optimización con pushdown.
- **Riesgo 2**: `AnalysisException` capturada en primera ejecución podría enmascarar otros errores de configuración (ej: nombre de tabla incorrecto). — **Mitigación**: La excepción solo se captura para `spark.read.table()` del satellite específico; cualquier otro error se propaga normalmente.
- **Riesgo 3**: `@dp.append_flow()` podría inyectar registros duplicados si `procesar_satellite()` no filtra correctamente (ej: si el Hash_Diferenciador no cambia pero el registro se detecta como "nuevo"). — **Mitigación**: El filtro es explícito: `Hash_Existente IS NULL` (nueva entidad) O `Hash_Diferenciador != Hash_Existente` (cambio real). Si no hay ninguno de los dos, el registro no se inserta.
- **Riesgo 4**: La MV de Bronce podría no estar lista cuando el `@dp.append_flow()` del Satellite se ejecute. — **Mitigación**: LSDP gestiona dependencias automáticamente — el flujo que lee una tabla espera a que su fuente esté disponible. Verificado en patrón de Bronce (MV depende de ST temporal).

## Referencias

- SYSTEM.md — Modelo de datos completo, patrones de código seguros, restricciones Serverless.
- steering/product.md — Capacidades principales y caso de uso.
- steering/tech.md — Stack tecnológico, restricciones y patrones LSDP.
- steering/structure.md — Convenciones de nombrado y organización del repositorio.
- Incremento 1 (bronce-utilities-ingesta) — Patrones de notebooks, diseño de utilidades, MV de Bronce como fuentes.
