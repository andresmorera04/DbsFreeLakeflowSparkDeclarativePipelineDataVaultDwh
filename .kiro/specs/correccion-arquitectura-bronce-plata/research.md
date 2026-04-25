# Research & Decisiones de Diseño

## Resumen
- **Feature**: `correccion-arquitectura-bronce-plata`
- **Alcance del Descubrimiento**: Extensión (modificación de sistema existente)
- **Hallazgos Clave**:
  - La arquitectura actual de Bronce con dos capas (Streaming Table temporal + Materialized View snapshot) se simplifica a una sola Streaming Table persistente por fuente.
  - Los Hubs y Links actualmente usan `@dp.materialized_view()` con `dropDuplicates()` — deben migrar a `dp.create_streaming_table()` + `@dp.append_flow()` con detección de duplicados vía LEFT ANTI JOIN.
  - La función `procesar_satellite()` utiliza un LEFT JOIN con comparación unidimensional de `Hash_Diferenciador` — debe mantener el LEFT JOIN exclusivamente por `Hash_{Hub}` y refinar el filtro WHERE con `Hash_Diferenciador` para Satellites estándar (Cliente, Operación).
  - Los Satellites transaccionales (Hub_Transaccion) requieren una nueva función `procesar_satellite_transaccional()` con LEFT ANTI JOIN por `Hash_Transaccion` + `fecha_transaccion`, sin ROW_NUMBER.

## Log de Investigación

### Patrón Actual de Bronce: Dos Capas por Fuente
- **Contexto**: Cada fuente de datos (CMSTFL, TRXPFL, BLNCFL) tiene dos tablas: una Streaming Table temporal (`*_temp`) y una Materialized View que filtra el snapshot más reciente por `FechaRegistroParquet`.
- **Fuentes Consultadas**: Análisis directo de `LSDPBronceCMSTFL.py`, `LSDPBronceTRXPFL.py`, `LSDPBronceBLNCFL.py`.
- **Hallazgos**:
  - Las 3 fuentes siguen un patrón idéntico: `@dp.table(temporary=True)` + `@dp.materialized_view()`.
  - La MV usa `dp.read()` interno + `F.broadcast(max_fecha)` para filtrar el snapshot más reciente.
  - Total: 6 funciones Python en 3 notebooks, que se reducirán a 3 funciones.
- **Implicaciones**: La eliminación de las MVs implica que Plata ahora consume datos históricos acumulados (no solo el snapshot más reciente). Esto es coherente con Data Vault 2.0, donde los Satellites acumulan historial y comparan hashes para detectar cambios.

### Patrón Actual de Hubs: Materialized Views con dropDuplicates
- **Contexto**: Los 3 Hubs (Cliente, Operacion, Transaccion) usan `@dp.materialized_view()` con `spark.read.table()` y `.dropDuplicates()`.
- **Fuentes Consultadas**: Análisis directo de los 3 notebooks de Hub.
- **Hallazgos**:
  - `Hub_Cliente`: llave simple `IdentificadorCliente` (CUSTID), expectations `expect_or_drop` + `expect_or_fail`.
  - `Hub_Operacion`: llave compuesta `IdentificadorCliente` + `SecuenciaSaldo` (CUSTID + BLSQ), expectations `expect_or_drop` + `expect_or_fail`.
  - `Hub_Transaccion`: llave simple `IdentificadorTransaccion` (TRXID), expectations `expect_or_fail` × 2.
  - Todos usan `calcular_hash_hub()` y `reordenar_columnas_lc()`.
- **Implicaciones**: La migración a Streaming Table requiere que `dropDuplicates()` sea reemplazado por un LEFT ANTI JOIN contra la tabla existente. Se necesita una nueva función helper `procesar_hub()` en `LSDPUtilidadPrincipal.py`.

### Patrón Actual de Links: Materialized Views con dropDuplicates
- **Contexto**: Los 2 Links (ClienteOperacion, ClienteTransaccion) usan `@dp.materialized_view()` con `.dropDuplicates()` por combinación de Hashes.
- **Fuentes Consultadas**: Análisis directo de los 2 notebooks de Link.
- **Hallazgos**:
  - `Link_Cliente_Operacion`: dedup por `["Hash_Cliente", "Hash_Operacion"]`, sin expectations.
  - `Link_Cliente_Transaccion`: dedup por `["Hash_Cliente", "Hash_Transaccion"]`, sin expectations.
  - Ambos calculan hashes desde campos AS400 originales (no leen de Hubs).
  - El Hash del Link se calcula como `calcular_hash_hub([hash_hub1, hash_hub2])`.
- **Implicaciones**: Misma estrategia que Hubs — LEFT ANTI JOIN. Se necesita `procesar_link()` en `LSDPUtilidadPrincipal.py`.

### Patrón Actual de Satellites: Streaming Tables con procesar_satellite()
- **Contexto**: Los 9 Satellites ya usan `dp.create_streaming_table()` + `@dp.append_flow()`, con lectura de tablas temporales `*_temp`. Los Satellites se dividen en **estándar** (Cliente, Operación — 7 tablas) y **transaccionales** (Hub_Transaccion — 2 tablas: `Sat_Transaccion_DatosEstables`, `Sat_Transaccion_Montos`).
- **Fuentes Consultadas**: Análisis directo de los 3 notebooks de Satellite + `LSDPUtilidadPrincipal.py`.
- **Hallazgos**:
  - Todos leen de `dp.read_stream("{Origen}_temp")` — referencia a tablas temporales.
  - La función `procesar_satellite()` actual:
    1. Lee la tabla existente vía `spark.read.table()`.
    2. Aplica `ROW_NUMBER() OVER(PARTITION BY hash_col ORDER BY FechaRegistro DESC) = 1`.
    3. Hace LEFT JOIN solo por `hash_col` (1 condición).
    4. Filtra por `Hash_Existente IS NULL OR Hash_Diferenciador != Hash_Existente`.
  - **Problema con la lógica actual**: El LEFT JOIN es por `hash_col` (`Hash_{Hub}`) únicamente, y luego filtra por `Hash_Existente IS NULL OR Hash_Diferenciador != Hash_Existente`. El enfoque del JOIN por `Hash_{Hub}` es correcto; lo que se refina es la claridad del filtro WHERE.
  - **Nueva lógica requerida (Satellites estándar)**: Mantener LEFT JOIN exclusivamente por `Hash_{Hub}` (`ON A.Hash_{Hub} = B.Hash_{Hub}`), con filtro `WHERE (B.Hash_{Hub} IS NULL) OR (A.Hash_Diferenciador != B.Hash_Diferenciador)`. El JOIN por `Hash_{Hub}` garantiza que todos los registros de datos nuevos sean relacionados con su último estado conocido; el WHERE determina si es entidad nueva o cambio en atributos.
  - **Nueva lógica requerida (Satellites transaccionales)**: LEFT ANTI JOIN por `Hash_Transaccion` + `fecha_transaccion`, sin ROW_NUMBER, acumulando toda la historia. `Hash_Diferenciador` se mantiene para trazabilidad pero no participa en deduplicación.
- **Implicaciones**: El cambio en `procesar_satellite()` simplifica la lógica de filtrado post-JOIN para Satellites estándar. Para Satellites transaccionales se necesita una nueva función `procesar_satellite_transaccional()` con semántica fundamentalmente distinta (acumulación completa vs. detección de cambios).

### Compatibilidad con Streaming Table Registrada en UC para Bronce
- **Contexto**: Al eliminar `temporary=True`, la ST se registra en Unity Catalog y es visible para Plata.
- **Fuentes Consultadas**: Documentación LSDP en SYSTEM.md, steering/tech.md.
- **Hallazgos**:
  - `@dp.table(name="...", temporary=True)` → no registra en UC. `@dp.table(name=f"{cat}.{esq}.{tabla}")` → registra en UC.
  - Las Streaming Tables registradas en UC son legibles tanto por `spark.read.table()` como por `dp.read_stream()`. Para esta versión del laboratorio, **todas las entidades de Plata** (Hubs, Links y Satellites) usan `dp.read_stream()` como mecanismo uniforme de lectura desde Bronce.
  - El cambio no requiere modificación del AutoLoader ni del mecanismo de checkpoint — solo del decorador.
- **Implicaciones**: Cambio de bajo riesgo en Bronce. El checkpoint de AutoLoader persiste independientemente del registro en UC.

## Evaluación de Patrones de Arquitectura

| Opción | Descripción | Fortalezas | Riesgos/Limitaciones | Notas |
|--------|-------------|------------|----------------------|-------|
| ST persistente directa (Bronce) | Una sola Streaming Table por fuente, registrada en UC con nombre definitivo | Simplicidad, linaje directo, eliminación de capa redundante | Plata consume datos acumulados (no snapshot) — la detección de cambios en Satellites ya maneja esto | **Seleccionada** |
| ST + Append Flow (Hubs/Links) | `dp.create_streaming_table()` + `@dp.append_flow()` con LEFT ANTI JOIN | Append-Only real, sin recalcular tabla completa, consistente con Satellites | Requiere leer tabla existente para comparar (primera ejecución = fallback) | **Seleccionada** |
| LEFT JOIN por Hash_Hub + WHERE Hash_Diferenciador (Satellites estándar) | JOIN exclusivamente por `Hash_{Hub}`, filtro WHERE por `Hash_Diferenciador` | Semántica clara: el JOIN relaciona cada entidad con su último estado; el WHERE determina si es nueva o cambió | Requiere que ROW_NUMBER siga aplicándose para obtener último registro por entidad | **Seleccionada** para Cliente/Operación |
| LEFT ANTI JOIN (Satellites transaccionales) | JOIN por `Hash_Transaccion` + `fecha_transaccion` sin ROW_NUMBER | Acumulación histórica completa, semántica transaccional correcta | No detecta cambios, solo deduplica registros exactos | **Seleccionada** para Hub_Transaccion |

## Decisiones de Diseño

### Decisión: Nueva función `procesar_hub()` en LSDPUtilidadPrincipal.py
- **Contexto**: Los Hubs necesitan detección de duplicados por llave de negocio, análogo a `procesar_satellite()`.
- **Alternativas Consideradas**:
  1. Inline la lógica en cada notebook de Hub — duplicación de código.
  2. Función centralizada `procesar_hub()` — reutilizable para los 3 Hubs.
- **Enfoque Seleccionado**: Función centralizada `procesar_hub()` que recibe el DataFrame de datos nuevos, el nombre completo de la tabla Hub, y la lista de columnas de llave de negocio. Retorna solo los registros con llaves que no existen en la tabla.
- **Razón**: Consistencia con el patrón existente de `procesar_satellite()`. Reutilización para los 3 Hubs.
- **Trade-offs**: Una función más en el módulo de utilidades, pero elimina duplicación en 3 notebooks.

### Decisión: Nueva función `procesar_link()` en LSDPUtilidadPrincipal.py
- **Contexto**: Los Links necesitan detección de duplicados por combinación de hashes de Hubs.
- **Alternativas Consideradas**:
  1. Reutilizar `procesar_hub()` pasando los hashes como "llaves de negocio".
  2. Función separada `procesar_link()` con semántica específica.
- **Enfoque Seleccionado**: Función separada `procesar_link()` que recibe DataFrame, nombre completo de tabla Link, y columnas hash de los dos Hubs. Retorna solo combinaciones nuevas.
- **Razón**: Aunque el algoritmo es similar a `procesar_hub()`, los Links tienen semántica distinta (combinación de hashes, no llaves de negocio). Función separada mejora la legibilidad y mantenibilidad.
- **Trade-offs**: Dos funciones similares, pero semántica clara que evita confusión.

### Decisión: Mantenimiento de lógica de `procesar_satellite()` con LEFT JOIN por Hash_Hub + WHERE Hash_Diferenciador
- **Contexto**: La lógica actual ya implementa LEFT JOIN solo por `Hash_{Hub}` y filtra por `Hash_Diferenciador` en la cláusula WHERE. Esta decisión confirma que la lógica existente es correcta y aplica únicamente a los **Satellites estándar (Cliente, Operación)**. Se mantiene el enfoque de JOIN por columna única (`Hash_{Hub}`) con filtro WHERE por `Hash_Diferenciador`.
- **Alternativas Consideradas**:
  1. Mantener lógica actual sin cambios (la implementación actual ya es correcta).
  2. LEFT JOIN compuesto por ambas columnas (`Hash_{Hub}` + `Hash_Diferenciador`) en la condición ON.
  3. LEFT JOIN exclusivamente por `Hash_{Hub}` (`ON A.Hash_{Hub} = B.Hash_{Hub}`) + filtro `WHERE (B.Hash_{Hub} IS NULL) OR (A.Hash_Diferenciador != B.Hash_Diferenciador)`.
- **Enfoque Seleccionado**: Opción 3 (coincide con la implementación actual) — LEFT JOIN exclusivamente por `Hash_{Hub}` para Satellites estándar.
- **Razón**: El JOIN por `Hash_{Hub}` garantiza que todos los registros de datos nuevos sean relacionados con el último estado conocido de la entidad en la Streaming Table. El WHERE determina explícitamente si se trata de una entidad nueva (`B.Hash_{Hub} IS NULL`) o un cambio en atributos (`A.Hash_Diferenciador != B.Hash_Diferenciador`). Esto es semánticamente más claro que el JOIN compuesto, donde un cambio en Hash_Diferenciador produce indirectamente un NULL en el lado derecho.
- **Trade-offs**: El ROW_NUMBER() sigue siendo necesario para obtener el último registro por entidad antes del JOIN. La primera ejecución sigue usando el fallback por `AnalysisException`.

### Decisión: Nueva función `procesar_satellite_transaccional()` en LSDPUtilidadPrincipal.py
- **Contexto**: Los Satellites del Hub_Transaccion (`Sat_Transaccion_DatosEstables`, `Sat_Transaccion_Montos`) tienen semántica transaccional: deben acumular la historia completa sin reducir al último registro. La lógica de `procesar_satellite()` (ROW_NUMBER + LEFT JOIN por Hash_Hub) no es aplicable.
- **Alternativas Consideradas**:
  1. Añadir parámetro `modo` a `procesar_satellite()` para alternar entre estándar y transaccional.
  2. Función separada `procesar_satellite_transaccional()` con semántica específica.
- **Enfoque Seleccionado**: Opción 2 — Función separada `procesar_satellite_transaccional()`.
- **Razón**: La semántica es fundamentalmente distinta (acumulación histórica vs. detección de cambios). Mezclar ambas lógicas en una sola función con condicionales degradaría la legibilidad y violaría el principio de responsabilidad única.
- **Trade-offs**: Una función adicional en el módulo de utilidades. La columna `Hash_Diferenciador` se mantiene para trazabilidad y auditoría pero no participa en la lógica de deduplicación.

### Decisión: Lectura uniforme de Bronce desde Plata con dp.read_stream()
- **Contexto**: Al eliminar la MV de snapshot, Plata ahora lee la Streaming Table completa (histórica) de Bronce. Todas las entidades de Plata (Hubs, Links y Satellites) utilizan `dp.read_stream()` como mecanismo de lectura uniforme desde Bronce.
- **Alternativas Consideradas**:
  1. Agregar filtrado por `FechaRegistroParquet` más reciente dentro de cada notebook de Plata.
  2. Dejar que las funciones de detección de duplicados/cambios manejen la deduplicación naturalmente.
  3. Usar `spark.read.table()` (batch) para Hubs/Links y `dp.read_stream()` (streaming) para Satellites.
- **Enfoque Seleccionado**: Opción 2 con lectura uniforme `dp.read_stream()` para todas las entidades.
- **Razón**: `dp.read_stream()` permite procesamiento incremental: solo los datos nuevos desde la última ejecución se procesan, evitando releer la tabla de Bronce completa en cada ejecución. Los Hubs usan LEFT ANTI JOIN por llave de negocio (ignoran duplicados automáticamente). Los Links usan LEFT ANTI JOIN por combinación de hashes. Los Satellites usan `procesar_satellite()` con hash diferenciador que detecta cambios. Todos los patrones manejan datos repetidos naturalmente.
- **Trade-offs**: Los Hubs y Links siguen necesitando `spark.read.table()` internamente (en `procesar_hub()`/`procesar_link()`) para leer su propia tabla destino y hacer el LEFT ANTI JOIN, pero la lectura de la fuente Bronce es incremental via streaming.

## Riesgos y Mitigaciones
- **Riesgo 1**: Primera ejecución post-migración — las tablas Hub/Link existentes (como MVs) deben eliminarse y recrearse como STs. **Mitigación**: El pipeline se ejecuta en modalidad *Run Pipeline with Full Table Refresh*, lo cual reconstruye desde cero todas las tablas Delta, Streaming Tables y Materialized Views. No se requiere estrategia de migración adicional.
- **Riesgo 2**: Performance al leer ST acumuladas de Bronce sin filtro de snapshot. **Mitigación**: Liquid Clustering por `FechaRegistroParquet` + optimizador de Spark; los LEFT ANTI JOIN son eficientes.
- **Riesgo 3**: Adición de columna `fecha_transaccion` en `Sat_Transaccion_DatosEstables` (esquema nuevo). **Mitigación**: El pipeline se ejecuta en modalidad *Run Pipeline with Full Table Refresh*, por lo que la nueva Streaming Table de Plata se genera con el nuevo esquema desde cero. No se materializa el riesgo de evolución de esquema sobre datos existentes.
- **Riesgo 4**: Checkpoint de AutoLoader al cambiar de `temporary=True` a registro en UC. **Mitigación**: El checkpoint es independiente del registro en UC; no se pierde estado.

## Referencias
- SYSTEM.md — Especificación técnica completa del proyecto.
- Documentación LSDP: `from pyspark import pipelines as dp` — módulo nativo del runtime Databricks.
- Data Vault 2.0: *Building a Scalable Data Warehouse with Data Vault 2.0* — principios Append-Only para Hubs, Links y Satellites.
