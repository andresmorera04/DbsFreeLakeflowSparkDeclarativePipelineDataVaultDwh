# Research & Design Decisions — oro-modelo-estrella-mv-tiempo

## Summary
- **Feature**: `oro-modelo-estrella-mv-tiempo`
- **Discovery Scope**: Extension (la Medalla de Oro es nueva, pero el patrón LSDP+UC+Delta y las utilidades existentes son extensión directa de Plata).
- **Discovery Process**: Light (`design-discovery-light.md`) — el pipeline LSDP, las restricciones Serverless, el modelo Data Vault y las convenciones del proyecto ya están documentados y verificados. La superficie de incertidumbre se reduce a 6 ítems acotados.
- **Key Findings**:
  - La Medalla de Plata usa **snake_case** para columnas de negocio en **Satellites** (`fecha_transaccion`, `monto_principal`, `identificador_cliente`, `tipo_transaccion`, etc.) y **PascalCase** para columnas de negocio en **Hubs** (`IdentificadorCliente`, `SecuenciaSaldo`, `IdentificadorTransaccion`) — verificado en runtime Databricks; usar snake_case contra Hubs produce `UNRESOLVED_COLUMN`. Los metadatos Data Vault usan **PascalCase con underscore** (`Hash_*`, `FechaRegistro`, `FuenteDatos`). Los requirements y `SYSTEM.md` mencionan `FechaTransaccion` (PascalCase) que no es el nombre real en los Satellites: la traducción de naming se aplica en la capa de Oro para columnas de Satellite; las columnas de Hub ya vienen en PascalCase y se leen directamente sin alias.
  - Las utilidades existentes (`LSDPConfiguracion.py`, `LSDPUtilidadPrincipal.py`) cubren constantes de negocio (`TIPO_DATM`, `TIPO_CATM`, umbrales) y patrones de hashing/clustering. La Medalla de Oro **no requiere** modificarlas; sí amerita un módulo nuevo `LSDPUtilidadOro.py` con helpers especializados (último por hash, asignación de DimId estable).
  - LSDP soporta incremental refresh para Materialized Views basadas en `SELECT … DISTINCT` cuando la fuente es una tabla Delta administrada. Operaciones con `ROW_NUMBER`/`Window` típicamente disparan refresh completo. Esta clasificación condiciona cómo se modela cada MV de Oro.

## Research Log

### Topic 1 — Compatibilidad de incremental refresh en LSDP Materialized Views (Free Edition Serverless)
- **Context**: R2 exige que `Dim_Tiempo` se refresque incrementalmente al aparecer fechas nuevas en `Sat_Transaccion_Montos`. Hay incertidumbre sobre si `DISTINCT` califica.
- **Sources Consulted**:
  - Databricks Lakeflow Spark Declarative Pipelines — docs oficiales sobre Materialized Views y refresh incremental.
  - Documentación interna del proyecto: `SYSTEM.md` (sección "Compatibilidad con Databricks Free Edition") y `tech.md` (decisiones técnicas LSDP).
  - Patrón ya usado en el repo para Sats (ST + AppendFlow vs MV).
- **Findings**:
  - Las MV con `select(...).distinct()` sobre una sola tabla Delta administrada son elegibles para refresh incremental cuando: (a) la fuente está dentro de Unity Catalog, (b) `withColumn` solo usa expresiones determinísticas, (c) no hay joins ni window functions, (d) no hay UDFs ni `current_date`/`current_timestamp`/`rand`.
  - MVs con `ROW_NUMBER() OVER` no son elegibles para incremental refresh — disparan refresh completo. Esto aplica a `Dim_Cliente` y `Dim_Operacion` por su lógica "último estado por hash".
  - MVs que combinan filtros + joins simples + `withColumn` determinístico (caso `Hec_Transacciones_ATM`) suelen requerir refresh completo aunque la fuente sea Delta.
- **Implications**:
  - `Dim_Tiempo` queda como única MV elegible para refresh incremental — el diseño la mantiene con `select` + `distinct` + `withColumn` determinístico únicamente.
  - `Dim_Cliente`, `Dim_Operacion` y `Hec_Transacciones_ATM` se diseñan asumiendo refresh completo. El costo es manejable porque el volumen del laboratorio es bajo y el cluster_by liquid mantiene buena performance.
  - Para fechas faltantes que aparezcan tarde (back-dated) `Dim_Tiempo` las recogerá automáticamente sin lógica extra.

### Topic 2 — Discrepancia de naming snake_case (Plata) vs PascalCase (Oro)
- **Context**: Plata usa snake_case para columnas de negocio; los requirements de Oro y los nombres del modelo estrella en `SYSTEM.md` referencian PascalCase.
- **Sources Consulted**: `LSDPPlataSatTransaccion.py` (líneas 73-95, 158-220), `LSDPPlataSatCliente.py`, `LSDPPlataSatOperacion.py`, `structure.md`.
- **Findings**:
  - Plata: `fecha_transaccion`, `monto_principal`, `comision_transaccion`, `total_transaccion`, `tipo_transaccion`, `moneda_transaccion`, `estado_transaccion`, `canal_transaccion`, `identificador_cliente`, `tipo_cuenta`, `moneda_cuenta`, `estado_cuenta`, `saldo_disponible`, `saldo_total`, `limite_credito`, `sexo_cliente`, `edad_cliente`, etc.
  - Plata mantiene PascalCase para: metadatos DV (`Hash_*`, `FechaRegistro`, `FuenteDatos`), columnas calculadas (`RangoMontoTransaccion`, `ClasificacionCanalATM`, `RangoEtario`, `CategoriaIngresos`) y **columnas de negocio en Hubs** (`IdentificadorCliente` en Hub_Cliente y Hub_Operacion, `SecuenciaSaldo` en Hub_Operacion, `IdentificadorTransaccion` en Hub_Transaccion).
  - El estándar Star Schema (Kimball) y `SYSTEM.md` Oro usan PascalCase consistentemente para todas las columnas.
- **Implications**:
  - La capa de Oro renombra explícitamente al construir cada MV usando `.alias()` o `.withColumnRenamed()` **únicamente para columnas de Satellite** (snake_case → PascalCase). Las columnas de Hub ya están en PascalCase y se leen directamente sin alias.
  - El SQL/BI consumidor de Oro ve únicamente PascalCase coherente con el modelo estrella.
  - La discrepancia textual del requirement (`FechaTransaccion`) se interpreta como nombre lógico **de Oro**; en código se lee `fecha_transaccion` desde `Sat_Transaccion_Montos` y se renombra a `FechaTransaccion`/`FechaClave` al exponer.

### Topic 3 — Algoritmo de asignación de DimId estable
- **Context**: R3 y R4 exigen que `DimIdCliente` y `DimIdOperacion` sean estables entre ejecuciones para el mismo conjunto de hashes.
- **Sources Consulted**: Patrones DW (Kimball Surrogate Keys), prácticas LSDP, `procesar_satellite()` del repo (uso interno de Window).
- **Findings**:
  - Tres alternativas evaluadas:
    - **A. Lookup persistente**: tabla auxiliar que asigna IDs incrementales por hash; preserva IDs históricos pero requiere ST adicional + lógica de "buscar nuevo, asignar siguiente". Complejo en LSDP MV.
    - **B. `dense_rank() OVER (ORDER BY Hash_*)`**: determinístico por orden lexicográfico del hash; estable para el mismo conjunto de hashes; no preserva IDs si el conjunto de hashes cambia.
    - **C. `monotonically_increasing_id()`**: NO determinístico (depende de partición Spark). Descartado.
  - El proyecto es un laboratorio donde Tipo 1 (sin historia) es aceptable y la estabilidad requerida es "para el mismo input"; la opción B es suficiente y simple.
- **Implications**:
  - Diseño adopta `dense_rank() OVER (ORDER BY Hash_X)` — determinístico, sin dependencia de orden de procesamiento, sin tabla extra.
  - Requiere documentar en `SYSTEM.md` que los `DimId` no son estables si se reprocesa con un subconjunto distinto de hashes (consistente con Tipo 1 puro).

### Topic 4 — Política de DimIdOperacion en Hec_Transacciones_ATM
- **Context**: R5.6 exige resolver `DimIdOperacion` transitivamente. Si un cliente tiene N operaciones, ¿cuál asignar al hecho?
- **Sources Consulted**: `Link_Cliente_Operacion`, `Hub_Operacion` (tiene `SecuenciaSaldo`), `Sat_Operacion_FechasEvento`.
- **Findings**:
  - El grano del hecho es la transacción (`Hash_Transaccion`). Cada transacción no se relaciona directamente con una operación específica en el modelo de Plata: solo se relaciona con un cliente y, vía `Link_Cliente_Operacion`, ese cliente tiene 1..N operaciones.
  - Posibles políticas:
    - **P1. Última operación por SecuenciaSaldo desc**: refleja la "operación más reciente" del cliente al momento del análisis. Determinística.
    - **P2. Una fila por combinación cliente×operación**: explota el grano (multiplica filas). Cambia semántica del hecho.
    - **P3. NULL si hay ambigüedad**: requiere expectation drop, reduce cobertura.
- **Implications**:
  - Diseño adopta **P1**: por cada `Hash_Cliente`, se selecciona la operación con `SecuenciaSaldo` más alta (criterio determinístico ya disponible en `Hub_Operacion`); empate se rompe por `Hash_Operacion` lexicográfico.
  - Documentado como decisión funcional en design.md y reflejado en `SYSTEM.md` (R1).
  - Si una transacción ATM proviene de un cliente sin operación en `Link_Cliente_Operacion`, el hecho asigna `DimIdOperacion = NULL` (warn, no fail) — expectation `expect`.

### Topic 5 — Elegibilidad de incremental refresh para Sat_Transaccion_Montos como fuente
- **Context**: R2.7 exige que la fuente de `Dim_Tiempo` sea una tabla Delta soportada por LSDP.
- **Sources Consulted**: `LSDPPlataSatTransaccion.py` (declaración con `dp.create_streaming_table()`), table properties (CDF habilitado).
- **Findings**:
  - `Sat_Transaccion_Montos` es una Streaming Table del propio pipeline LSDP, registrada en Unity Catalog, en formato Delta, con CDF habilitado. Cumple los requisitos de fuente para MV con incremental refresh.
  - LSDP gestiona automáticamente la dependencia entre la ST `Sat_Transaccion_Montos` y la MV `Dim_Tiempo`.
- **Implications**: La fuente es válida sin trabajo adicional. No se requiere materialización intermedia.

### Topic 6 — Estructura del módulo de utilidad de Oro
- **Context**: Decidir si extender `LSDPUtilidadPrincipal.py` o crear módulo nuevo (gap analysis Opción C).
- **Sources Consulted**: `LSDPUtilidadPrincipal.py` (250 líneas, foco Plata Append-Only), patrón de tests existente, principios de Single Responsibility.
- **Findings**:
  - `LSDPUtilidadPrincipal.py` está orientado al patrón Append-Only de Plata (ST + AppendFlow). Los helpers de Oro (snapshot Tipo 1 + DimId) tienen otra semántica.
  - Crear `LSDPUtilidadOro.py` aísla responsabilidades, facilita tests independientes y mantiene `LSDPUtilidadPrincipal.py` estable para Plata.
- **Implications**: Diseño adopta módulo nuevo `utilities/LSDPUtilidadOro.py` con cuatro helpers principales (ver design.md componentes).

## Architecture Pattern Evaluation

| Option | Description | Strengths | Risks / Limitations | Notes |
|--------|-------------|-----------|---------------------|-------|
| Star Schema clásico (Kimball Tipo 1) | Dimensiones sin historia + hecho central | Simple, alineado con BI; encaja con Tipo 1 que pide R3/R4 | Pierde historia (aceptable en este laboratorio) | **Seleccionado** |
| Star Schema con SCD Tipo 2 | Dimensiones con `FechaInicio`/`FechaFin` y vigencia | Preserva historia analítica | Complejidad alta; no requerido por requirements; conflicto con MV | Descartado |
| Wide Fact Table (sin dimensiones) | Hecho denormalizado con todos los atributos | Lecturas simples | Anti-patrón Kimball; impide análisis dimensional | Descartado |
| Galaxy Schema (varios hechos) | Múltiples tablas de hechos compartiendo dimensiones | Extensibilidad futura | Fuera de alcance — solo se requiere Hec_Transacciones_ATM | Diferido |

## Design Decisions

### Decision: Naming PascalCase en Oro con renombrado explícito
- **Context**: Plata usa snake_case; estándar Star Schema usa PascalCase; los requirements y `SYSTEM.md` referencian PascalCase.
- **Alternatives Considered**:
  1. Propagar snake_case a Oro (consistente con Plata, rompe convención BI).
  2. Renombrar a PascalCase al construir cada MV de Oro.
  3. Crear vistas de traducción intermedias.
- **Selected Approach**: Opción 2 — `select(F.col("fecha_transaccion").alias("FechaTransaccion"), …)` en cada notebook de Oro.
- **Rationale**: Mantiene Plata sin cambios, alinea Oro con estándares dimensionales, evita capas extra.
- **Trade-offs**: Cada notebook de Oro contiene una capa explícita de renombrado; pequeña duplicación tolerable y trazable.
- **Follow-up**: Documentar el mapeo snake_case→PascalCase en `SYSTEM.md` Oro y validar nombres en tests.

### Decision: Asignación de DimId con `dense_rank() OVER (ORDER BY Hash_*)`
- **Context**: Necesidad de surrogate IDs estables sin tabla persistente.
- **Alternatives Considered**: Lookup persistente (1), dense_rank por hash (2), monotonically_increasing_id (3, descartado).
- **Selected Approach**: Helper `asignar_dim_id_estable(df, hash_col, id_col)` que aplica `dense_rank() OVER (ORDER BY hash_col)`.
- **Rationale**: Determinístico, sin estado externo, suficiente para Tipo 1.
- **Trade-offs**: IDs cambian si el conjunto de hashes de entrada cambia (aceptable para Tipo 1).
- **Follow-up**: Asegurar pruebas que validen estabilidad para el mismo input.

### Decision: DimIdOperacion en hechos = "operación más reciente del cliente por SecuenciaSaldo desc"
- **Context**: 1:N entre cliente y operación; transacciones no apuntan directamente a una operación.
- **Alternatives Considered**: Última por SecuenciaSaldo (P1), explosión 1:N (P2), NULL (P3).
- **Selected Approach**: P1 — `ROW_NUMBER() OVER (PARTITION BY Hash_Cliente ORDER BY SecuenciaSaldo DESC, Hash_Operacion ASC) = 1` para resolver la operación dominante.
- **Rationale**: Determinístico, mantiene grano del hecho, refleja la operación vigente al cliente.
- **Trade-offs**: No es estricto "operación de la transacción"; es "operación más reciente del cliente". Documentado.
- **Follow-up**: Si una transacción no tiene operación asociada → `DimIdOperacion = NULL` con expectation `expect` (warn).

### Decision: Refresh incremental solo en `Dim_Tiempo`; refresh completo en las demás MV
- **Context**: Operadores no soportados (ROW_NUMBER, joins múltiples) impiden incremental en otras MVs.
- **Alternatives Considered**: Forzar incremental con MV materializada por etapas (descartado por complejidad y costo).
- **Selected Approach**: `Dim_Tiempo` con `select`+`distinct`+`withColumn` determinístico. `Dim_Cliente`, `Dim_Operacion`, `Hec_Transacciones_ATM` con refresh completo.
- **Rationale**: Cumple R2 (incremental para `Dim_Tiempo`), ajusta expectativa para las demás. Volumen del laboratorio bajo.
- **Trade-offs**: Costo computacional de refrescar 3 MVs completas en cada ejecución del pipeline.
- **Follow-up**: Si en producción crece el volumen, considerar particionamiento por fecha o ST con append flows en `Hec_*`.

### Decision: Crear módulo `LSDPUtilidadOro.py` independiente
- **Context**: Aislar lógica de Oro de la de Plata.
- **Alternatives Considered**: Extender `LSDPUtilidadPrincipal.py` (Opción A del gap), módulo separado (Opción B/C del gap).
- **Selected Approach**: Módulo nuevo `utilities/LSDPUtilidadOro.py`.
- **Rationale**: Single Responsibility; tests independientes; cero impacto en Plata.
- **Trade-offs**: Un archivo más para descubrir.
- **Follow-up**: Tests `tests/test_utilidad_oro.py` con cobertura unitaria de cada helper.

### Decision: Ajustes derivados de la validación de diseño (2026-04-25)

Tras `/kiro-validate-design`, el usuario aprobó tres ajustes adicionales que se propagan al diseño:

- **Lista cerrada de columnas en `Dim_Cliente` y `Dim_Operacion`**: `design.md` define tablas exhaustivas con `Columna Oro · Tipo · Origen Plata · Satellite Plata` para cada dimensión, eliminando los "etc." abiertos. Habilita paralelizar la implementación de las dimensiones (tareas 3.2 y 3.3) sin riesgo de divergencia.
- **Severidad de la expectativa `Anio BETWEEN 1900 AND 2100` en `Dim_Tiempo`**: degradada a `expect` (warn). Se preservan como `expect_all_or_fail` únicamente `FechaClave IS NOT NULL` y `Mes BETWEEN 1 AND 12`. Razón: un único valor atípico en `fecha_transaccion` no debe abortar el pipeline ni bloquear el refresh incremental autocontenido. R2.9 actualizado en `requirements.md`.
- **Lectura directa de Satellites transaccionales en `Hec_Transacciones_ATM`**: `Sat_Transaccion_DatosEstables` y `Sat_Transaccion_Montos` se leen con `spark.read.table` directo, sin aplicar `obtener_ultimo_por_hash`, porque son transaccionales (una fila por `Hash_Transaccion` garantizada por `procesar_satellite_transaccional` en Plata). Se añade una expectativa `expect("unicidad_por_hash_transaccion", ...)` en el hecho como red de seguridad. El helper `obtener_ultimo_por_hash` queda **reservado para Satellites de estado** (Cliente/Operación). NO aplica a Sats de Cliente ni Operación: ahí sigue siendo obligatorio.

### Decision: Refinamientos derivados de la segunda validación de diseño (2026-04-25)

Tras la segunda ejecución de `/kiro-validate-design`, el usuario aprobó tres refinamientos adicionales:

- **Unicidad por hash de transacción implementada con `expect_or_drop` sobre columna marcador**: en `Hec_Transacciones_ATM`, antes de la proyección final, se calcula `_marca_duplicado` con `row_number().over(Window.partitionBy("Hash_Transaccion").orderBy("Hash_Diferenciador"))` y se declara `expect_or_drop("unicidad_por_hash_transaccion", "_marca_duplicado = 1")`. Esta formulación evita window functions dentro del predicado de la expectation (no soportadas por LSDP) delegando la evaluación al `withColumn` previo. La columna marcador se elimina del esquema final.
- **Join base obligatorio en `Dim_Cliente` y `Dim_Operacion`**: el diseño documenta explícitamente que `Hub_Cliente` y `Hub_Operacion` son los orígenes base del join (aportan los hashes y la llave de negocio); cada Satellite de estado se reduce a su última versión con `obtener_ultimo_por_hash` antes de un LEFT JOIN desde el Hub para preservar entidades sin Sat opcional.
- **Convención de booleanos en Oro**: nueva sección en `design.md` (`## Data Models → Convención de booleanos en Oro`) establece que `BooleanType` nativo se reserva a banderas calculadas en Oro (`EsFinSemana`, `EsRetiro`, `EsDeposito`); los clasificadores categóricos provenientes de Plata (`IndicadorVip`, `IndicadorSobregiro`, `EstadoUtilizacionCredito`, `CategoriaSaldo`) se preservan como `StringType`.

### Decision: Cierre de la tercera validación de diseño (2026-04-25)

Tras la tercera ejecución de `/kiro-validate-design`, el usuario aprobó tres ajustes documentales finales:

- **Lista cerrada de columnas en `Hec_Transacciones_ATM`**: convertida a tabla exhaustiva (`Columna Oro · Tipo · Origen · Notas`) en `design.md` con declaración explícita de que cualquier columna interna (incluida `_marca_duplicado`) DEBE eliminarse en el `select` final. Cierra el riesgo de propagación de metadata interna al esquema publicado en UC.
- **Aclaración al texto de R5.3**: nota añadida tanto en `requirements.md` como en `design.md` (§NotebookHecTransaccionesATM → Batch/Job Contract) explicando que la frase "tomando el último registro por `Hash_Transaccion` cuando aplique" se interpreta como salvaguarda contractual implementada con `expect_or_drop` sobre la columna marcador, NO como aplicación del helper `obtener_ultimo_por_hash`. Los Sats transaccionales son únicos por `Hash_Transaccion` por construcción de Plata.
- **Lineage Hec↔Dims confirmado por LSDP**: añadida nota explícita en `design.md` (§Data Models → Domain Model → Business rules) confirmando que el motor declarativo infiere y orquesta el orden topológico de refresh automáticamente a partir de las llamadas `spark.read.table(...)`, garantizando que las dimensiones se refresquen antes que el hecho dentro de la misma ejecución. No se requiere validación manual de cobertura ni job aparte.

## Risks & Mitigations

> **Estado**: todas las mitigaciones de esta sección fueron **aprobadas por el usuario el 2026-04-25** y se propagan al diseño como decisiones formales (ver `design.md` → Error Handling, Migration Strategy y notas por componente).

- **Riesgo R-01 — Restricción futura de `DISTINCT` para incremental refresh**: Si LSDP Free Edition restringe `DISTINCT` para incremental refresh en una versión futura, `Dim_Tiempo` cae a refresh completo.
  - **Mitigación aprobada**: la MV es pequeña (una fila por fecha distinta); refresh completo seguirá siendo viable. No se introduce lógica defensiva adicional. Reflejado en `design.md` → NotebookDimTiempo (Implementation Notes/Risks).
- **Riesgo R-02 — Supuesto "operación dominante por cliente"**: La política "operación más reciente" para `DimIdOperacion` puede asignar una operación distinta a la real para transacciones históricas.
  - **Mitigación aprobada**: (1) documentar el supuesto explícitamente en `SYSTEM.md` (sección Oro/Hec_Transacciones_ATM) como parte de la tarea P0 de DocumentationUpdate; (2) tratar la incorporación de un identificador de operación en `TRXPFL` como mejora futura fuera del alcance actual. Reflejado en `design.md` → Error Handling y NotebookHecTransaccionesATM (Implementation Notes/Risks).
- **Riesgo R-03 — Inestabilidad de `DimId*` ante cambio del conjunto de hashes**: `dense_rank` produce IDs distintos si cambia el conjunto de hashes.
  - **Mitigación aprobada**: (1) documentar como propiedad esperada de Tipo 1 en `SYSTEM.md`; (2) regla de consumo BI: ningún reporte ni constante hard-coded debe depender de un valor literal de `DimIdCliente`/`DimIdOperacion`; (3) test de estabilidad para el mismo input cubierto por R8.4. Reflejado en `design.md` → Data Models (Business rules & invariants) y Testing Strategy.
- **Riesgo R-04 — Referencias dispersas en docs/steering al cambiar `SYSTEM.md`**: Cambios en `SYSTEM.md` (R1) podrían romper referencias en otros specs/steering.
  - **Mitigación aprobada**: tarea P0 de DocumentationUpdate ejecuta búsquedas regex (`Dim_Tiempo`, `current_date`, `spark.range`) en todo el repositorio antes y después de la edición, y reporta cero coincidencias residuales antes de pasar a P1. Reflejado en `design.md` → DocumentationUpdate (Implementation Notes) y Migration Strategy (validation checkpoints).

## References
- Databricks Lakeflow Spark Declarative Pipelines — Materialized Views (https://docs.databricks.com/aws/en/dlt/) — operadores soportados para incremental refresh.
- Kimball, R. — *The Data Warehouse Toolkit* — convenciones Star Schema, SCD Tipo 1, surrogate keys.
- Repositorio interno: `SYSTEM.md`, `.kiro/steering/tech.md`, `.kiro/steering/structure.md`, `LSDPPlataSatTransaccion.py`, `LSDPUtilidadPrincipal.py`.
