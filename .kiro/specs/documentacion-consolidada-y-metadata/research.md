# Research & Design Decisions — `documentacion-consolidada-y-metadata`

## Summary
- **Feature**: `documentacion-consolidada-y-metadata`
- **Discovery Scope**: Extension (brownfield documentation + un nuevo notebook de metadatos sobre un pipeline LSDP existente)
- **Key Findings**:
  - El código ya implementa estrategias mixtas en Plata (AUTO CDC SCD=1 para `Hub_Cliente`, `Hub_Operacion`, `Link_Cliente_Operacion`; `@dp.append_flow()` para `Hub_Transaccion`, `Link_Cliente_Transaccion`, todos los Satellites). La documentación debe partir de esta realidad, no de un diseño teórico.
  - Oro usa una `@dp.table(temporary=True)` (`Trx_ATM_Stream`) y dependencias intermedias (`Map_Cliente_Operacion_Dominante`, `Trx_ATM_Enriquecida`) cuyo razonamiento técnico (cost model `CHANGESET_SIZE_THRESHOLD_EXCEEDED`, `NUM_JOINS_THRESHOLD_EXCEEDED`) es crítico y debe ser explicado en el Manual Técnico.
  - Databricks Free Edition Serverless impone restricciones (sin `cache`, sin RDD, sin UDFs, ANSI Mode on) que el notebook `NbComentariosTablas.py` debe respetar; los comandos `COMMENT ON` y `ALTER TABLE … ALTER COLUMN … COMMENT` están soportados sobre Delta y cumplen idempotencia nativamente (se sobrescriben).

## Research Log

### Estrategia LSDP por entidad (revisión código real)

- **Context**: Confirmar qué decoradores y patrones se usan por entidad para que la documentación refleje exactamente el código.
- **Sources Consulted**: `src/LSDP_Lab_DataVault_DWH/transformations/*.py`, `.kiro/steering/tech.md`, `SYSTEM.md`.
- **Findings**:
  - Bronce (`CMSTFL`, `TRXPFL`, `BLNCFL`): `@dp.table` persistente con AutoLoader directo y `cluster_by=["FechaRegistroParquet"]`.
  - Plata Hubs/Link OPT-001 (`Hub_Cliente`, `Hub_Operacion`, `Link_Cliente_Operacion`): `dp.create_streaming_table()` + `@dp.view` + `dp.create_auto_cdc_flow(stored_as_scd_type=1)`.
  - Plata otros (`Hub_Transaccion`, `Link_Cliente_Transaccion`, todos los `Sat_*`): `dp.create_streaming_table()` + `@dp.append_flow()` con helpers `procesar_hub`, `procesar_link`, `procesar_satellite`, `procesar_satellite_transaccional`.
  - Plata vista CDF: `LSDPPlataVistaTRXPFLCDF.py` expone CDF de TRXPFL para alimentar Plata.
  - Oro: `Dim_Cliente`, `Dim_Operacion`, `Dim_Tiempo`, `Map_Cliente_Operacion_Dominante`, `Trx_ATM_Enriquecida` (Streaming Table temporal `temporary=True`), `Hec_Transacciones_ATM` (Materialized View).
- **Implications**: La documentación debe enumerar los 18 notebooks reales y sus decoradores correspondientes; el Manual Técnico debe explicar el porqué de la `temporary=True` y de evitar joins en la MV.

### Soporte de `COMMENT` en Unity Catalog Serverless

- **Context**: Validar que `COMMENT ON TABLE`/`ALTER TABLE … ALTER COLUMN … COMMENT` funcionan sobre Streaming Tables, Materialized Views y Vistas en Free Edition Serverless.
- **Sources Consulted**: Documentación oficial Databricks SQL `COMMENT ON`, `ALTER TABLE`, `ALTER VIEW`; `SYSTEM.md` (sección Stack Técnico).
- **Findings**:
  - `COMMENT ON TABLE catalogo.esquema.objeto IS 'texto'` aplica tanto a tablas Delta (Streaming Tables) como a Materialized Views administradas por Unity Catalog.
  - Para vistas (incluidas Materialized Views) también se puede usar `ALTER VIEW … SET TBLPROPERTIES` con `comment` o el `COMMENT ON`.
  - Comentarios de columna: `ALTER TABLE … ALTER COLUMN <col> COMMENT '<texto>'` para tablas; en Materialized Views los comentarios de columna deben definirse en su definición; cuando no se puede vía ALTER, se usa la forma de `COMMENT ON COLUMN catalogo.esquema.objeto.columna IS '…'` soportada en Unity Catalog.
  - Estos comandos son idempotentes: ejecutarlos varias veces sustituye el comentario sin error.
- **Implications**: El notebook puede usar exclusivamente `spark.sql()` con dos sentencias por columna/tabla, capturando excepciones por tabla inexistente (caso "pipeline no ejecutado todavía").

### Versionado de templates de spec y plan de alineación

- **Context**: El usuario solicita preservar historial al alinear specs con el código.
- **Sources Consulted**: `.kiro/specs/{bronce-utilities-ingesta,correccion-arquitectura-bronce-plata,oro-modelo-estrella-mv-tiempo,plata-data-vault-notebooks}/spec.json` y artefactos.
- **Findings**: Los specs históricos están aprobados (`ready_for_implementation: true`). Sobrescribirlos rompería la trazabilidad histórica.
- **Implications**: El Plan de Alineación se aloja **dentro del spec activo** (`.kiro/specs/documentacion-consolidada-y-metadata/PlanAlineacionSpecs.md`) y cada spec histórico recibe únicamente un anexo `CHANGELOG.md` con divergencias y referencias, sin tocar `requirements.md`, `design.md` ni `tasks.md` originales.

## Architecture Pattern Evaluation

| Option | Description | Strengths | Risks / Limitations | Notes |
|--------|-------------|-----------|---------------------|-------|
| Documentación inline en notebooks (docstrings) | Llevar todo el catálogo a docstrings de cada `@dp.*` | Cercano al código | No es navegable como catálogo, no permite diagramas Mermaid, no aporta para stakeholders | Insuficiente para Req 3/4/5 |
| Single-file `docs/Documentacion.md` | Un solo Markdown gigante con todo | Una sola fuente | Mezcla audiencias (analista vs ingeniero vs nuevo usuario), > 2000 líneas | Rechazado |
| **Tres documentos especializados en `docs/` + Notebook de metadatos** | Modelo, Manual, Quickstart separados; comentarios persistidos en UC vía notebook idempotente | Audiencias diferenciadas, navegable, tablas/diagramas focalizados, comentarios visibles en Catalog Explorer | Requiere coherencia cruzada explícita | **Seleccionado** — alineado a Req 3/4/5/6 |

## Design Decisions

### Decision: Catálogo único de columnas como fuente para `ModeloDatos.md` y `NbComentariosTablas.py`

- **Context**: Req 6.6 exige que comentarios del notebook deriven del catálogo de `ModeloDatos.md`. Mantener dos catálogos separados invita a inconsistencias.
- **Alternatives Considered**:
  1. Catálogo en YAML/JSON separado (`docs/catalogo_columnas.yml`) consumido por el notebook + render a Markdown.
  2. Catálogo embebido en `ModeloDatos.md`; el notebook contiene un diccionario Python equivalente y un test de paridad.
  3. Catálogo embebido en notebook y Markdown lo importa por extracción.
- **Selected Approach**: Opción 2. Markdown sigue siendo la fuente humana primaria; el notebook embebe un `dict` Python `COMENTARIOS_TABLAS` y `COMENTARIOS_COLUMNAS` con la misma información. Una verificación visual de paridad se documenta en `docs/ModeloDatos.md` (sección "Sincronización con `NbComentariosTablas.py`").
- **Rationale**: Free Edition Serverless no expone parsers Markdown del lado del notebook; añadir YAML/JSON suma archivos sin valor para el laboratorio. Mantener un dict inline es legible y revisable en code review.
- **Trade-offs**: Mantener dos artefactos exige disciplina en revisiones; mitigado con pull request checklist (Req 7.4).
- **Follow-up**: Añadir en `tasks.md` una tarea explícita de paridad catálogo↔dict.

### Decision: Separación estricta de manuales por audiencia (`ModeloDatos`, `ManualTecnico`, `Quickstart`)

- **Context**: Tres entregables Markdown con audiencias y profundidades distintas (analistas, ingenieros, nuevos usuarios).
- **Alternatives Considered**:
  1. Un solo `README` extendido en `docs/`.
  2. Tres documentos independientes con cross-links.
- **Selected Approach**: Opción 2.
- **Rationale**: Requisitos 3/4/5 demandan estilos distintos (catálogo, didáctico, paso a paso).
- **Trade-offs**: Más archivos a mantener; mitigado con encabezados estandarizados (Req 7.7).

### Decision: Plan de Alineación dentro del spec activo + CHANGELOG por spec histórico

- **Context**: Req 2 pide preservar specs históricos.
- **Alternatives Considered**:
  1. Reescribir cada spec histórico.
  2. Mover specs viejos a `archive/`.
  3. Anexar `CHANGELOG.md` por spec histórico y centralizar el plan en el spec activo.
- **Selected Approach**: Opción 3.
- **Rationale**: Mantiene `requirements/design/tasks` originales intactos (auditabilidad) y centraliza el análisis de divergencia.
- **Trade-offs**: Lectura cruzada por reviewer; aceptable.

### Decision: Captura de errores en `NbComentariosTablas.py` por entidad

- **Context**: Req 6.7 exige no abortar si una tabla no existe (p. ej., primera ejecución antes del LSDP).
- **Alternatives Considered**:
  1. `try/except` envolviendo todo el bloque por medalla.
  2. Helper `aplicar_comentarios(catalogo, esquema, tabla, columnas)` con `try/except` interno y log estructurado.
- **Selected Approach**: Opción 2.
- **Rationale**: Aísla fallas por tabla, permite ejecución parcial y produce log uniforme.
- **Trade-offs**: Más código de ayuda; aceptable y reutilizable.

## Risks & Mitigations

- **Riesgo R1 — Drift entre `ModeloDatos.md` y `NbComentariosTablas.py`**:
  - Mitigación aplicada en diseño: `ModeloDatos.md` debe incluir una sección fija "Sincronización con `NbComentariosTablas.py`" y el notebook debe abrir con una nota visible indicando que cualquier cambio en el catálogo exige actualizar ambos artefactos en la misma PR.
  - Mitigación operativa: `tasks.md` debe incluir una tarea explícita de paridad catálogo↔dict y la estrategia de pruebas debe exigir una revisión manual de pares `(tabla, columna)` entre ambos artefactos antes de cerrar el incremento.
- **Riesgo R2 — Diagramas Mermaid demasiado densos para GitHub render**:
  - Mitigación aplicada en diseño: un `erDiagram` por medalla y un `flowchart` macro separado para linaje; se prohíbe combinar las tres medallas en un mismo diagrama.
  - Mitigación operativa: usar nombres cortos de nodos, sin paréntesis, `@` ni corchetes, y añadir verificación de render en la estrategia de validación documental.
- **Riesgo R3 — Documentación de Quickstart obsoleta por cambios de UI Databricks**:
  - Mitigación aplicada en diseño: Quickstart debe describir pasos por concepto estable (crear Git Folder en el directorio del workspace del usuario, configurar pipeline LSDP, ejecutar notebooks) y no por navegación dependiente de pantallas o screenshots.
  - Mitigación operativa: cada versión de `Quickstart.md` debe declarar una fecha ISO 8601 de última verificación y una nota breve indicando que la UI puede cambiar sin alterar el flujo conceptual.
- **Riesgo R4 — `COMMENT ON COLUMN` no soportado sobre Materialized Views**:
  - Mitigación aplicada en diseño: el notebook debe ejecutar una secuencia de fallback por columna: primero `ALTER TABLE ... ALTER COLUMN ... COMMENT`; si el motor rechaza el objeto por tipo, reintentar con `COMMENT ON COLUMN ... IS ...`; si ambos fallan, registrar `SKIPPED` y continuar.
  - Mitigación operativa: la estrategia de pruebas debe incluir validación específica sobre objetos de Oro susceptibles a ser Materialized Views o Views para confirmar que el fallback queda cubierto y que la ejecución no aborta.

## References

- [Databricks SQL — `COMMENT ON`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-comment-on.html) — sintaxis de comentarios sobre tabla/columna/catálogo en UC.
- [Databricks SQL — `ALTER TABLE`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-table.html) — `ALTER COLUMN ... COMMENT`.
- [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/) — decoradores `@dp.table`, `@dp.materialized_view`, `dp.create_auto_cdc_flow`.
- [Databricks Free Edition](https://docs.databricks.com/aws/en/free-edition/) — limitaciones de cómputo Serverless.
- `SYSTEM.md` — Single Source of Truth del proyecto (versión vigente del repo).
