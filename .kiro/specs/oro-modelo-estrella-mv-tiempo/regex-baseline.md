# Línea base regex pre-edición — `oro-modelo-estrella-mv-tiempo`

> **Tarea 1.1** ([tasks.md](tasks.md#L9)) · Gating **R-04** aprobado ([design.md §Risks](design.md#L762)).
> **Fecha de captura**: 2026-04-25T20:30:00Z.
> **Búsquedas ejecutadas en la raíz del repo** sobre los patrones definidos en `_Requirements: 1.5, 1.7_`:
> 1. `Dim_Tiempo`
> 2. `current_date`
> 3. `spark\.range` (regex)
> 4. `create_streaming_table.*Dim_Tiempo` (regex)

## Resumen ejecutivo

| Patrón | Total matches | A remediar | Legítimos (preservar) | Falsos positivos |
|--------|---------------|-----------|----------------------|------------------|
| `Dim_Tiempo` | 127 | 16 (en `SYSTEM.md`) + 7 (en spec `plata-data-vault-notebooks`) = 23 | 104 (spec actual + naming refs) | 0 |
| `current_date` | 21 | 5 (en `SYSTEM.md`) | 16 (spec actual + prohibiciones documentadas) | 0 |
| `spark.range` | 20 | 2 (en `SYSTEM.md`) | 13 (notebooks de generación de Parquets sintéticos + ejemplo permitido) | 5 (referencias informativas en spec actual) |
| `create_streaming_table.*Dim_Tiempo` | 6 | 0 (ya cero matches en `SYSTEM.md` con esa combinación específica regex) | 6 (refs en spec actual + spec plata) | 0 |

## A) Inventario a remediar (arquitectura previa)

Estos matches **DEBEN** desaparecer o reescribirse antes de cerrar la tarea 1.4 (re-ejecución de los regex con cero coincidencias residuales en este conjunto).

### A.1 `SYSTEM.md` — fuente principal del cambio (tarea 1.2)

| Línea | Tipo | Patrón | Acción esperada |
|-------|------|--------|-----------------|
| 208 | Tabla descriptora | `Dim_Tiempo \| Generada/Calculada` | Reescribir descripción a "MV incremental basada en `Sat_Transaccion_Montos.fecha_transaccion`" |
| 210 | "Regla especial para Dim_Tiempo" | `Dim_Tiempo` + lógica "día actual / día de ayer" | Reescribir regla a comportamiento incremental sin lógica de fechas explícita |
| 420 | Ejemplo código #1 | `name=f"{catalogo_oro}.{esquema_oro}.Dim_Tiempo"` (en `dp.create_streaming_table`) | Sustituir bloque completo por `@dp.materialized_view` |
| 430 | Ejemplo código #1 | `@dp.append_flow(target=...Dim_Tiempo)` | Eliminar (incluido en sustitución del bloque) |
| 431 | Ejemplo código #1 | `def cargar_dim_tiempo()` | Reescribir cuerpo de la función con `spark.read.table(Sat_Transaccion_Montos).select(...).distinct()...withColumn(...)` |
| 433 | Ejemplo código #1 | `spark.range(0, 2)` | Eliminar (lógica imperativa) |
| 435 | Ejemplo código #1 | `F.when(F.col("id") == 0, F.current_date())` | Eliminar |
| 436 | Ejemplo código #1 | `F.date_sub(F.current_date(), 1)` | Eliminar |
| 647 | Tabla compatibilidad Free Edition | `Streaming Tables ... Dim_Tiempo en Oro` | Retirar mención de `Dim_Tiempo` de la fila de Streaming Tables y agregar nota de MV incremental |
| 2393 | Encabezado de sección | `### Dim_Tiempo (Acumulativa — Streaming Table con Append Flow)` | Reescribir a `### Dim_Tiempo (Vista Materializada Incremental)` |
| 2419 | Ejemplo código #2 | `name=f"{catalogo_oro}.{esquema_oro}.Dim_Tiempo"` (en `dp.create_streaming_table`) | Sustituir bloque completo por `@dp.materialized_view` |
| 2424 | Ejemplo código #2 | `@dp.append_flow(target=...Dim_Tiempo)` | Eliminar |
| 2425 | Ejemplo código #2 | `def cargar_dim_tiempo()` | Reescribir |
| 2426 | Ejemplo código #2 | `hoy = F.current_date()` | Eliminar |
| 2431 | Ejemplo código #2 | `spark.range(0, 2)` | Eliminar |
| 2439 | Ejemplo código #2 | `existente = spark.read.table(...Dim_Tiempo)` | Eliminar (control de duplicados manual) |
| 2977-2978 | Listado descriptivo | "Verificar día actual (`current_date()`)" / "día de ayer" | Reescribir a refresh incremental automático |

### A.2 `.kiro/specs/plata-data-vault-notebooks/` — referencias derivadas (tarea 1.3)

Estas referencias citan el patrón de `Dim_Tiempo` (ST + Append Flow) como **justificación aprobada** del patrón ST+AppendFlow para los Satellites de Plata. La justificación arquitectónica de los Satellites de Plata sigue siendo válida (Append-Only requerido por DV2.0), pero la **referencia cruzada a `Dim_Tiempo`** debe reformularse para no perpetuar la arquitectura previa.

| Archivo | Línea | Contenido a reformular |
|---------|-------|------------------------|
| `design.md` | 117 | "Este es el mismo patrón aprobado en SYSTEM.md para `Dim_Tiempo` (R10)" → reformular sin mencionar `Dim_Tiempo` (justificar por DV2.0 directamente) |
| `research.md` | 11 | "(probado en Dim_Tiempo de SYSTEM.md)" → reformular |
| `research.md` | 33 | Sección "Dim_Tiempo acumulativa" en fuentes consultadas → actualizar referencia |
| `research.md` | 37 | "ya está probado y aprobado en SYSTEM.md para la `Dim_Tiempo` (Oro)" → reformular |
| `research.md` | 42 | "el patrón aprobado de `Dim_Tiempo` en SYSTEM.md" → reformular |
| `research.md` | 92 | "patrón probado en Dim_Tiempo (SYSTEM.md)" → reformular |
| `research.md` | 111 | "Es el mismo patrón aprobado para `Dim_Tiempo` en SYSTEM.md (R10)" → reformular |

## B) Matches legítimos — PRESERVAR (no cuentan como residuales en gating R-04)

### B.1 Spec actual `oro-modelo-estrella-mv-tiempo`
Todas las menciones a `Dim_Tiempo`, `current_date`, `spark.range` dentro de [requirements.md](requirements.md), [design.md](design.md), [research.md](research.md), [tasks.md](tasks.md) y [gap-analysis.md](gap-analysis.md) **describen explícitamente el cambio arquitectónico** (la nueva MV, las prohibiciones, las mitigaciones de riesgo, los tests previstos). Total: ~100 matches. **PRESERVAR íntegramente**.

### B.2 `SYSTEM.md` — referencias estructurales sin arquitectura previa
| Línea | Contexto | Justificación |
|-------|---------|---------------|
| 610 | Tabla "APIs prohibidas en Serverless": `spark.range()` listado como **alternativa permitida** a RDDs | Documentación correcta sin relación a `Dim_Tiempo` |
| 757 | Ejemplo `df = spark.range(0, 1000)` ilustrando un caso permitido | Documentación correcta |
| 2854 | Diagrama ASCII del modelo estrella mostrando `Dim_Tiempo` como nodo | Solo nomenclatura, sin arquitectura |
| 2969 | Tabla de PKs: `Dim_Tiempo \| FechaClave` | Solo nomenclatura |
| 3040 | Expectativa E9 sobre FK del hecho hacia `Dim_Tiempo` | Solo nomenclatura |
| 3110 | Tabla de naming convention: ejemplo `Dim_Tiempo` | Solo nomenclatura |
| 3140 | Tabla de notebooks: `05_Oro_Dimensiones` incluye `Dim_Tiempo` | Solo nomenclatura |

### B.3 `.kiro/steering/structure.md`
| Línea | Contexto | Justificación |
|-------|---------|---------------|
| 51 | Tabla naming Oro: ejemplo `Dim_Tiempo` | Solo nomenclatura, coherente con el cambio |

### B.4 Notebooks de generación de Parquets sintéticos (`src/.../explorations/GenerarParquets/`)
| Archivo | Líneas | Justificación |
|---------|--------|---------------|
| `NbGenerarTransaccionalCliente.py` | 293 | `spark.range(1, n+1)` para generar IDs sintéticos en exploración. Fuera del scope LSDP |
| `NbGenerarMaestroCliente.py` | 338, 426, 427, 665 | Idem |

Estos notebooks **no son parte del pipeline LSDP** (son scripts de generación de datos de laboratorio). El uso de `spark.range()` en ellos es legítimo y **se preserva**.

## C) Verificación post-edición (criterio de cierre tarea 1.4)

Tras completar las tareas 1.2 y 1.3, re-ejecutar las cuatro búsquedas y verificar:

- ✅ `SYSTEM.md`: cero matches del inventario A.1 (deben quedar solo refs en B.2).
- ✅ `.kiro/specs/plata-data-vault-notebooks/`: cero matches a "Dim_Tiempo" como patrón aprobado (refs A.2 reformuladas).
- ✅ Total de matches `Dim_Tiempo` en repo ≤ B.1 (~100) + B.2 (7) + B.3 (1) + cualquier nueva mención coherente con MV incremental.
- ✅ Total de matches `current_date` en repo ≤ refs preservadas en spec actual (~16 en B.1, todas describiendo prohibiciones).
- ✅ Total de matches `spark.range` en repo ≤ B.1 (5 informativos) + B.2 (2 permitidos) + B.4 (5 notebooks de generación).
- ✅ `create_streaming_table.*Dim_Tiempo`: ya está en cero en `SYSTEM.md` con esa combinación específica (verificar que no reaparece tras la edición).
