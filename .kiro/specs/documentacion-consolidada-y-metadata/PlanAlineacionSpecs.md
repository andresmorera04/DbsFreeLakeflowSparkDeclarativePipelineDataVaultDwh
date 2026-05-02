# Plan de Alineación de Specs con el Código Real

**Proyecto**: LSDP Lab DataVault DWH  
**Fecha**: 2026-05-01  
**Incremento SDD**: `documentacion-consolidada-y-metadata`  
**Autor**: LSDP Lab DataVault DWH  
**Spec activo**: documentacion-consolidada-y-metadata  
**Plataforma**: Databricks Free Edition · Serverless Compute · Unity Catalog  
**Referencia spec activo**: [spec.json](./spec.json)

---

## Propósito

Este documento registra las divergencias detectadas entre los artefactos aprobados de los cuatro
specs históricos y el comportamiento real de la implementación. Cada divergencia incluye el
artefacto afectado, descripción, impacto técnico y acción correctiva aplicada o propuesta.

> **Importante**: Los artefactos históricos aprobados (requirements.md, design.md, tasks.md) de
> cada spec **no son modificados**. Las correcciones se documentan aquí y en los CHANGELOG.md de
> cada spec histórico.

---

## Spec 1: `bronce-utilities-ingesta`

**Propósito original**: Definir la ingesta incremental de Bronce con AutoLoader y las utilidades
compartidas del pipeline (`LSDPConfiguracion.py`, `LSDPUtilidadPrincipal.py`).

### Divergencias detectadas

| ID | Artefacto afectado | Descripción | Impacto | Acción correctiva |
|----|-------------------|-------------|---------|-------------------|
| B1-D1 | `design.md` — Parámetros del pipeline | Design documenta `pipeline.ruta_base` + `pipeline.ruta_base_autoloader` como parámetros únicos. Implementación real: 6 parámetros de rutas granulares (`pipeline.ruta_cmstfl`, `pipeline.ruta_trxpfl`, `pipeline.ruta_blncfl`, `pipeline.schema_location_cmstfl`, `pipeline.schema_location_trxpfl`, `pipeline.schema_location_blncfl`) derivados de `pipeline.volumen` | Medio — Los valores de configuración del pipeline no coinciden con la documentación de referencia | Documentado en SYSTEM.md sección Parámetros; CHANGELOG.md en este spec |
| B1-D2 | `design.md` — Descripción de `procesar_satellite_transaccional()` | El design describe la función como deduplicadora de Satellites de transacción. La implementación real en `LSDPPlataSatTransaccion.py` usa `@dp.append_flow()` puro sin invocar esta función | Bajo — La función existe en `LSDPUtilidadPrincipal.py` pero no es llamada desde los notebooks; la deduplicación la provee CDF + unicidad de TRXID | Documentado en SYSTEM.md; función preservada en utilidades por compatibilidad |
| B1-D3 | `requirements.md` — Columna `_rescued_data` | Requerimiento menciona esta columna como explícita en el esquema. En AutoLoader es generada automáticamente como parte del schema evolution, no se declara manualmente | Bajo — Comportamiento correcto pero descripción imprecisa | Documentado en SYSTEM.md sección Bronce |

---

## Spec 2: `correccion-arquitectura-bronce-plata`

**Propósito original**: Corregir la arquitectura de Bronce eliminando el patrón ST temporal + MV
snapshot, y unificar el patrón de Plata con `dp.create_streaming_table()` + flujos.

### Divergencias detectadas

| ID | Artefacto afectado | Descripción | Impacto | Acción correctiva |
|----|-------------------|-------------|---------|-------------------|
| BP-D1 | `design.md` — Hub_Transaccion y Link_Cliente_Transaccion | Design describe `procesar_hub()` y `procesar_link()` con LEFT ANTI JOIN como estrategia de deduplicación para estas entidades. La implementación real usa `@dp.append_flow()` puro sobre `vista_trxpfl_cdf` (CDF), que provee deduplicación natural por semántica del Change Data Feed | Medio — Las funciones `procesar_hub()` y `procesar_link()` existen y son correctas funcionalmente, pero los notebooks de transacción no las invocan. La fuente CDF garantiza unicidad estructuralmente | Las funciones se preservan en `LSDPUtilidadPrincipal.py`; notebooks usan CDF como fuente directa |
| BP-D2 | `design.md` — OPT-001 (Auto CDC SCD=1) | Design propone OPT-001 como optimización opcional. En la implementación final, OPT-001 se aplica a Hub_Cliente, Hub_Operacion y Link_Cliente_Operacion como estrategia definitiva, no opcional | Bajo — Mejora de claridad; el diseño fue aprobado con esta estrategia aunque el texto la presentó como "opción" | Documentado en SYSTEM.md y `product.md` de steering como estrategia definitiva |
| BP-D3 | `tasks.md` — Clustering keys de Link_Cliente_Operacion | Tasks menciona `["Hash_Cliente", "Hash_Operacion"]`. Implementación real: `["Hash_Cliente", "Hash_Operacion", "FechaRegistro"]` | Bajo — Diferencia de 1 columna en clustering; impacto en rendimiento de queries mínimo | Documentado en este plan; SYSTEM.md refleja la implementación real |

---

## Spec 3: `oro-modelo-estrella-mv-tiempo`

**Propósito original**: Implementar el modelo estrella de Oro con dimensiones Tipo 1 y tabla de
hechos usando Vistas Materializadas con refresh incremental.

### Divergencias detectadas

| ID | Artefacto afectado | Descripción | Impacto | Acción correctiva |
|----|-------------------|-------------|---------|-------------------|
| ORO-D1 | `design.md` — Nombre de la tabla de hechos | Design original menciona `Fact_Transacciones_ATM`. Implementación real: `Hec_Transacciones_ATM` (patrón de nomenclatura `Hec_` adoptado consistentemente con el nombre del notebook `LSDPOroHecTransaccionesATM.py`) | Alto — Cualquier documentación o referencia que use `Fact_Transacciones_ATM` apunta a un objeto inexistente en Unity Catalog | Corregido en SYSTEM.md; documentado en `docs/ModeloDatos.md`; CHANGELOG.md en este spec |
| ORO-D2 | `design.md` — Trx_ATM_Stream | Design describe `Trx_ATM_Stream` como `@dp.table(temporary=True)`. Nombre correcto en implementación: `Trx_ATM_Stream` (coincide). Verificado sin divergencia | — | Sin acción requerida |
| ORO-D3 | `design.md` — Map_Cliente_Operacion_Dominante | Design describe esta MV temporal con `groupBy().agg(max(struct(...)))`. Implementación: `@dp.materialized_view(name="Map_Cliente_Operacion_Dominante", temporary=True)` usando `seleccionar_operacion_dominante()` de `LSDPUtilidadOro.py`. Coincide | — | Sin acción requerida |
| ORO-D4 | `design.md` — `asignar_dim_id_estable()` | Design describe llave subrogada con `F.abs(F.hash(...).cast("long"))`. Implementación real: `F.xxhash64(hash_col).cast("long")` (determinístico, sin `abs`, admite valores negativos por diseño) | Medio — El ID puede ser negativo; válido para llave subrogada interna | Documentado en SYSTEM.md (R12) y en `docs/ManualTecnico.md` |
| ORO-D5 | `design.md` — R10 (Dim_Tiempo) | Design inicial menciona "Streaming Table acumulativa" para Dim_Tiempo. Implementación real: `@dp.materialized_view` con refresh incremental (Enzyme CDF). Corrección B.2 aprobada | Medio — Tipo de tabla diferente; comportamiento final correcto y más eficiente | Documentado en SYSTEM.md (R10 corregido); CHANGELOG.md en este spec |

---

## Spec 4: `plata-data-vault-notebooks`

**Propósito original**: Implementar todos los notebooks de Plata (Hubs, Links, Satellites) con
el modelo Data Vault 2.0 y las utilidades correspondientes.

### Divergencias detectadas

| ID | Artefacto afectado | Descripción | Impacto | Acción correctiva |
|----|-------------------|-------------|---------|-------------------|
| PL-D1 | `design.md` — Corrección B.1 (Satellites transaccionales) | Design dice: `procesar_satellite_transaccional()` deduplica por `[hash_col, fecha_transaccion]`. Versión B.1 aprobada cambia a deduplicación por `[hash_col]` solo. Implementación real: `@dp.append_flow()` puro sin invocar ningún helper. La fuente CDF garantiza que solo llegan eventos nuevos del último commit | Alto — Descripción del mecanismo incompleta en el design; comportamiento funcional correcto por la garantía de unicidad de TRXID | Documentado en SYSTEM.md; `procesar_satellite_transaccional()` preservada en utilidades por compatibilidad; diseño descrito correctamente en `docs/ManualTecnico.md` |
| PL-D2 | `design.md` — Columnas de Sat_Transaccion_* | Design menciona "36 cols" para `Sat_Transaccion_DatosEstables`. Verificado en código: 34 cols de negocio + `Hash_Diferenciador` + `FechaRegistro` + `FuenteDatos` = 37 total (incluyendo `VersionCarga` y `FechaCargaBronce` de CDF) | Bajo — Discrepancia de conteo de columnas en documentación de referencia | Documentado en `docs/ModeloDatos.md` con conteos verificados del código |
| PL-D3 | `design.md` — `vista_trxpfl_cdf` como `@dp.view` | Design describe esta vista como fuente compartida para los 4 consumidores transaccionales. Verificado en `LSDPPlataVistaTRXPFLCDF.py`: `@dp.view(name="vista_trxpfl_cdf")`. Coincide | — | Sin acción requerida |
| PL-D4 | `design.md` — Liquid Clustering Hub_Transaccion | Design: `["FechaRegistro", "Hash_Transaccion"]`. Implementación: `["FechaRegistro", "Hash_Transaccion"]`. Coincide | — | Sin acción requerida |

---

## Resumen Ejecutivo de Alineación

| Spec | Divergencias detectadas | Críticas | Medias | Bajas | Estado |
|------|------------------------|---------|--------|-------|--------|
| `bronce-utilities-ingesta` | 3 | 0 | 1 | 2 | Documentado ✅ |
| `correccion-arquitectura-bronce-plata` | 3 | 0 | 2 | 1 | Documentado ✅ |
| `oro-modelo-estrella-mv-tiempo` | 5 | 1 | 2 | 2 | Documentado ✅ |
| `plata-data-vault-notebooks` | 4 | 1 | 1 | 2 | Documentado ✅ |
| **Total** | **15** | **2** | **6** | **7** | ✅ |

**Divergencias críticas resueltas**:
1. `ORO-D1`: `Fact_Transacciones_ATM` → `Hec_Transacciones_ATM` — corregido en SYSTEM.md y docs/
2. `PL-D1`: Mecanismo real de Sat_Transaccion_* — documentado en SYSTEM.md y docs/ManualTecnico.md

---

_Generado por cc-sdd durante el incremento `documentacion-consolidada-y-metadata` · 2026-05-01_
