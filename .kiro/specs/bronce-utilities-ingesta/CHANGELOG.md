# CHANGELOG — bronce-utilities-ingesta

**Proyecto**: LSDP Lab DataVault DWH  
**Autor**: LSDP Lab DataVault DWH  
**Fecha**: 2026-05-01  
**Spec activo**: documentacion-consolidada-y-metadata  
**Plataforma**: Databricks Free Edition · Serverless Compute · Unity Catalog

## [2026-05-01] Alineación post-implementación

**Incremento de referencia**: `documentacion-consolidada-y-metadata`  
**Plan de alineación**: [PlanAlineacionSpecs.md](../documentacion-consolidada-y-metadata/PlanAlineacionSpecs.md)

### Divergencias documentadas (sin modificar artefactos aprobados)

- **B1-D1** — Parámetros del pipeline: el design describía `pipeline.ruta_base` y
  `pipeline.ruta_base_autoloader` como parámetros únicos. La implementación real de
  `LSDPConfiguracion.py` usa `pipeline.volumen` + 6 parámetros de rutas granulares por fuente
  (`pipeline.ruta_cmstfl`, `pipeline.ruta_trxpfl`, `pipeline.ruta_blncfl`,
  `pipeline.schema_location_cmstfl`, `pipeline.schema_location_trxpfl`,
  `pipeline.schema_location_blncfl`). Documentado en `SYSTEM.md` sección Parámetros.

- **B1-D2** — `procesar_satellite_transaccional()`: la función fue diseñada para deduplicar
  Satellites transaccionales con LEFT ANTI JOIN por `[hash_col]`. En la implementación final,
  `LSDPPlataSatTransaccion.py` usa `@dp.append_flow()` puro; la fuente `vista_trxpfl_cdf`
  (Change Data Feed) provee deduplicación natural. La función se preserva en
  `LSDPUtilidadPrincipal.py` por compatibilidad futura. Documentado en `SYSTEM.md`.

- **B1-D3** — `_rescued_data`: descripción aclarada. AutoLoader genera esta columna
  automáticamente como parte del schema evolution; no requiere declaración manual en el código.

### Artefactos preservados (sin modificación)

- `requirements.md` — Aprobado y sin cambios
- `design.md` — Aprobado y sin cambios
- `tasks.md` — Aprobado y sin cambios
- `spec.json` — Estado de aprobación preservado

---

_Entrada creada durante el incremento `documentacion-consolidada-y-metadata` · 2026-05-01_
