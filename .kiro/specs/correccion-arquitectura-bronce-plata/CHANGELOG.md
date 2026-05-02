# CHANGELOG — correccion-arquitectura-bronce-plata

**Proyecto**: LSDP Lab DataVault DWH  
**Autor**: LSDP Lab DataVault DWH  
**Fecha**: 2026-05-01  
**Spec activo**: documentacion-consolidada-y-metadata  
**Plataforma**: Databricks Free Edition · Serverless Compute · Unity Catalog

## [2026-05-01] Alineación post-implementación

**Incremento de referencia**: `documentacion-consolidada-y-metadata`  
**Plan de alineación**: [PlanAlineacionSpecs.md](../documentacion-consolidada-y-metadata/PlanAlineacionSpecs.md)

### Divergencias documentadas (sin modificar artefactos aprobados)

- **BP-D1** — Hub_Transaccion / Link_Cliente_Transaccion: el design describe `procesar_hub()` y
  `procesar_link()` con LEFT ANTI JOIN como estrategia de deduplicación. La implementación final
  usa `@dp.append_flow()` puro sobre `vista_trxpfl_cdf` (Change Data Feed), que provee
  deduplicación natural porque el CDF entrega solo eventos del último commit y TRXID es globalmente
  único entre ejecuciones. Las funciones `procesar_hub()` y `procesar_link()` existen en
  `LSDPUtilidadPrincipal.py` y son correctas funcionalmente, pero no son invocadas por los
  notebooks del linaje transaccional.

- **BP-D2** — OPT-001 (Auto CDC SCD=1): el design presentó esta optimización como "opcional".
  En la implementación definitiva, OPT-001 se aplica sistemáticamente a Hub_Cliente,
  Hub_Operacion y Link_Cliente_Operacion como estrategia permanente (no opcional). La descripción
  en `product.md` de steering refleja esto correctamente.

- **BP-D3** — Clustering keys de Link_Cliente_Operacion: `design.md` menciona
  `["Hash_Cliente", "Hash_Operacion"]`; implementación real usa
  `["Hash_Cliente", "Hash_Operacion", "FechaRegistro"]` (una columna adicional para optimizar
  queries temporales sobre el link).

### Artefactos preservados (sin modificación)

- `requirements.md` — Aprobado y sin cambios
- `design.md` — Aprobado y sin cambios
- `tasks.md` — Aprobado y sin cambios
- `spec.json` — Estado de aprobación preservado

---

_Entrada creada durante el incremento `documentacion-consolidada-y-metadata` · 2026-05-01_
