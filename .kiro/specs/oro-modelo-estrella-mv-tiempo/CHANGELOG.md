# CHANGELOG — oro-modelo-estrella-mv-tiempo

**Proyecto**: LSDP Lab DataVault DWH  
**Autor**: LSDP Lab DataVault DWH  
**Fecha**: 2026-05-01  
**Spec activo**: documentacion-consolidada-y-metadata  
**Plataforma**: Databricks Free Edition · Serverless Compute · Unity Catalog

## [2026-05-01] Alineación post-implementación

**Incremento de referencia**: `documentacion-consolidada-y-metadata`  
**Plan de alineación**: [PlanAlineacionSpecs.md](../documentacion-consolidada-y-metadata/PlanAlineacionSpecs.md)

### Divergencias documentadas (sin modificar artefactos aprobados)

- **ORO-D1 (Crítica)** — Nombre de la tabla de hechos: `design.md` y `tasks.md` usan el nombre
  `Fact_Transacciones_ATM`. La implementación real registra la tabla en Unity Catalog como
  `Hec_Transacciones_ATM` (notebook `LSDPOroHecTransaccionesATM.py`, decorador
  `@dp.materialized_view(name=f"{catalogo_oro}.{esquema_oro}.Hec_Transacciones_ATM", ...)`).
  Toda documentación y consulta SQL debe usar `Hec_Transacciones_ATM`. Corregido en `SYSTEM.md`
  y documentado en `docs/ModeloDatos.md`.

- **ORO-D4** — `asignar_dim_id_estable()`: el design describía la llave subrogada con
  `F.abs(F.hash(...).cast("long"))`. La implementación real usa
  `F.xxhash64(hash_col).cast("long")`, que es determinístico y no requiere `abs`. Los valores
  pueden ser negativos, lo cual es válido para llaves subrogadas internas del modelo estrella.
  Documentado en `SYSTEM.md` (R12) y en `docs/ManualTecnico.md`.

- **ORO-D5** — Dim_Tiempo (Corrección B.2 aprobada): design inicial mencionaba posibilidad de
  "Streaming Table acumulativa" para Dim_Tiempo. La corrección B.2 aprobó usar
  `@dp.materialized_view` con refresh incremental nativo (Enzyme CDF), que es más eficiente y
  admite solo operadores determinísticos (`select`, `distinct`, `withColumn`). Corregido en
  `SYSTEM.md` (R10).

### Artefactos preservados (sin modificación)

- `requirements.md` — Aprobado y sin cambios
- `design.md` — Aprobado y sin cambios
- `tasks.md` — Aprobado y sin cambios
- `spec.json` — Estado de aprobación preservado

---

_Entrada creada durante el incremento `documentacion-consolidada-y-metadata` · 2026-05-01_
