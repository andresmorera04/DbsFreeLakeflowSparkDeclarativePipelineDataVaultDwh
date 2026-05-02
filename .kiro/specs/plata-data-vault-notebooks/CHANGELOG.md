# CHANGELOG — plata-data-vault-notebooks

**Proyecto**: LSDP Lab DataVault DWH  
**Autor**: LSDP Lab DataVault DWH  
**Fecha**: 2026-05-01  
**Spec activo**: documentacion-consolidada-y-metadata  
**Plataforma**: Databricks Free Edition · Serverless Compute · Unity Catalog

## [2026-05-01] Alineación post-implementación

**Incremento de referencia**: `documentacion-consolidada-y-metadata`  
**Plan de alineación**: [PlanAlineacionSpecs.md](../documentacion-consolidada-y-metadata/PlanAlineacionSpecs.md)

### Divergencias documentadas (sin modificar artefactos aprobados)

- **PL-D1 (Crítica)** — Mecanismo de Satellites transaccionales (Corrección B.1): el design
  describe la estrategia B.1 como `procesar_satellite_transaccional()` con LEFT ANTI JOIN por
  `[hash_col]`. La implementación real en `LSDPPlataSatTransaccion.py` usa `@dp.append_flow()`
  puro sin invocar ningún helper de deduplicación. La fuente `vista_trxpfl_cdf` entrega solo los
  eventos del último commit (Change Data Feed), y TRXID es globalmente único entre ejecuciones
  por diseño. La función `procesar_satellite_transaccional()` existe en
  `LSDPUtilidadPrincipal.py` y es correcta, pero no se invoca actualmente. El resultado
  funcional es equivalente: cada transacción se almacena exactamente una vez. Documentado en
  `SYSTEM.md` y `docs/ManualTecnico.md`.

- **PL-D2** — Conteo de columnas de Sat_Transaccion_*: el design menciona "36 cols" para
  `Sat_Transaccion_DatosEstables`. Conteo real verificado en `LSDPPlataSatTransaccion.py`:
  34 columnas de negocio + `VersionCarga` + `FechaCargaBronce` (de CDF) + `Hash_Diferenciador` +
  `FechaRegistro` + `FuenteDatos` = 39 columnas totales. Las columnas `VersionCarga` y
  `FechaCargaBronce` son columnas adicionales de trazabilidad provenientes de `vista_trxpfl_cdf`.
  Documentado en `docs/ModeloDatos.md`.

### Artefactos preservados (sin modificación)

- `requirements.md` — Aprobado y sin cambios
- `design.md` — Aprobado y sin cambios
- `tasks.md` — Aprobado y sin cambios
- `spec.json` — Estado de aprobación preservado

---

_Entrada creada durante el incremento `documentacion-consolidada-y-metadata` · 2026-05-01_
