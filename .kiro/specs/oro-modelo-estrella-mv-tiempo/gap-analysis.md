# Gap Analysis — oro-modelo-estrella-mv-tiempo

## 1. Resumen Ejecutivo

- **Tipo de proyecto**: Brownfield. Bronce (3 STs) y Plata (3 Hubs + 2 Links + 9 Satellites) **están implementados y funcionando**. La Medalla de Oro es **completamente nueva**: no existen notebooks `LSDPOro*` ni utilidades de Oro en el repositorio.
- **Reutilización alta**: las utilidades existentes (`LSDPConfiguracion.py`, `LSDPUtilidadPrincipal.py`) cubren parámetros del pipeline, constantes ATM/umbrales, hashes y patrones de procesamiento. Oro reutilizará sin cambios `obtener_configuracion`, `TIPO_DATM/CATM`, `TIPOS_ATM`, `clasificar_por_umbral`, `reordenar_columnas_lc`.
- **Hallazgo crítico**: la columna fuente real en `Sat_Transaccion_Montos` se llama **`fecha_transaccion`** (snake_case), no `FechaTransaccion` (PascalCase) como aparece en `SYSTEM.md` y en los requirements del spec. Mismo caso para `monto_principal`, `comision_transaccion`, `total_transaccion`, `identificador_cliente`. El diseño de Oro debe trabajar con los nombres reales.
- **Brechas técnicas principales**: (a) inexistencia de patrón "último estado por hash" reutilizable para dimensiones Tipo 1; (b) inexistencia de helper para llaves subrogadas estables (`DimIdCliente`, `DimIdOperacion`); (c) ausencia total de tests para Oro; (d) `SYSTEM.md` describe `Dim_Tiempo` como ST + Append Flow — debe reescribirse a MV incremental.
- **Recomendación**: enfoque **Híbrido (Opción C)** — extender `LSDPConfiguracion.py`/`LSDPUtilidadPrincipal.py` con helpers nuevos para Oro y crear notebooks nuevos `LSDPOro*` y un módulo `LSDPUtilidadOro.py`. Esfuerzo estimado **M** (3–7 días), riesgo **Medio** (incremental refresh de MV requiere validación empírica en LSDP).

---

## 2. Investigación del Estado Actual

### 2.1 Layout y assets relacionados al dominio

| Capa | Archivos existentes | Estado |
|------|---------------------|--------|
| Bronce | `LSDPBronceCMSTFL.py`, `LSDPBronceTRXPFL.py`, `LSDPBronceBLNCFL.py` | ✅ Implementado |
| Plata Hubs | `LSDPPlataHubCliente.py`, `LSDPPlataHubOperacion.py`, `LSDPPlataHubTransaccion.py` | ✅ Implementado |
| Plata Links | `LSDPPlataLinkClienteOperacion.py`, `LSDPPlataLinkClienteTransaccion.py` | ✅ Implementado |
| Plata Sats (Cliente) | `LSDPPlataSatCliente.py` (4 sats) | ✅ Implementado |
| Plata Sats (Operación) | `LSDPPlataSatOperacion.py` (3 sats) | ✅ Implementado |
| Plata Sats (Transacción) | `LSDPPlataSatTransaccion.py` (2 sats) | ✅ Implementado |
| Utilities | `LSDPConfiguracion.py`, `LSDPUtilidadPrincipal.py` | ✅ Implementado |
| **Oro** | — | ❌ **No existe** |

### 2.2 Convenciones extraídas

- **Naming notebooks**: `LSDP{Medalla}{Concepto}.py` — pattern verificado en transformations.
- **Imports estándar**: `from pyspark import pipelines as dp`, `from pyspark.sql import functions as F`, `from utilities.LSDPConfiguracion import obtener_configuracion`, `from utilities.LSDPUtilidadPrincipal import ...`.
- **Carga de configuración**: siempre `config = obtener_configuracion(spark)` al inicio del notebook.
- **Naming de columnas en Plata** (regla corregida tras verificación en runtime Databricks): **snake_case en español** para columnas de negocio en **Satellites** (`fecha_transaccion`, `monto_principal`, `identificador_cliente`, `tipo_transaccion`, etc.); **PascalCase** para columnas de negocio en **Hubs** (`IdentificadorCliente` en Hub_Cliente y Hub_Operacion, `SecuenciaSaldo` en Hub_Operacion, `IdentificadorTransaccion` en Hub_Transaccion) — usar snake_case contra Hubs produce `UNRESOLVED_COLUMN` en runtime; **PascalCase con underscore** para metadatos Data Vault (`Hash_Transaccion`, `Hash_Diferenciador`, `FechaRegistro`, `FuenteDatos`); campos calculados usan PascalCase (`RangoMontoTransaccion`, `NivelRiesgoFraude`, `ClasificacionCanalATM`).
- **Decoradores LSDP en Plata**: todos los Sats usan `dp.create_streaming_table(... expect_all_or_fail=...)` + `@dp.append_flow(target=...)` con nombre de 3 partes en `name=`.
- **Table properties**: `_PROP_TABLE` con auto-optimize, CDF, retention; se replica en cada Sat.
- **Columnas de cluster**: siempre `["FechaRegistro", "Hash_*"]`.

### 2.3 Superficies de integración para Oro

| Tabla de Plata | Columnas relevantes para Oro | Comentario |
|----------------|-------------------------------|------------|
| `Hub_Cliente` | `Hash_Cliente`, `IdentificadorCliente`, `FechaRegistro` | Insumo `Dim_Cliente` |
| `Sat_Cliente_DatosEstables` | `Hash_Cliente`, `sexo_cliente`, `edad_cliente`, `RangoEtario`, `CategoriaIngresos`, `Hash_Diferenciador`, `FechaRegistro` | Último por `Hash_Cliente` |
| `Sat_Cliente_Contacto` | `Hash_Cliente`, atributos contacto | Último por `Hash_Cliente` |
| `Sat_Cliente_Clasificacion` | `Hash_Cliente`, `tipo_cliente`, `segmento_cliente`, `nivel_riesgo`, `indicador_vip`, `estado_kyc`, etc. | Último por `Hash_Cliente` |
| `Sat_Cliente_Financiero` | `Hash_Cliente`, `score_cliente`, `ingresos_cliente`, fechas | Último por `Hash_Cliente` |
| `Hub_Operacion` | `Hash_Operacion`, `IdentificadorCliente`, `SecuenciaSaldo` | Insumo `Dim_Operacion` |
| `Sat_Operacion_DatosEstables` | `Hash_Operacion`, `tipo_cuenta`, `moneda_cuenta`, `estado_cuenta`, `CategoriaSaldo`, `EstadoUtilizacionCredito`, `IndicadorSobregiro` | Último por `Hash_Operacion` |
| `Sat_Operacion_Montos` | `Hash_Operacion`, `saldo_disponible`, `saldo_total`, `limite_credito`, `RatioCuenta` | Último por `Hash_Operacion` |
| `Sat_Operacion_FechasEvento` | `Hash_Operacion`, fechas | Último por `Hash_Operacion` |
| `Hub_Transaccion` | `Hash_Transaccion`, `IdentificadorTransaccion` | Insumo `Hec_Transacciones_ATM` |
| `Sat_Transaccion_DatosEstables` | `Hash_Transaccion`, **`fecha_transaccion`**, `tipo_transaccion`, `moneda_transaccion`, `estado_transaccion`, `canal_transaccion`, `ClasificacionCanalATM` | Filtrar `tipo_transaccion ∈ TIPOS_ATM` |
| **`Sat_Transaccion_Montos`** | `Hash_Transaccion`, **`fecha_transaccion`**, `identificador_cliente`, **`monto_principal`**, `comision_transaccion`, `total_transaccion`, `RangoMontoTransaccion` | **Fuente de `Dim_Tiempo`** (DISTINCT `fecha_transaccion`) y métricas del hecho |
| `Link_Cliente_Transaccion` | `Hash_Cliente`, `Hash_Transaccion` | Resolver `DimIdCliente` |
| `Link_Cliente_Operacion` | `Hash_Cliente`, `Hash_Operacion` | Resolver `DimIdOperacion` transitivamente |

---

## 3. Análisis de Factibilidad de Requisitos

### 3.1 Mapeo Requisito → Asset

| Req | Asset existente reutilizable | Brecha (Missing/Unknown/Constraint) |
|-----|------------------------------|--------------------------------------|
| **R1** Actualización docs | `SYSTEM.md`, `.kiro/steering/*.md` | **Missing**: secciones de `Dim_Tiempo` describen ST+AppendFlow; `tech.md` no menciona MVs en Oro; ejemplos de código deben reescribirse. |
| **R2** `Dim_Tiempo` MV incremental | `Sat_Transaccion_Montos` (Plata, columna `fecha_transaccion` snake_case) | **Constraint**: nombre de columna real es `fecha_transaccion`, no `FechaTransaccion`. **Unknown/Research Needed**: confirmar lista exacta de operadores soportados por el incremental refresh de MVs en LSDP Free Edition (`SELECT`, `DISTINCT`, `WHERE`, `withColumn` determinístico — verificar si `DISTINCT` califica para refresh incremental o solo para refresh completo). |
| **R3** `Dim_Cliente` Tipo 1 | `Hub_Cliente`, 4 Sats de Cliente | **Missing**: helper "último registro por hash" (existe lógica embebida en `procesar_satellite()` pero no expuesta como helper público); helper para asignar `DimIdCliente` estable. |
| **R4** `Dim_Operacion` Tipo 1 | `Hub_Operacion`, 3 Sats de Operación | Mismo gap que R3. |
| **R5** `Hec_Transacciones_ATM` | `Hub_Transaccion`, 2 Sats de Transacción, ambos Links | **Constraint**: `Sat_Transaccion_Montos` ya trae `identificador_cliente` (no se necesita Link para `DimIdCliente`, optimización potencial). **Unknown/Research Needed**: política de `DimIdOperacion` cuando un cliente tiene **múltiples** operaciones — ¿cuál asignar al hecho? |
| **R6** Restricciones Serverless/LSDP | Patrón ya verificado en Plata (`Hub`, `Link`, `Sat`) | Sin gap; aplica las mismas reglas. |
| **R7** Estructura código | `transformations/`, `utilities/` | Sin gap estructural; se siguen patrones existentes. |
| **R8** Tests | `tests/test_notebooks_plata.py` (patrón AST estático) | **Missing**: `tests/test_notebooks_oro.py` y posiblemente `test_utilidades_oro.py`. |

### 3.2 Necesidades técnicas derivadas de los requisitos

- **Modelos de datos**: 3 dimensiones (`Dim_Cliente`, `Dim_Operacion`, `Dim_Tiempo`) + 1 hecho (`Hec_Transacciones_ATM`).
- **Operaciones de DataFrame nuevas**: `DISTINCT` sobre fechas, joins multi-Sat (último estado), generación de surrogate IDs estables, joins con dimensiones para resolución de FKs.
- **No funcionales**:
  - **Performance**: cluster_by adecuado por dimensión (`DimId*`) y por fecha en hecho.
  - **Reliability**: expectations `or_fail` para nulos críticos; expectations `or_drop` opcionales para datos sucios.
  - **Compatibilidad Serverless**: replicar patrón ya validado.
  - **Incremental refresh**: depende de la elegibilidad de la MV — *Research Needed*.

### 3.3 Señales de complejidad

- ✅ Lógica algorítmica conocida (CASE WHEN, SHA2, joins, ROW_NUMBER) — ya aplicada en Plata.
- ⚠️ Workflow de **DimId estable**: requiere algoritmo determinístico (orden por `Hash_*`, `dense_rank()` o `row_number()`).
- ⚠️ **Incremental refresh de MV** sobre `DISTINCT(fecha_transaccion)` — incertidumbre técnica que merece validación empírica antes de aprobar el diseño.

---

## 4. Opciones de Implementación

### Opción A — Extender utilidades existentes y notebooks pequeños

Extender `LSDPUtilidadPrincipal.py` con `obtener_ultimo_por_hash()`, `asignar_dim_id()`, `leer_satellite_actual()`, etc.; crear sólo 4 notebooks `LSDPOro*` mínimos.

- ✅ Máxima coherencia con la arquitectura actual; un solo módulo de utilidades.
- ✅ Curva de aprendizaje cero para mantenedores que ya conocen `LSDPUtilidadPrincipal.py`.
- ❌ Mezcla responsabilidades: utilidades de Plata (Append-Only Sats) y Oro (snapshots Tipo 1) en el mismo archivo.
- ❌ Crece `LSDPUtilidadPrincipal.py` (actualmente ~250 líneas) a más de 400.

### Opción B — Módulo nuevo `LSDPUtilidadOro.py` y notebooks nuevos

Crear `utilities/LSDPUtilidadOro.py` con helpers exclusivos de Oro; los 4 notebooks importan desde el nuevo módulo. `LSDPConfiguracion.py` no se toca o solo se le agregan constantes específicas (p. ej. `RANGOS_DIMID_CLIENTE`).

- ✅ Separación nítida de responsabilidades por medalla.
- ✅ Tests independientes (`test_utilidades_oro.py`).
- ✅ Facilita reutilización futura de la utilidad de Oro en otros modelos estrella.
- ❌ Más archivos para navegar.
- ❌ Puede haber duplicación menor con `LSDPUtilidadPrincipal.py` (p. ej. patrón ROW_NUMBER por hash).

### Opción C — Híbrido (recomendado)

- En `LSDPConfiguracion.py`: agregar **solo** constantes de Oro si aplican (mínimas).
- En `LSDPUtilidadPrincipal.py`: dejar intacto.
- Nuevo `utilities/LSDPUtilidadOro.py` con helpers: `obtener_ultimo_por_hash()`, `unir_sats_actuales()`, `asignar_dim_id_estable()`, `validar_columnas_oro()`.
- 4 notebooks nuevos en `transformations/`: `LSDPOroDimTiempo.py`, `LSDPOroDimCliente.py`, `LSDPOroDimOperacion.py`, `LSDPOroHecTransaccionesATM.py`.
- Nuevos tests: `tests/test_notebooks_oro.py` (estático) y `tests/test_utilidad_oro.py` (funcional).
- Para R1: editar `SYSTEM.md` puntualmente (4 secciones afectadas: tabla de tipos, regla especial, ejemplo de código, tabla compatibilidad Free Edition); revisar `.kiro/steering/*.md` y `docs/*.md`.

- ✅ Reutiliza utilidades existentes sin contaminarlas.
- ✅ Permite tests independientes y aislados.
- ✅ Trazabilidad clara: cada notebook → un destino LSDP en Oro.
- ❌ Requiere disciplina para no duplicar lógica con `LSDPUtilidadPrincipal.py`.

---

## 5. Esfuerzo y Riesgo

| Componente | Esfuerzo | Riesgo | Justificación |
|------------|----------|--------|---------------|
| **R1 Documentación** (`SYSTEM.md`, steering, docs) | S | Low | Cambios localizados a ~5 secciones. |
| **R2 `Dim_Tiempo` MV incremental** | S | **Medium** | Patrón nuevo en el repo; elegibilidad de incremental refresh no verificada empíricamente. |
| **R3 `Dim_Cliente`** + **R4 `Dim_Operacion`** | M | Medium | Joins multi-Sat + resolución de último estado + surrogate ID estable. |
| **R5 `Hec_Transacciones_ATM`** | M | Medium | Resolución transitiva de `DimIdOperacion` requiere decisión funcional. |
| **R6 Compliance Serverless** | S | Low | Patrón ya validado en Plata. |
| **R7 Estructura** | S | Low | Replica convenciones existentes. |
| **R8 Tests** | S | Low | Replica plantilla de `test_notebooks_plata.py`. |
| **Total** | **M (3–7 días)** | **Medium** | Mayor riesgo concentrado en R2 (incremental refresh). |

---

## 6. Items "Research Needed" para la fase de Diseño

1. **Elegibilidad de incremental refresh de MV con `DISTINCT`**: confirmar en docs LSDP Free Edition si `spark.read.table(...).select("fecha_transaccion").distinct()` es elegible para incremental refresh o forzaría refresh completo. Alternativa: usar `groupBy("fecha_transaccion").agg(...)` o `select(...).dropDuplicates()`.
2. **Política de `DimIdOperacion` por transacción**: cuando un cliente tiene N operaciones activas (1:N entre Cliente↔Operación), definir regla de asignación al hecho (ej.: operación más reciente, operación con mayor saldo, una por defecto, o expandir el grano del hecho). Decisión funcional pendiente.
3. **Estabilidad de `DimId*`**: confirmar algoritmo determinístico aceptable (`row_number() over (order by Hash_*)` vs. `dense_rank()` vs. lookup persistente desde ejecución previa). Para Tipo 1 puro un orden lexicográfico por hash es suficiente; para escenarios de eliminación se requiere lookup.
4. **Discrepancia naming `FechaTransaccion` vs `fecha_transaccion`**: decidir si el spec/`SYSTEM.md` se alinea al naming snake_case real de Plata (recomendado) o si se renombra en Oro a PascalCase (`FechaTransaccion`) por consistencia con la convención del modelo estrella.
5. **Filtrado del hecho**: validar si filtrar por `tipo_transaccion ∈ TIPOS_ATM` se hace **antes** del join con dimensiones (recomendado por performance) y si conviene crear una vista temporal `@dp.temporary_view` previa para reutilización entre flows.
6. **Política de refresh para dimensiones Tipo 1**: las MV de `Dim_Cliente`/`Dim_Operacion` con `ROW_NUMBER` y joins a 3-4 tablas probablemente requieran refresh completo; confirmar implicancias de rendimiento/costo en Free Edition.

---

## 7. Recomendaciones para la Fase de Diseño

- **Enfoque preferido**: Opción C (Híbrido).
- **Decisiones clave a formalizar en `design.md`**:
  - Naming definitivo de columnas en Oro (alinear con snake_case de Plata o PascalCase del modelo estrella).
  - Algoritmo determinístico para `DimIdCliente` y `DimIdOperacion`.
  - Política para `DimIdOperacion` en el hecho (múltiples operaciones por cliente).
  - Estructura final de `LSDPUtilidadOro.py` (firmas de helpers).
  - Lista exacta de secciones de `SYSTEM.md` a modificar y diff esperado.
- **Validar empíricamente** la elegibilidad de incremental refresh para `Dim_Tiempo` antes de marcar diseño como completo (puede hacerse en exploración previa o como tarea early de implementación con criterio go/no-go).
- **Conservar table_properties** equivalentes al `_PROP_TABLE` de Plata para coherencia.

---

## 8. Output Checklist

- ✅ Requirement-to-Asset Map con gaps tagged
- ✅ Opciones A/B/C con trade-offs
- ✅ Esfuerzo (M) y Riesgo (Medium) justificados
- ✅ Items "Research Needed" enumerados
- ✅ Recomendación para diseño y decisiones pendientes
