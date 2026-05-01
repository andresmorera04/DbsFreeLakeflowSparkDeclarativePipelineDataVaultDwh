# Plan de Implementación

- [x] 1. Funciones nuevas en utilidades para procesamiento de Plata
- [x] 1.1 Implementar la función de detección de cambios Append-Only para Satellites
  - Crear `procesar_satellite()` en `LSDPUtilidadPrincipal.py` que compare el `Hash_Diferenciador` entre datos entrantes y el último registro existente por llave hash de entidad
  - Usar ventana `ROW_NUMBER OVER (PARTITION BY hash_col ORDER BY FechaRegistro DESC) = 1` para obtener el registro más reciente por entidad
  - Hacer `left join` entre datos nuevos y último existente, filtrando donde `Hash_Existente IS NULL` (entidad nueva) o `Hash_Diferenciador != Hash_Existente` (cambio detectado)
  - Capturar `AnalysisException` para primera ejecución (tabla no existe) retornando todos los registros sin filtrar; propagar cualquier otra excepción
  - Retornar SOLO los registros nuevos/cambiados (sin columnas auxiliares `_rn`, `Hash_Existente`)
  - No importar LSDP — usar solo funciones nativas de `pyspark.sql.functions` y `Window`
  - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 14.3, 14.5_

- [x] 1.2 (P) Implementar la función genérica de clasificación por umbrales
  - Crear `clasificar_por_umbral()` en `LSDPUtilidadPrincipal.py` que reciba una columna PySpark y un diccionario de umbrales `{nombre: (min, max)}`
  - Construir dinámicamente la cadena `F.when(col.between(min, max), F.lit(nombre))` iterando sobre el diccionario
  - Retornar `"DESCONOCIDO"` para valores fuera de todos los rangos (vía `otherwise`)
  - Compatible con `IntegerType`, `LongType` y `DoubleType`
  - No importar LSDP — usar solo funciones nativas de `pyspark.sql.functions`
  - _Requirements: 11.1, 11.2, 11.3, 11.5, 11.6, 14.4, 14.5_

- [x] 2. Hub_Cliente — Materialized View de llave de negocio de cliente
  - Crear notebook `LSDPPlataHubCliente.py` que lea de la MV de Bronce `CMSTFL` vía `spark.read.table()`
  - Importar configuración centralizada con `obtener_configuracion(spark)` y funciones helper desde utilidades
  - Producir 4 columnas: `FechaRegistro` (`current_timestamp()`), `Hash_Cliente` (SHA2-256 de `CUSTID`), `IdentificadorCliente` (CUSTID), `FuenteDatos` (nombre 3 partes de tabla fuente)
  - Deduplicar por `IdentificadorCliente` con `dropDuplicates()`
  - Aplicar `reordenar_columnas_lc()` con `["FechaRegistro", "Hash_Cliente"]`
  - Registrar como `@dp.materialized_view()` con nombre de 3 partes, `cluster_by` y expectations: `id_cliente_positivo` (DROP) y `hash_cliente_no_nulo` (FAIL)
  - No propagar columnas exclusivas de Bronce (`año`, `mes`, `dia`, `FechaRegistroParquet`, `_rescued_data`)
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.1, 2.2, 2.3, 2.4, 12.1, 12.6, 13.1, 13.2, 13.3, 13.4, 13.5, 13.6_

- [x] 3. (P) Hub_Operacion — Materialized View de llave de negocio compuesta de operación
  - Crear notebook `LSDPPlataHubOperacion.py` que lea de la MV de Bronce `BLNCFL` vía `spark.read.table()`
  - Importar configuración centralizada y funciones helper
  - Producir 5 columnas: `FechaRegistro`, `Hash_Operacion` (SHA2-256 de `CUSTID|BLSQ`), `IdentificadorCliente`, `SecuenciaSaldo`, `FuenteDatos`
  - Deduplicar por combinación (`IdentificadorCliente`, `SecuenciaSaldo`)
  - Aplicar `reordenar_columnas_lc()` con `["FechaRegistro", "Hash_Operacion"]`
  - Registrar como `@dp.materialized_view()` con nombre de 3 partes, `cluster_by` y expectations: `id_cliente_positivo` (DROP) y `hash_operacion_no_nulo` (FAIL)
  - No propagar columnas exclusivas de Bronce
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 3.1, 3.2, 3.3, 3.4, 12.1, 12.6, 13.1, 13.2, 13.3, 13.4, 13.5, 13.6_

- [x] 4. (P) Hub_Transaccion — Materialized View de llave de negocio de transacción
  - Crear notebook `LSDPPlataHubTransaccion.py` que lea de la MV de Bronce `TRXPFL` vía `spark.read.table()`
  - Importar configuración centralizada y funciones helper
  - Producir 4 columnas: `FechaRegistro`, `Hash_Transaccion` (SHA2-256 de `TRXID` — StringType nativo, sin cast adicional), `IdentificadorTransaccion`, `FuenteDatos`
  - Deduplicar por `IdentificadorTransaccion`
  - Aplicar `reordenar_columnas_lc()` con `["FechaRegistro", "Hash_Transaccion"]`
  - Registrar como `@dp.materialized_view()` con nombre de 3 partes, `cluster_by` y expectations: `id_transaccion_no_nulo` (FAIL) y `hash_transaccion_no_nulo` (FAIL)
  - No propagar columnas exclusivas de Bronce
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 4.1, 4.2, 4.3, 4.4, 12.2, 12.6, 13.1, 13.2, 13.3, 13.4, 13.5, 13.6_

- [x] 5. (P) Link_Cliente_Operacion — Materialized View de relación cliente-operación
  - Crear notebook `LSDPPlataLinkClienteOperacion.py` que lea de la MV de Bronce `BLNCFL` vía `spark.read.table()`
  - Importar configuración centralizada y funciones helper
  - Calcular `Hash_Cliente` desde `CUSTID` y `Hash_Operacion` desde `CUSTID|BLSQ` usando campos AS400 originales (no leer de los Hubs)
  - Calcular `Hash_Link_Cliente_Operacion` como SHA2-256 de `Hash_Cliente|Hash_Operacion`
  - Producir 5 columnas: `FechaRegistro`, `Hash_Link_Cliente_Operacion`, `Hash_Cliente`, `Hash_Operacion`, `FuenteDatos`
  - Deduplicar por combinación (`Hash_Cliente`, `Hash_Operacion`)
  - Aplicar `reordenar_columnas_lc()` con `["FechaRegistro", "Hash_Cliente", "Hash_Operacion"]`
  - Registrar como `@dp.materialized_view()` con nombre de 3 partes y `cluster_by`
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 5.1, 5.2, 5.3, 5.4, 13.1, 13.2, 13.3, 13.4, 13.5, 13.6_

- [x] 6. (P) Link_Cliente_Transaccion — Materialized View de relación cliente-transacción
  - Crear notebook `LSDPPlataLinkClienteTransaccion.py` que lea de la MV de Bronce `TRXPFL` vía `spark.read.table()`
  - Importar configuración centralizada y funciones helper
  - Calcular `Hash_Cliente` desde `CUSTID` y `Hash_Transaccion` desde `TRXID` usando campos AS400 originales
  - Calcular `Hash_Link_Cliente_Transaccion` como SHA2-256 de `Hash_Cliente|Hash_Transaccion`
  - Producir 5 columnas: `FechaRegistro`, `Hash_Link_Cliente_Transaccion`, `Hash_Cliente`, `Hash_Transaccion`, `FuenteDatos`
  - Deduplicar por combinación (`Hash_Cliente`, `Hash_Transaccion`)
  - Aplicar `reordenar_columnas_lc()` con `["FechaRegistro", "Hash_Cliente", "Hash_Transaccion"]`
  - Registrar como `@dp.materialized_view()` con nombre de 3 partes y `cluster_by`
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 6.1, 6.2, 6.3, 6.4, 13.1, 13.2, 13.3, 13.4, 13.5, 13.6_

- [x] 7. Satellites de Cliente — 4 Streaming Tables Acumulativas con detección de cambios
- [x] 7.1 Definir las 4 Streaming Tables y la lectura compartida de Bronce
  - En notebook `LSDPPlataSatCliente.py`, importar configuración centralizada, funciones helper (`calcular_hash_hub`, `calcular_hash_diferenciador`, `procesar_satellite`, `clasificar_por_umbral`) y constantes de umbrales
  - Leer de `CMSTFL` vía `spark.read.table()` una sola vez para compartir entre las 4 funciones de append_flow
  - Definir 4 Streaming Tables con `dp.create_streaming_table()`, cada una con `cluster_by=["FechaRegistro", "Hash_Cliente"]`, expectations `hash_diferenciador_no_nulo` (FAIL) y `table_properties`
  - No propagar columnas exclusivas de Bronce
  - _Requirements: 1.1, 1.2, 1.3, 1.5, 1.6, 7.1, 7.6, 12.5, 13.1, 13.2, 13.3_

- [x] 7.2 Implementar el Satellite de datos estables del cliente con campos calculados
  - Renombrar campos AS400 a español (CUSSX→sexo_cliente, CUSTT→tratamiento_cliente, CUSDB→fecha_nacimiento, etc.) usando `.alias()`
  - Calcular `Hash_Cliente` con `calcular_hash_hub([F.col("CUSTID")])`
  - Calcular `RangoEtario` con `clasificar_por_umbral(edad_cliente, UMBRAL_RANGO_ETARIO)` y `CategoriaIngresos` con `clasificar_por_umbral(ingresos_cliente, UMBRAL_CATEGORIA_INGRESOS)` — nota: `ingresos_cliente` se lee de CMSTFL pero no se persiste en este Satellite
  - Calcular `Hash_Diferenciador` SHA2-512 excluyendo `FechaRegistro` y `FuenteDatos`; agregar estas columnas después
  - Invocar `procesar_satellite()` para obtener solo registros nuevos/cambiados
  - Definir `@dp.append_flow()` apuntando al Satellite, retornando el DataFrame de cambios con 17 columnas en el orden del esquema de diseño
  - _Requirements: 7.1, 7.2, 7.7, 7.8, 10.1, 10.2, 10.3, 11.1, 11.2, 11.7, 13.4, 13.5, 13.6, 14.1, 14.2_

- [x] 7.3 (P) Implementar el Satellite de contacto del cliente
  - Renombrar 15 campos AS400 de contacto/datos personales a español (CUSNM→nombre_cliente, CUSLN→apellido_cliente, etc.)
  - Calcular `Hash_Cliente` y `Hash_Diferenciador` SHA2-512 excluyendo `FechaRegistro` y `FuenteDatos`
  - Invocar `procesar_satellite()` y definir `@dp.append_flow()` retornando DataFrame de 19 columnas
  - _Requirements: 7.1, 7.3, 7.7, 7.8, 10.1, 10.2, 10.3, 13.4, 13.5, 13.6, 14.1, 14.2_

- [x] 7.4 (P) Implementar el Satellite de clasificación del cliente
  - Renombrar 19 campos AS400 de clasificación/segmentación a español (CUSTP→tipo_cliente, CUSSG→segmento_cliente, etc.)
  - Calcular `Hash_Cliente` y `Hash_Diferenciador` SHA2-512 excluyendo `FechaRegistro` y `FuenteDatos`
  - Invocar `procesar_satellite()` y definir `@dp.append_flow()` retornando DataFrame de 23 columnas
  - _Requirements: 7.1, 7.4, 7.7, 7.8, 10.1, 10.2, 10.3, 13.4, 13.5, 13.6, 14.1, 14.2_

- [x] 7.5 (P) Implementar el Satellite financiero del cliente
  - Renombrar 7 campos numéricos y 18 fechas de evento AS400 a español (CUSAC2→cantidad_cuentas, CUSOD→fecha_apertura_relacion, etc.)
  - Calcular `Hash_Cliente` y `Hash_Diferenciador` SHA2-512 excluyendo `FechaRegistro` y `FuenteDatos`
  - Invocar `procesar_satellite()` y definir `@dp.append_flow()` retornando DataFrame de 28 columnas
  - Agregar expectation adicional `score_cliente_en_rango` con severidad WARN para `score_cliente BETWEEN 300 AND 1150` en el `create_streaming_table` correspondiente
  - _Requirements: 7.1, 7.5, 7.7, 7.8, 10.1, 10.2, 10.3, 12.3, 13.4, 13.5, 13.6, 14.1, 14.2_

- [x] 8. Satellites de Operación — 3 Streaming Tables Acumulativas con detección de cambios
- [x] 8.1 Definir las 3 Streaming Tables y la lectura compartida de Bronce
  - En notebook `LSDPPlataSatOperacion.py`, importar configuración, funciones helper y constantes de umbrales
  - Leer de `BLNCFL` vía `spark.read.table()` una sola vez
  - Definir 3 Streaming Tables con `dp.create_streaming_table()`, cada una con `cluster_by=["FechaRegistro", "Hash_Operacion"]`, expectations `hash_diferenciador_no_nulo` (FAIL) y `table_properties`
  - No propagar columnas exclusivas de Bronce
  - _Requirements: 1.1, 1.2, 1.3, 1.5, 1.6, 8.1, 8.5, 12.5, 13.1, 13.2, 13.3_

- [x] 8.2 Implementar el Satellite de datos estables de operación con campos calculados
  - Renombrar 29 campos AS400 cualitativos a español (BLACT→tipo_cuenta, BLACN→numero_cuenta, etc.)
  - Calcular `Hash_Operacion` con `calcular_hash_hub([F.col("CUSTID"), F.col("BLSQ")])`
  - Calcular `CategoriaSaldo` con `clasificar_por_umbral(saldo_disponible, UMBRAL_CATEGORIA_SALDO)`, `EstadoUtilizacionCredito` con `clasificar_por_umbral(ratio_cuenta, UMBRAL_UTILIZACION_CREDITO)` e `IndicadorSobregiro` con `clasificar_por_umbral(valor_sobregiro, UMBRAL_SOBREGIRO)` — las columnas fuente se leen pero no se persisten en este Satellite
  - Calcular `Hash_Diferenciador` SHA2-512 excluyendo `FechaRegistro` y `FuenteDatos`
  - Invocar `procesar_satellite()` y definir `@dp.append_flow()` retornando DataFrame de 36 columnas
  - _Requirements: 8.1, 8.2, 8.6, 8.7, 10.1, 10.2, 10.3, 11.3, 11.6, 11.7, 13.4, 13.5, 13.6, 14.1, 14.2_

- [x] 8.3 (P) Implementar el Satellite de montos de operación
  - Renombrar 34 campos monetarios/ratios AS400 a español (BLAV→saldo_disponible, BLTB→saldo_total, etc.)
  - Calcular `Hash_Operacion`, `Hash_Diferenciador` SHA2-512 excluyendo `FechaRegistro` y `FuenteDatos`
  - Invocar `procesar_satellite()` y definir `@dp.append_flow()` retornando DataFrame de 38 columnas
  - _Requirements: 8.1, 8.3, 8.6, 8.7, 10.1, 10.2, 10.3, 13.4, 13.5, 13.6, 14.1, 14.2_

- [x] 8.4 (P) Implementar el Satellite de fechas de evento de operación
  - Renombrar 19 campos DateType AS400 a español (BLOD→fecha_apertura_cuenta, BLXD→fecha_expiracion_cuenta, etc.)
  - Calcular `Hash_Operacion`, `Hash_Diferenciador` SHA2-512 excluyendo `FechaRegistro` y `FuenteDatos`
  - Invocar `procesar_satellite()` y definir `@dp.append_flow()` retornando DataFrame de 23 columnas
  - _Requirements: 8.1, 8.4, 8.6, 8.7, 10.1, 10.2, 10.3, 13.4, 13.5, 13.6, 14.1, 14.2_

- [x] 9. Satellites de Transacción — 2 Streaming Tables Acumulativas con detección de cambios
- [x] 9.1 Definir las 2 Streaming Tables y la lectura compartida de Bronce
  - En notebook `LSDPPlataSatTransaccion.py`, importar configuración, funciones helper y constantes (`TIPO_DATM`, `TIPO_CATM`)
  - Leer de `TRXPFL` vía `spark.read.table()` una sola vez
  - Definir 2 Streaming Tables con `dp.create_streaming_table()`: Sat_Transaccion_DatosEstables con expectations `hash_diferenciador_no_nulo` (FAIL), y Sat_Transaccion_Montos con expectations `monto_transaccion_positivo` (DROP) y `hash_diferenciador_no_nulo` (FAIL)
  - Ambas con `cluster_by=["FechaRegistro", "Hash_Transaccion"]` y `table_properties`
  - No propagar columnas exclusivas de Bronce
  - _Requirements: 1.1, 1.2, 1.3, 1.5, 1.6, 9.1, 9.4, 12.4, 12.5, 13.1, 13.2, 13.3_

- [x] 9.2 Implementar el Satellite de datos estables de transacción con ClasificacionCanalATM
  - Renombrar campos categóricos, fechas auxiliares y timestamps AS400 a español (TRXTYP→tipo_transaccion, TRXCUR→moneda_transaccion, etc.)
  - Calcular `Hash_Transaccion` con `calcular_hash_hub([F.col("TRXID")])` — StringType nativo
  - Calcular `ClasificacionCanalATM` inline: DATM→RETIRO_ATM, CATM→DEPOSITO_ATM, canal ATM + otro tipo→OTRA_OP_ATM, else→NO_ATM. No usar `clasificar_por_umbral()` para esta clasificación
  - Calcular `Hash_Diferenciador` SHA2-512 excluyendo `FechaRegistro` y `FuenteDatos`
  - Invocar `procesar_satellite()` y definir `@dp.append_flow()` retornando DataFrame de 34 columnas
  - _Requirements: 9.1, 9.2, 9.5, 9.6, 10.1, 10.2, 10.3, 11.4, 11.6, 11.7, 13.4, 13.5, 13.6, 14.1, 14.2_

- [x] 9.3 (P) Implementar el Satellite de montos de transacción con campos calculados
  - Renombrar 30 campos monetarios/riesgo AS400 a español (TRXAMT→monto_principal, TRXCM→comision_transaccion, etc.) incluyendo `identificador_cliente` (CUSTID) y `fecha_transaccion` (TRXDT)
  - Calcular `Hash_Transaccion` con `calcular_hash_hub([F.col("TRXID")])`
  - Calcular `RangoMontoTransaccion` con `clasificar_por_umbral(monto_principal, UMBRAL_RANGO_MONTO)` y `NivelRiesgoFraude` con `clasificar_por_umbral(riesgo_fraude, UMBRAL_RIESGO_FRAUDE)` (escala 0-100)
  - Calcular `Hash_Diferenciador` SHA2-512 excluyendo `FechaRegistro` y `FuenteDatos`
  - Invocar `procesar_satellite()` y definir `@dp.append_flow()` retornando DataFrame de 36 columnas
  - _Requirements: 9.1, 9.3, 9.5, 9.6, 10.1, 10.2, 10.3, 11.5, 11.6, 11.7, 13.4, 13.5, 13.6, 14.1, 14.2_

- [x] 10. Validación de integración del pipeline Plata completo
- [x] 10.1 Verificar cobertura de expectations y compatibilidad Serverless en todos los notebooks
  - Revisar que cada Hub aplica expectations de hash no nulo (FAIL) y de llave de negocio (DROP o FAIL según corresponda)
  - Revisar que cada Satellite aplica `hash_diferenciador_no_nulo` (FAIL) en `dp.create_streaming_table()`
  - Confirmar que ningún notebook usa `.cache()`, `.persist()`, RDDs, UDFs, `spark.sparkContext`, threading ni `import databricks.sdk.pipelines`
  - Verificar que todos los hashes usan `F.sha2()` con SHA2-256 para Hubs/Links y SHA2-512 para Satellites, con `F.concat_ws("|", ...)` y `.cast("string")` donde corresponda
  - Verificar que `FechaRegistro` y `FuenteDatos` están excluidos del `Hash_Diferenciador` en los 9 Satellites
  - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7_

- [x] 10.2 (P) Verificar trazabilidad de imports y reutilización de utilidades
  - Confirmar que todos los notebooks importan configuración desde `utilities.LSDPConfiguracion` vía `obtener_configuracion(spark)`
  - Confirmar que todos los notebooks importan funciones helper desde `utilities.LSDPUtilidadPrincipal` sin redefinir lógica de hash ni constantes
  - Confirmar que las 2 funciones nuevas (`procesar_satellite`, `clasificar_por_umbral`) no contienen imports de LSDP
  - Confirmar que constantes `UMBRAL_*`, `TIPO_DATM`, `TIPO_CATM` y `HASH_*` se referencian desde `LSDPConfiguracion` y nunca están hardcodeadas
  - _Requirements: 1.3, 1.4, 14.1, 14.2, 14.3, 14.4, 14.5_

- [x] 10.3 Habilitar Change Data Feed en TODAS las streaming tables de Plata sin excepción
  - Requisito de incrementabilidad para las MV de Oro: Enzyme requiere CDF en todas las fuentes upstream para evitar `COMPLETE_RECOMPUTE`. Los Sats ya lo tenían; ahora se exige también para Hubs y Links.
  - Aplicar `table_properties` con `delta.enableChangeDataFeed=true` (junto a `delta.autoOptimize.autoCompact`, `delta.autoOptimize.optimizeWrite`, `delta.deletedFileRetentionDuration` y `delta.logRetentionDuration`) en `dp.create_streaming_table` de:
    - `LSDPPlataHubCliente.py` → `Hub_Cliente`
    - `LSDPPlataHubOperacion.py` → `Hub_Operacion`
    - `LSDPPlataHubTransaccion.py` → `Hub_Transaccion`
    - `LSDPPlataLinkClienteOperacion.py` → `Link_Cliente_Operacion`
    - `LSDPPlataLinkClienteTransaccion.py` → `Link_Cliente_Transaccion`
  - Test estático: `tests/test_notebooks_plata.py::test_todos_habilitan_change_data_feed` exige `table_properties` y `"delta.enableChangeDataFeed": "true"` en los 8 notebooks de Plata (3 Hubs + 2 Links + 3 Sats).
  - _Requirements: 12.x (cross-stage), enabler de spec `oro-modelo-estrella-mv-tiempo` tarea 6.2_

---

# Hallazgo Crítico — Cierre del Incremento (FIND-001)

**Fecha de detección:** 2026-04-13  
**Severidad:** CRÍTICA  
**Estado:** Identificado — pendiente de resolución en el próximo incremento

## Descripción del Problema

Al agregar nuevos parquets con una fecha posterior (`/año=2026/mes=04/dia=02/`) para simular comportamiento diario de datos, los 9 Satellites de la capa Plata fallaron fatalmente con el error:

> *"Streaming tables may only use append-only streaming sources. We detected an update or delete to one or more rows in the source table."*

### Satellites afectados (9 de 9)
| Notebook | Satellites |
|---|---|
| `LSDPPlataSatCliente.py` | Sat_Cliente_DatosEstables, Sat_Cliente_Contacto, Sat_Cliente_Clasificacion, Sat_Cliente_Financiero |
| `LSDPPlataSatOperacion.py` | Sat_Operacion_DatosEstables, Sat_Operacion_Montos, Sat_Operacion_FechasEvento |
| `LSDPPlataSatTransaccion.py` | Sat_Transaccion_DatosEstables, Sat_Transaccion_Montos |

## Causa Raíz

Incompatibilidad arquitectónica en el flujo `Materialized View (Bronce) → Streaming Table (Plata Satellites)`:

1. Las **Materialized Views de Bronce** (`CMSTFL`, `BLNCFL`, `TRXPFL`) filtran datos a `max(FechaRegistroParquet)`. Cuando se agregan parquets de un nuevo día, la MV se recalcula eliminando las filas del día anterior y reemplazándolas con las del nuevo día.
2. Este recálculo genera un **commit Delta non-append** (operación WRITE con deletes implícitos).
3. Los Satellites leían de estas MVs con `spark.readStream.table()`, que exige fuentes **append-only**.
4. Al detectar el commit non-append en la MV fuente, Lakeflow Declarative Pipelines dispara el error fatal en cada `@dp.append_flow`.

**Diagrama del flujo problemático:**
```
AutoLoader (ST temp, append-only ✓)
    → Materialized View (snapshot max fecha, non-append ✗)
        → Satellites (Streaming Table + append_flow, requiere append-only)
```

## Hotfix Aplicado (mitigación inmediata)

Se cambió la lectura de los Satellites de las **Materialized Views** a las **Streaming Tables temporales** (que sí son append-only):

| Notebook | Antes (MV — non-append) | Después (ST temp — append-only) |
|---|---|---|
| `LSDPPlataSatCliente.py` | `spark.readStream.table(_fuente)` | `dp.read_stream("CMSTFL_temp")` |
| `LSDPPlataSatOperacion.py` | `spark.readStream.table(_fuente)` | `dp.read_stream("BLNCFL_temp")` |
| `LSDPPlataSatTransaccion.py` | `spark.readStream.table(_fuente)` | `dp.read_stream("TRXPFL_temp")` |

**Nota operativa:** Tras desplegar el hotfix, se requiere un **Full Refresh** de los 9 Satellites afectados en Databricks para limpiar el estado de error y reiniciar los checkpoints de streaming con la nueva fuente.

## Resolución Pendiente — Próximo Incremento

El hotfix resuelve el error inmediato, pero introduce una consideración de diseño que debe evaluarse en el próximo incremento:

- **Evaluar el impacto**: Los Satellites ahora procesan **todos los datos acumulados** desde la ST temporal (no solo el snapshot del día más reciente de la MV). La función `procesar_satellite()` filtra por `Hash_Diferenciador`, pero el volumen de datos procesados por ejecución es mayor.
- **Rediseñar el flujo Bronce→Plata Satellites**: Considerar si el patrón `MV snapshot → ST satellite` debe reemplazarse definitivamente por `ST temp → ST satellite`, o si se requiere una estrategia intermedia (e.g., `skipChangeCommits`, particionamiento por fecha en la lectura streaming).
- **Validar comportamiento incremental**: Confirmar que el checkpoint de streaming de `dp.read_stream()` sobre tablas temporales LSDP funciona correctamente en ejecuciones incrementales diarias sin reprocesar todo el histórico.
- **Actualizar el diseño y los requisitos** de la spec para reflejar la decisión arquitectónica final.

---

## Incremento OPT-001 — Linaje transaccional sobre Change Data Feed (2026-04-28)

- [x] Crear `src/LSDP_Lab_DataVault_DWH/transformations/LSDPPlataVistaTRXPFLCDF.py` con `@dp.view vista_trxpfl_cdf` que lee TRXPFL con `readChangeFeed=true`, filtra `_change_type ∈ {insert, update_postimage}` y promueve `_commit_version` → `VersionCarga` y `_commit_timestamp` → `FechaCargaBronce`.
- [x] Migrar `LSDPPlataHubTransaccion.py`: lectura desde `dp.read_stream("vista_trxpfl_cdf")` y eliminación de la llamada a `procesar_hub` (TRXID es globalmente único).
- [x] Migrar `LSDPPlataLinkClienteTransaccion.py`: lectura desde la vista CDF y eliminación de `procesar_link`.
- [x] Migrar `LSDPPlataSatTransaccion.py` (`Sat_Transaccion_DatosEstables` y `Sat_Transaccion_Montos`): lectura desde la vista CDF, eliminación de `procesar_satellite_transaccional` y propagación de `VersionCarga` + `FechaCargaBronce` en el `select` final.
- [x] Conservar intactas `procesar_satellite/_hub/_link/_satellite_transaccional` en `LSDPUtilidadPrincipal.py` (sigue siendo necesario para el linaje maestro CMSTFL/BLNCFL).
- [x] Conservar `expect_all_or_fail` / `expect_all_or_drop`, `cluster_by` y `table_properties` de las tablas Plata.
- [x] Ajustar `tests/test_notebooks_plata.py`: relajar las aserciones de `procesar_hub`/`procesar_link`/`procesar_satellite_transaccional` para el linaje transaccional y añadir 5 tests OPT-001 (existencia y forma de la vista CDF, lectura desde `vista_trxpfl_cdf`, propagación de `VersionCarga`/`FechaCargaBronce`, ausencia de helpers de deduplicación).
- [x] Ejecutar `pytest -q`: 241/241 pasando.
- [x] Actualizar `spec.json` con el bloque `optimizations[OPT-001]` y bumpear `updated_at`.
- [x] Anexar la sección "Incremento OPT-001" a `design.md`, `tasks.md` y `research.md`.
