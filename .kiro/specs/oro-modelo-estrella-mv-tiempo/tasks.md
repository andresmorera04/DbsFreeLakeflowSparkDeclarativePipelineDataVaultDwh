# Implementation Plan — oro-modelo-estrella-mv-tiempo

> Distribución por olas (basada en design.md → Migration Strategy):
> - **P0 (prerrequisito secuencial)**: tareas 1 y 2.
> - **P1 (paralelizable)**: tarea 3 (3 notebooks de dimensiones independientes).
> - **P2**: tarea 4 (hecho) y tarea 5 (tests).

## 1. Actualización obligatoria de documentación previa al desarrollo

- [x] 1.1 Establecer línea base regex pre-edición y registrar coincidencias actuales
  - Ejecutar búsquedas regex en todo el repo para `Dim_Tiempo`, `current_date`, `spark.range` y `create_streaming_table.*Dim_Tiempo`
  - Registrar las coincidencias detectadas como inventario a remediar (gating R-04 aprobado)
  - _Requirements: 1.5, 1.7_

- [x] 1.2 Reescribir las secciones de `SYSTEM.md` que describen `Dim_Tiempo` con la arquitectura previa
  - Reemplazar la descripción de `Dim_Tiempo` como Streaming Table acumulativa por Vista Materializada incremental basada en valores distintos del campo de fecha de transacción del Satellite de montos
  - Sustituir el ejemplo de código de `Dim_Tiempo` por la nueva definición declarativa con lectura batch del Satellite, eliminando los patrones imperativos de "ayer/hoy", el rango sintético y las funciones no determinísticas
  - Actualizar la tabla de compatibilidad con Free Edition para retirar `Dim_Tiempo` de la fila de Streaming Tables y agregar la nota explícita sobre su nueva implementación como Vista Materializada incremental
  - Reescribir la "Regla especial para Dim_Tiempo" para describir el nuevo comportamiento incremental sin lógica de fechas explícita
  - Preservar todos los atributos derivados existentes de la dimensión (año, mes, trimestre, etc.)
  - Documentar el supuesto aprobado de "operación dominante por cliente" para la FK de operación del hecho ATM (mitigación R-02)
  - Documentar la propiedad de estabilidad de las llaves subrogadas Tipo 1 y la regla de consumo BI de no referenciar valores literales (mitigación R-03)
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.6_

- [x] 1.3 Sincronizar archivos de steering y specs con la nueva arquitectura
  - Revisar y actualizar archivos de `.kiro/steering/` y otros specs que mencionen la arquitectura previa de la dimensión de tiempo
  - Mantener coherencia terminológica entre product, tech, structure y SYSTEM
  - _Requirements: 1.5_

- [x] 1.4 Validación de cierre de la actualización documental (gating R-04 aprobado)
  - Re-ejecutar las búsquedas regex de la subtarea 1.1 y verificar que no queden referencias residuales a la arquitectura previa
  - Confirmar que la fase documental está completa y aprobada antes de iniciar cualquier código de la Medalla de Oro
  - _Requirements: 1.7_

## 2. Crear el módulo de utilidades reutilizables de la Medalla de Oro

- [x] 2.1 Implementar el helper de selección del último registro por hash (ámbito: Sats de estado)
  - Recibir un Satellite de **estado** (Cliente u Operación), el nombre de la columna de hash y opcionalmente la columna de orden temporal
  - Devolver una sola fila por hash con el registro más reciente, usando ventana determinística con desempate por hash diferenciador
  - Documentar en docstring que el helper **NO** se aplica a Satellites transaccionales (Sat_Transaccion_*), que son una fila por hash por diseño
  - Garantizar pureza funcional: sin caché, sin acciones, sin acceso a contexto Spark
  - _Requirements: 3.2, 4.2, 6.2, 6.3, 6.7_

- [x] 2.2 Implementar el helper de asignación de llaves subrogadas estables
  - Calcular un identificador entero largo a partir del orden lexicográfico del hash de negocio
  - Garantizar estabilidad para el mismo conjunto de hashes entre ejecuciones
  - Documentar en docstring la propiedad: el ID puede cambiar si cambia el conjunto de entrada (mitigación R-03)
  - _Requirements: 3.3, 4.3, 6.2, 6.3, 6.7, 8.4_

- [x] 2.3 Implementar el helper de selección de operación dominante por cliente
  - Combinar Hub de operación y Link cliente-operación
  - Seleccionar una sola operación por cliente con prioridad por secuencia de saldo descendente y desempate por hash de operación ascendente
  - Devolver un DataFrame con una fila por hash de cliente con su hash de operación dominante
  - _Requirements: 5.6, 6.2, 6.3, 6.7_

- [x] 2.4 Implementar el helper de validación de columnas requeridas en Oro
  - Recibir un DataFrame, una lista de columnas obligatorias y el nombre de la entidad
  - Lanzar un error explícito con la columna faltante y la entidad cuando falte alguna columna
  - Diseñar para falla rápida en CI antes de que el motor declarativo registre el esquema
  - _Requirements: 8.1, 8.5_

## 3. Construir las tres dimensiones del modelo estrella

- [x] 3.1 (P) Implementar el notebook de la dimensión de tiempo como Vista Materializada incremental
  - Leer en modo batch el Satellite de montos de transacción y obtener los valores distintos del campo de fecha
  - Renombrar la columna fuente al nombre de llave de la dimensión y derivar todos los atributos calendario con funciones determinísticas nativas
  - Usar exclusivamente operadores compatibles con incremental refresh (sin joins, sin window, sin funciones no determinísticas)
  - Configurar liquid clustering por la llave de fecha
  - Declarar las expectations: como `expect_all_or_fail` solo la no nulidad de la llave de fecha y el rango válido del mes; degradar la cota de año a `expect` (warn) para no abortar el pipeline ante valores atípicos
  - Aplicar la mitigación R-01 aprobada: no introducir lógica defensiva si el motor cambia la elegibilidad
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 6.1, 6.5, 6.6, 6.7, 6.8, 7.1, 7.3, 7.4, 7.6_

- [x] 3.2 (P) Implementar el notebook de la dimensión de cliente como Vista Materializada Tipo 1
  - Usar el Hub de cliente como origen base del join (aporta el hash de cliente y el identificador de negocio); aplicar el helper de último por hash a cada uno de los cuatro Satellites de cliente y combinarlos con LEFT JOIN desde el Hub para preservar clientes sin Sat opcional
  - Asignar la llave subrogada de cliente con el helper de IDs estables
  - Construir el esquema final exclusivamente con la lista cerrada de columnas definida en el diseño (tabla `Dim_Cliente`); excluir cualquier columna no listada, las columnas exclusivas de Bronce y la metadata DV (`Hash_Diferenciador`, `FuenteDatos`)
  - Renombrar columnas de Plata a la convención PascalCase de Oro según la tabla cerrada
  - Respetar la convención de booleanos en Oro: preservar como string los indicadores categóricos provenientes de Plata (`IndicadorVip`, etc.); no convertir a boolean nativo
  - Configurar liquid clustering por la llave de cliente y declarar las expectations de no nulos
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 6.1, 6.2, 6.3, 6.5, 6.6, 6.7, 7.1, 7.3, 7.4, 7.5, 7.6_

- [x] 3.3 (P) Implementar el notebook de la dimensión de operación como Vista Materializada Tipo 1
  - Usar el Hub de operación como origen base del join (aporta el hash de operación, el identificador de cliente propietario y la secuencia de saldo); aplicar el helper de último por hash a cada Satellite de operación y combinarlos con LEFT JOIN desde el Hub para preservar operaciones sin Sat opcional
  - Asignar la llave subrogada de operación con el helper de IDs estables
  - Construir el esquema final exclusivamente con la lista cerrada de columnas definida en el diseño (tabla `Dim_Operacion`); excluir cualquier columna no listada, las columnas exclusivas de Bronce y la metadata DV
  - Renombrar columnas de Plata a la convención PascalCase de Oro según la tabla cerrada
  - Respetar la convención de booleanos en Oro: preservar como string los clasificadores provenientes de Plata (`CategoriaSaldo`, `EstadoUtilizacionCredito`, `IndicadorSobregiro`); no convertir a boolean nativo
  - Configurar liquid clustering por la llave de operación y declarar las expectations de no nulos
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 6.1, 6.2, 6.3, 6.5, 6.6, 6.7, 7.1, 7.3, 7.4, 7.5, 7.6_

## 4. Construir la tabla de hechos de transacciones ATM

- [x] 4.1 Implementar el notebook del hecho de transacciones ATM con FKs resueltas
  - Leer los Satellites de transacción (datos estables y montos) y el Hub de transacción con lectura batch directa, **sin aplicar el helper de último por hash** (los Sats transaccionales tienen una fila por hash por diseño)
  - Filtrar por los tipos de transacción ATM usando las constantes centralizadas, antes de cualquier join
  - Resolver la FK de cliente vía Link cliente-transacción y la dimensión de cliente
  - Resolver la FK de operación con el helper de operación dominante por cliente y la dimensión de operación (mitigación R-02 aprobada)
  - Tomar la fecha de transacción del Satellite de montos para garantizar correspondencia con la dimensión de tiempo
  - Renombrar a la convención de Oro y derivar las banderas booleanas de retiro y depósito (`BooleanType` nativo según la convención de booleanos en Oro) con expresiones condicionales nativas
  - Antes de la proyección final, calcular una columna marcador de duplicado por hash de transacción con `row_number` sobre `Window.partitionBy(Hash_Transaccion).orderBy(Hash_Diferenciador)`; declarar una expectation `expect_or_drop` con el predicado simple `_marca_duplicado = 1` para descartar duplicados sin abortar el pipeline; eliminar la columna marcador del esquema final
  - Configurar liquid clustering por fecha y llave de cliente
  - Declarar expectations `expect_all_or_fail` para las FKs y de dominio para el tipo de transacción
  - Usar broadcast hint únicamente como sugerencia y nunca el broadcast del contexto
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 7.1, 7.3, 7.4, 7.5, 7.6_

## 5. Construir la suite de pruebas unitarias de la Medalla de Oro

- [x] 5.1 (P) Implementar las pruebas funcionales del módulo de utilidades de Oro
  - Validar el comportamiento del helper de último por hash con DataFrames sintéticos, incluyendo determinismo ante empates
  - Validar la estabilidad de la asignación de llaves subrogadas para el mismo conjunto de hashes (mitigación R-03 aprobada)
  - Validar que la selección de operación dominante prioriza correctamente y desempata por hash
  - Validar que el helper de columnas obligatorias falla con mensaje explícito cuando falta una columna
  - _Requirements: 8.4, 8.5_

- [x] 5.2 (P) Implementar las pruebas estructurales estáticas de los notebooks de Oro
  - Verificar que cada notebook de Oro use el decorador de Vista Materializada con nombre de tres partes
  - Verificar que el notebook de la dimensión de tiempo lea el Satellite de montos y no use funciones no determinísticas, rango sintético ni Streaming Table
  - Verificar que el notebook del hecho ATM filtre exclusivamente por los tipos de transacción ATM, lea los Sats transaccionales sin aplicar el helper de último por hash, declare la expectation `expect_or_drop` de unicidad sobre la columna marcador y configure el liquid clustering esperado
  - Verificar que los notebooks de las dimensiones de cliente y operación expongan exactamente las columnas definidas en las tablas cerradas del diseño y respeten la convención de booleanos en Oro (string para clasificadores provenientes de Plata)
  - Verificar que el notebook del hecho ATM exponga exactamente las columnas de la tabla cerrada de `Hec_Transacciones_ATM` y que NO propague columnas internas con prefijo `_` (en particular `_marca_duplicado`)
  - Verificar la ausencia transversal de APIs prohibidas en Serverless (RDD, caché, contexto Spark, UDFs, threading)
  - _Requirements: 8.1, 8.2, 8.3, 8.5_

## 6. Refactorización de Hec_Transacciones_ATM para procesamiento incremental

- [x] 6.1 Refactorizar `Hec_Transacciones_ATM` para procesamiento incremental correcto como MV
  - Mantener la tabla de hechos como `@dp.materialized_view` — bajo ninguna circunstancia debe ser una Streaming Table
  - Eliminar completamente el campo `_marca_duplicado` y la Window function asociada del notebook; la unicidad de `Hash_Transaccion` está garantizada por Plata (`procesar_satellite_transaccional`)
  - Eliminar el import de `pyspark.sql.window.Window` del notebook de Hec (el Window permanece solo dentro de `LSDPUtilidadOro.py` vía `seleccionar_operacion_dominante`, operando sobre datos batch)
  - Usar `spark.read.table()` para TODAS las fuentes — nunca `readStream` dentro de `@dp.materialized_view`
  - Estructurar la MV con operadores que favorezcan el refresh incremental por CDF; si `seleccionar_operacion_dominante` introduce un Window que bloquee el CDF, LSDP caerá a full refresh automáticamente — comportamiento aceptado
  - Actualizar `design.md`: Hec vuelve a ser MV con `spark.read.table()`, sin `readStream`, sin `_marca_duplicado`; actualizar diagrama Mermaid, trazabilidad R5.1 y sección de componentes
  - Actualizar `tests/test_notebooks_oro.py`: verificar `@dp.materialized_view`, `spark.read.table` (no `readStream`), ausencia de `_marca_duplicado`
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 7.1, 7.3, 7.4, 7.5, 7.6_

- [x] 6.2 Habilitar refresh incremental real de `Hec_Transacciones_ATM` (causa raíz del COMPLETE_RECOMPUTE)
  - **Causa raíz identificada**: `row_number() OVER (...)` dentro de `seleccionar_operacion_dominante` (top‑1 por grupo) NO es elegible para mantenimiento incremental por Enzyme; ese operador, presente en el plan del hecho, fuerza `COMPLETE_RECOMPUTE` en cada ejecución. Causas secundarias: dimensiones cuyos `DimId*` se reasignaban con `dense_rank()` y fuentes upstream sin Change Data Feed.
  - Crear MV auxiliar `LSDPOroMapClienteOperacionDominante.py` (`Map_Cliente_Operacion_Dominante`) declarada con `@dp.materialized_view(temporary=True, ...)` y nombre **no calificado** — dataset interno del pipeline, NO publicado en Unity Catalog — que encapsule el `Window/row_number` y exponga `Hash_Cliente, Hash_Operacion, DimIdOperacion` con CDF habilitado. Esta MV concentra el `COMPLETE_RECOMPUTE` (cardinalidad ≈ #clientes) y libera al hecho.
  - Refactorizar `LSDPOroHecTransaccionesATM.py`: eliminar el import y uso de `seleccionar_operacion_dominante`, eliminar las lecturas a `Hub_Operacion`, `Link_Cliente_Operacion` y `Dim_Operacion`. Resolver `DimIdOperacion` consumiendo `Map_Cliente_Operacion_Dominante` con `spark.read.table("Map_Cliente_Operacion_Dominante")` (nombre no calificado, porque la MV es `temporary` y no se publica en UC) más un join equi-key por `Hash_Cliente`. El plan del hecho queda compuesto solo por filtros, joins equi-key y proyecciones — elegible para refresh incremental por Enzyme.
  - Política reforzada: para la tabla de hechos, los datos de cliente y operación se obtienen principalmente desde Hubs y Links; los Sats de Cliente/Operación NO se leen desde el hecho.
  - Refactorizar `asignar_dim_id_estable` en `LSDPUtilidadOro.py` para usar `xxhash64(hash_col).cast("long")` en lugar de `dense_rank() OVER (...)` — ID determinista por fila, invariante a la composición del DataFrame, sin operadores de plan que bloqueen el incremental.
  - Habilitar `delta.enableChangeDataFeed=true` (junto al resto de `table_properties` estándar) en TODAS las streaming tables de Plata sin excepción: `Hub_Cliente`, `Hub_Operacion`, `Hub_Transaccion`, `Link_Cliente_Operacion`, `Link_Cliente_Transaccion` (los Sats ya lo tenían).
  - Actualizar `design.md`: nueva sección `NotebookMapClienteOperacionDominante`, dependencias del hecho actualizadas (consume `Map_Cliente_Operacion_Dominante`, ya no `Hub_Operacion`/`Link_Cliente_Operacion`/`Dim_Operacion`), ajustar tabla de columnas (origen de `DimIdOperacion`) y firmar la nueva semántica de `asignar_dim_id_estable`.
  - Actualizar tests:
    - `tests/test_notebooks_plata.py`: nuevo test `test_todos_habilitan_change_data_feed` exigiendo `delta.enableChangeDataFeed=true` en todas las streaming tables de Plata.
    - `tests/test_notebooks_oro.py`: registrar `MapClienteOperacionDominante` en el catálogo de notebooks; `test_hec_atm_usa_operacion_dominante` exige consumo de `Map_Cliente_Operacion_Dominante` y prohíbe `Window` y `seleccionar_operacion_dominante` directos en el hecho; nuevo `test_hec_atm_no_lee_sat_operacion_ni_sat_cliente`; nuevos tests específicos de la MV auxiliar (uso de `@dp.materialized_view`, encapsulación del Window, lectura de Hub/Link/Dim, columnas cerradas, CDF habilitado).
    - `tests/test_utilidad_oro.py`: reemplazar las aserciones de "secuencial denso" y "orden lexicográfico" por tests de invariancia del ID respecto a la composición del DataFrame (xxhash64 determinista por fila).
  - _Requirements: 5.6, 7.1, 8.1, 8.4_

- [x] 6.3 Reducir el plan del hecho a 1 join (mitigación NUM_JOINS_THRESHOLD_EXCEEDED)
  - **Causa raíz observada en producción**: tras aplicar 6.2, el log de planning del hecho mostró `INCREMENTAL_PLAN_REJECTED_BY_COST_MODEL` con subtipo `NUM_JOINS_THRESHOLD_EXCEEDED` y `MAINTENANCE_TYPE_COMPLETE_RECOMPUTE` como técnica elegida. Enzyme construía un plan incremental válido (sin Window, con CDF en todas las fuentes), pero el cost model lo descartaba por contener 5 joins encadenados sobre fuentes grandes.
  - **Mitigación**: pre-componer datos y FKs en MVs `temporary` para que el hecho quede con un único join.
  - Cambios:
    - Extender `LSDPOroMapClienteOperacionDominante.py`: agregar `LEFT JOIN F.broadcast(Dim_Cliente)` y exponer `DimIdCliente` en el esquema cerrado (queda: `Hash_Cliente, Hash_Operacion, DimIdCliente, DimIdOperacion`). Mantiene `temporary=True` y CDF.
    - Crear `LSDPOroTrxATMEnriquecida.py` (NUEVO, `temporary=True`, CDF) que pre-compone `Sat_Transaccion_DatosEstables` (con filtro `DATM/CATM` aplicado aguas arriba) + `Sat_Transaccion_Montos` + `Hub_Transaccion` + `Link_Cliente_Transaccion` por `Hash_Transaccion`. `cluster_by=["Hash_Cliente", "FechaClave"]`. Esquema cerrado de 13 columnas.
    - Refactorizar `LSDPOroHecTransaccionesATM.py` a un único broadcast join LEFT entre `Trx_ATM_Stream` y `Map_Cliente_Operacion_Dominante` por `Hash_Cliente`, más derivaciones `EsRetiro`/`EsDeposito` y proyección final. Eliminar todas las lecturas directas de Sats, Hubs, Links y Dimensiones desde el hecho.
  - Plan resultante del hecho: 1 join + 2 `withColumn` + projection — debajo del umbral del cost model y elegible para refresh incremental por CDF.
  - Actualizar `design.md`: nueva sección `NotebookTrxATMEnriquecida`; sección `NotebookMapClienteOperacionDominante` reescrita (ahora también resuelve `DimIdCliente`); sección `NotebookHecTransaccionesATM` reescrita (deps = solo las dos MVs temporary; tabla de columnas con origen actualizado).
  - Actualizar tests en `tests/test_notebooks_oro.py`:
    - Registrar `TrxATMEnriquecida` en el catálogo `ORO`.
    - Bloque nuevo de tests AST para `Trx_ATM_Stream`: `temporary=True`, CDF, filtro DATM/CATM aguas arriba, lectura de los 4 datasets de Plata, columnas cerradas (13).
    - Endurecer tests del hecho: `test_hec_atm_no_lee_sats_transaccionales_directamente`, prohibir `Hub_Transaccion`, `Link_Cliente_Transaccion`, `Dim_Cliente` en el archivo; nuevo `test_hec_atm_un_solo_join` (cuenta literal de `.join(` == 1); `test_hec_atm_referencia_trx_enriquecida_por_nombre_no_calificado`.
    - Ajustar tests de `Map_Cliente_Operacion_Dominante`: schema con `DimIdCliente`, lectura de `Dim_Cliente`.
  - _Requirements: 5.6, 7.1, 8.1, 8.4_

- [x] 6.4 Mitigación CHANGESET_SIZE_THRESHOLD_EXCEEDED (Opción D: A + B combinados)
  - **Causa raíz observada en producción**: tras 6.3, el log de planning del hecho mostró un nuevo rechazo del cost model — `INCREMENTAL_PLAN_REJECTED_BY_COST_MODEL / CHANGESET_SIZE_THRESHOLD_EXCEEDED`, decisión `MAINTENANCE_TYPE_COMPLETE_RECOMPUTE`. Causas concretas:
    - `Map_Cliente_Operacion_Dominante` emite changeset ≈ 199% de su contenido (toda la tabla en INSERT + DELETE) porque `seleccionar_operacion_dominante` usa `row_number()` sobre `Window.partitionBy("Hash_Cliente")` — operador no elegible para mantenimiento incremental por Enzyme → cae en `COMPLETE_RECOMPUTE` cada corrida.
    - `Trx_ATM_Stream` (MV) emite changeset ≈ 113% por la misma razón estructural (MV recomputada completa en cada planificación).
    - Cumulativamente, el delta upstream que llega al hecho ≈ 30M cambios contra ~14M filas target → cost model elige `COMPLETE_RECOMPUTE`.
  - **Mitigación (Opción D = A + B combinados)**:
    - **(B) Eliminar Window de `seleccionar_operacion_dominante`**: reescritura a `groupBy("Hash_Cliente").agg(F.max(F.struct("SecuenciaSaldo", "Hash_Operacion")))`. Esta agregación SÍ es elegible para mantenimiento incremental por Enzyme. Desempate determinista en empates: DESC sobre `Hash_Operacion` (desviación menor del criterio original ASC, aceptada).
    - **(A) Convertir `Trx_ATM_Stream` de MV a Streaming Table**: declarada con `@dp.table(name="Trx_ATM_Stream", temporary=True, ...)` sobre una función que devuelve un DataFrame de streaming. Fuente `Sat_Transaccion_DatosEstables` con `spark.readStream.table(...)`; lookups (`Sat_Transaccion_Montos`, `Hub_Transaccion`, `Link_Cliente_Transaccion`) con `spark.read.table(...)` (stream-static joins). Lakeflow SDP la materializa automáticamente como Streaming Table. **No se usa `dp.create_streaming_table(...)`** porque esa API no acepta el argumento `temporary` en el runtime actual (`TypeError: create_streaming_table() got an unexpected keyword argument 'temporary'` observado en FULL REFRESH). Por la semántica append-only, su CDF emite solo los `Hash_Transaccion` realmente nuevos por micro-batch.
  - Cambios:
    - `src/LSDP_Lab_DataVault_DWH/utilities/LSDPUtilidadOro.py`: reescribir `seleccionar_operacion_dominante` con `groupBy + max(struct)`; documentar tiebreak DESC.
    - `src/LSDP_Lab_DataVault_DWH/transformations/LSDPOroMapClienteOperacionDominante.py`: actualizar narrativa/docstring (ya no "encapsula Window"; ahora "agregación incrementalizable").
    - `src/LSDP_Lab_DataVault_DWH/transformations/LSDPOroTrxATMEnriquecida.py`: reescribir como Streaming Table + AppendFlow con stream-static joins.
    - `src/LSDP_Lab_DataVault_DWH/transformations/LSDPOroHecTransaccionesATM.py`: SIN CAMBIOS (el cambio es transparente; sigue leyendo `Trx_ATM_Stream` y `Map_Cliente_Operacion_Dominante` por nombre no calificado).
  - Actualizar `design.md`: secciones `NotebookMapClienteOperacionDominante` (sin Window; elegible incremental) y `NotebookTrxATMEnriquecida` (Streaming Contract en lugar de Batch).
  - Actualizar tests en `tests/test_notebooks_oro.py`:
    - Reemplazar `test_map_dom_encapsula_window` por `test_map_dom_no_usa_window_ni_row_number`: verificar sobre el cuerpo del helper en `LSDPUtilidadOro.py` que NO usa `F.row_number()` ni `.over(...)`, y que SÍ usa `groupBy(...) + F.max(F.struct(...))`.
    - Reemplazar `test_trx_atm_enriquecida_usa_materialized_view` por `test_trx_atm_enriquecida_es_streaming_table`: verificar uso de `@dp.table(`, `temporary=True`, `spark.readStream.table` y ausencia de `@dp.materialized_view` y de `dp.create_streaming_table`.
  - Actualizar tests en `tests/test_utilidad_oro.py`: reemplazar `test_seleccionar_operacion_dominante_desempata_por_hash_asc` por `test_seleccionar_operacion_dominante_desempata_de_forma_determinista` (tiebreak DESC, determinista).
  - Resultado esperado del cost model: el changeset de Map se reduce drásticamente (solo grupos cuyo `max(struct(...))` cambia), y el changeset de Trx queda acotado a transacciones realmente nuevas → el plan del hecho deja de exceder el `CHANGESET_SIZE_THRESHOLD` y el mantenimiento incremental (`ROW_BASED` u otro) pasa a ser elegible y elegido por el cost model.
  - _Requirements: 5.6, 7.1, 8.1, 8.4_

- [x] 6.5 Renombrado de `Trx_ATM_Enriquecida` → `Trx_ATM_Stream` (mitigación CANNOT_CHANGE_DATASET_TYPE)
  - **Causa raíz observada en producción** (al aplicar 6.4 en FULL REFRESH): `Cannot change the dataset type of a pipeline table from MATERIALIZED_VIEW to STREAMING_TABLE for ...trx_atm_enriquecida`. Lakeflow SDP NO permite cambiar el tipo de un dataset existente bajo el mismo identificador; el storage table de la versión MV anterior queda "type-locked".
  - **Mitigación de raíz**: renombrar el dataset a `Trx_ATM_Stream`. Al ser un identificador nuevo, SDP crea un storage table nuevo desde cero como Streaming Table sin colisionar con la versión MV anterior. Evita depender de operaciones manuales de drop por parte del operador.
  - Cambios:
    - `src/LSDP_Lab_DataVault_DWH/transformations/LSDPOroTrxATMEnriquecida.py`: `name="Trx_ATM_Stream"` en `@dp.table(...)`, función renombrada a `trx_atm_stream`, narrativa actualizada con la nota sobre `CANNOT_CHANGE_DATASET_TYPE`.
    - `src/LSDP_Lab_DataVault_DWH/transformations/LSDPOroHecTransaccionesATM.py`: `spark.read.table("Trx_ATM_Stream")` y narrativa actualizada.
    - El nombre del archivo `LSDPOroTrxATMEnriquecida.py` se conserva (cambio limitado al identificador del dataset y a su función decorada).
  - Tests: actualizar `test_hec_atm_referencia_trx_enriquecida_por_nombre_no_calificado` y `test_trx_atm_enriquecida_es_streaming_table` para asertar `Trx_ATM_Stream`; mantener nombres de tests por estabilidad histórica.
  - Specs: `design.md` y `tasks.md` ya migrados (todas las menciones del dataset usan `Trx_ATM_Stream`).
  - _Requirements: 5.6, 7.1, 8.1, 8.4_

- [x] 6.6 Mitigación CHANGESET_SIZE_THRESHOLD persistente (Solución 1: pre-resolver FKs en `Trx_ATM_Stream` + Solución 3: archivos pequeños como refuerzo)
  - **Causa raíz observada en producción** (al aplicar 6.5): el JSON `planning_information` evidenció que el cost model SEGUÍA eligiendo `COMPLETE_RECOMPUTE` para `Hec_Transacciones_ATM` aun después de eliminar Window y reducir joins. Métricas:
    - `Map_Cliente_Operacion_Dominante`: changeset = **199%** (8M filas sobre 4M target).
    - `Trx_ATM_Stream`: changeset = **79%** (9.5M filas sobre 12M target del Hec).
    - `ROW_BASED` cost = **6.5e22** vs `COMPLETE_RECOMPUTE` cost = **1.35e16** — 6 órdenes de magnitud de diferencia. El cost model elegía correctamente `COMPLETE_RECOMPUTE` porque el changeset de los upstreams broadcast superaba el target del Hec.
  - **Constraint del usuario**: NO cambiar `Hec_Transacciones_ATM` a Streaming Table; debe seguir siendo `@dp.materialized_view`.
  - **Solución 1 — pre-resolver FKs en `Trx_ATM_Stream`** (cambio arquitectónico principal):
    - Mover el join Hec→Map dentro de `Trx_ATM_Stream` como un 5° join estático por `Hash_Cliente` (sort-merge / shuffle-hash, **sin** `F.broadcast` — Map tiene ~4M filas y la `BroadcastHashedRelation` excede la memoria del executor; ambas tablas clusterizadas por `Hash_Cliente` permiten un shuffle eficiente), agregando `DimIdCliente` y `DimIdOperacion` al esquema cerrado de la Streaming Table (15 columnas en lugar de 13).
    - Resultado: `Hec_Transacciones_ATM` queda con plan = `read("Trx_ATM_Stream")` + 2 `withColumn` (EsRetiro, EsDeposito) + `select` — **CERO joins, cero agregaciones, cero ventanas, cero lecturas a Map**. El changeset masivo de `Map_Cliente_Operacion_Dominante` deja de propagarse al hecho porque el hecho ya no la consume directamente; el changeset que llega al hecho es exclusivamente el delta append-only de `Trx_ATM_Stream`, trivialmente elegible para `ROW_BASED`.
    - Nueva expectation `expect_all_or_fail` en `Trx_ATM_Stream`: `dim_id_cliente_no_nulo: "DimIdCliente IS NOT NULL"`.
    - **Semántica FK-fijada-al-append** (aceptada por el usuario): las FKs (`DimIdCliente`, `DimIdOperacion`) quedan congeladas al momento del append. Cambios posteriores en `Map_Cliente_Operacion_Dominante` NO re-enriquecen transacciones históricas. Esta es la semántica correcta para una tabla de hechos transaccionales (refleja el estado del cliente al momento de la transacción).
  - **Solución 3 — refuerzo de propiedades Delta** (no-arquitectónico):
    - Agregar `delta.targetFileSize=16mb` y `delta.tuneFileSizesForRewrites=true` a `table_properties` de `Trx_ATM_Stream` y `Hec_Transacciones_ATM`. Archivos más pequeños y focalizados bajan el coste estimado de `ROW_BASED` por el cost model.
  - Cambios:
    - `src/LSDP_Lab_DataVault_DWH/transformations/LSDPOroTrxATMEnriquecida.py`: nueva 4ª regla en `_validaciones_trx_atm`; `_PROP_TABLE` extendido (7 props, agrega targetFileSize y tuneFileSizesForRewrites con comentario justificativo); `_COLUMNAS_CERRADAS_TRX_ATM` extendido a **15 columnas** (agrega `DimIdCliente`, `DimIdOperacion`); función `trx_atm_stream()` lee `Map_Cliente_Operacion_Dominante` por nombre no calificado y agrega `.join(map_dom, on="Hash_Cliente", how="left")` como 4° join (sin `F.broadcast` por OOM observado); docstring actualizado con explicación de Sol. 1 y nota sobre el OOM evitado.
    - `src/LSDP_Lab_DataVault_DWH/transformations/LSDPOroHecTransaccionesATM.py`: eliminar lectura de `Map_Cliente_Operacion_Dominante` y join broadcast por `Hash_Cliente`; pipeline reducido a `read + 2 withColumn + select`; `table_properties` ahora incluye `delta.targetFileSize=16mb` y `delta.tuneFileSizesForRewrites=true`; header narrativo y docstring actualizados (cero joins, FKs pre-resueltas, mitigación Sol. 1 + Sol. 3).
    - `tests/test_notebooks_oro.py`: `test_hec_atm_un_solo_join` → `test_hec_atm_sin_joins` (cuenta `.join(` == 0); `test_hec_atm_referencia_map_dom_por_nombre_no_calificado` → `test_hec_atm_no_consume_map_dom_directamente`; `test_hec_atm_usa_operacion_dominante` reescrito (asegura que Hec consume `Trx_ATM_Stream` y NO `"Map_Cliente_Operacion_Dominante"`); `test_trx_atm_enriquecida_lee_los_cuatro_datasets_plata` → `test_trx_atm_enriquecida_lee_los_cinco_datasets_upstream`; `test_trx_atm_enriquecida_columnas_cerradas` extendido a 15 columnas con `DimIdCliente`/`DimIdOperacion`; nuevos `test_trx_atm_enriquecida_target_file_size_reforzado` y `test_hec_atm_target_file_size_reforzado` (Sol. 3).
    - `.kiro/specs/oro-modelo-estrella-mv-tiempo/design.md`: secciones `NotebookTrxATMEnriquecida` (5 fuentes incluyendo Map; esquema 15 cols; semántica FK-at-append; Sol. 3 props) y `NotebookHecTransaccionesATM` (cero joins; lee solo `Trx_ATM_Stream`; Sol. 3 props; tabla de columnas con origen actualizado).
  - **Resultado esperado**: `Hec_Transacciones_ATM` con plan = read + 2 withColumn + select; `ROW_BASED` cost trivialmente menor que `COMPLETE_RECOMPUTE`; el cost model elige mantenimiento incremental (`SELECTED_ROW_BASED_INCREMENTAL`) en cada refresh, eliminando definitivamente el bloqueo `CHANGESET_SIZE_THRESHOLD_EXCEEDED`.
  - **Acción operacional requerida**: dado que `Trx_ATM_Stream` cambia su esquema (de 13 a 15 columnas), Lakeflow SDP requerirá un **FULL REFRESH** de `Trx_ATM_Stream` y de `Hec_Transacciones_ATM` en el primer despliegue.
  - **Incidente observado en FULL REFRESH (corregido en esta misma tarea)**: el primer FULL REFRESH falló con `Photon SparkOutOfMemoryError` durante `BuildHashedRelation` (`BroadcastHashedRelation` de **1.768 GiB**, var-len data 976 MiB) en `bronce.lab2.trx_atm_stream`. Causa raíz: `F.broadcast(map_dom)` forzaba broadcast de ~4M filas con `Hash_Cliente` string, excediendo la memoria del executor. Mitigación: eliminar el hint `F.broadcast` y dejar al optimizer elegir el algoritmo (sort-merge / shuffle-hash). Ambas tablas (`Trx_ATM_Stream` y `Map_Cliente_Operacion_Dominante`) están clusterizadas por `Hash_Cliente`, lo que hace el shuffle eficiente.
- [x] 6.7 Corrección B.2 — `dropDuplicates` en fuentes estáticas de `Trx_ATM_Stream` (defensa en profundidad)
  - **Causa raíz observada en producción de lab**: tras desplegar la corrección B.1 (deduplicación de Satellites por `hash_col` solo), el `GROUP BY IdentificadorTransaccion HAVING COUNT(*) > 1` en `Hec_Transacciones_ATM` seguía devolviendo `Q = 11` por transacción en datos residuales pre-B.1. La causa: `Sat_Transaccion_DatosEstables` ya era append-only (fuente streaming), pero `Sat_Transaccion_Montos`, `Hub_Transaccion` y `Link_Cliente_Transaccion` eran lookups estáticos sin deduplicación — si cualquiera de estos tenía N filas del mismo `Hash_Transaccion`, el stream-static join producía N duplicados por batch.
  - **Solución**: Añadir `.dropDuplicates(["Hash_Transaccion"])` a los tres lookups estáticos transaccionales (NO a `Map_Cliente_Operacion_Dominante` que deduplica por `Hash_Cliente`) inmediatamente después del `select()` y antes del join.
  - **Cambios**:
    - `src/LSDP_Lab_DataVault_DWH/transformations/LSDPOroTrxATMEnriquecida.py`: `.dropDuplicates(["Hash_Transaccion"])` en `sat_montos`, `hub_trx` y `link_cli_trx` (3 líneas añadidas).
    - `tests/test_notebooks_oro.py`: 4 nuevos tests bajo `# ─── B.2 — Deduplicación de fuentes estáticas en Trx_ATM_Stream ─────────────────`.
  - **Acción operacional requerida**: FULL REFRESH del pipeline para regenerar Plata y Oro desde Bronce; el query de verificación debe devolver 0 filas después.
  - _Requirements: 5.6, 7.1_
