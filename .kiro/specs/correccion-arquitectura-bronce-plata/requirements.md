# Requirements Document

## Introducción

Este documento define los requisitos del incremento correctivo **correccion-arquitectura-bronce-plata**, cuyo objetivo es reestructurar las medallas Bronce y Plata del pipeline LSDP Data Vault DWH. Los cambios principales son: (1) simplificación de Bronce a una sola Streaming Table persistente por fuente, eliminando las Materialized Views de snapshot; (2) migración completa de todas las entidades Data Vault en Plata (Hubs, Links, Satellites) al patrón Streaming Table + Append Flow con comportamiento Append-Only; (3) refinamiento de la lógica de detección de cambios en Satellites estándar (Cliente, Operación) mediante la función `procesar_satellite()`; (4) acumulación histórica completa en los Satellites del Hub_Transaccion sin aplicar ROW_NUMBER, usando la llave compuesta `Hash_Transaccion` + `fecha_transaccion` para evitar duplicados, mediante la nueva función `procesar_satellite_transaccional()` — manteniendo `Hash_Diferenciador` para trazabilidad sin participación en deduplicación; y (5) actualización integral de la documentación técnica y el Steering del proyecto.

## Descripción del Proyecto (Entrada)

Incremento correctivo que reestructura la arquitectura de las medallas Bronce y Plata del pipeline LSDP Data Vault DWH:

### Cambios en Bronce
- Eliminar las vistas materializadas (Materialized Views) que contenían solo el snapshot más reciente de cada fuente.
- Las Streaming Tables dejan de ser temporales (`temporary=True`) y pasan a registrarse directamente en Unity Catalog.
- Los nombres de las Streaming Tables adoptan los nombres que anteriormente tenían las vistas materializadas (ej: `CMSTFL_temp` → `CMSTFL`). La capa de Bronce queda compuesta por **una sola tabla por fuente** (Streaming Table registrada en Unity Catalog).

### Cambios en Plata — Consumo desde Bronce
- Todas las tablas de Plata (Hubs, Links y Satellites) deben consumir directamente de las Streaming Tables de Bronce registradas en Unity Catalog con los nombres definitivos, utilizando `dp.read_stream()` como mecanismo de lectura uniforme.

### Cambios en Plata — Todas las tablas Data Vault como Streaming Tables
- Hubs, Links y Satellites deben ser **Streaming Tables** con comportamiento Append-Only.
- Los Hubs y Links ya no son Materialized Views; pasan al patrón `dp.create_streaming_table()` + `@dp.append_flow()`.
- La llave diferenciadora para Hubs son las llaves de negocio.
- La llave diferenciadora para Links es la combinación de los Hash de los dos Hubs que relaciona.

### Cambios en Plata — Lógica de detección de cambios en Satellites Estándar (Cliente, Operación)
- La llave diferenciadora de los Satellites estándar (Cliente, Operación) es la combinación `Hash_{HubAlQuePertenece}` + `Hash_Diferenciador`.
- El proceso: se toma el registro más actualizado de la Streaming Table por cada `Hash_{HubAlQuePertenece}` (ROW_NUMBER() OVER(PARTITION BY Hash_{Hub} ORDER BY FechaRegistro DESC) = 1), y luego se realiza un `LEFT JOIN` exclusivamente por `Hash_{Hub}` (`ON A.Hash_{Hub} = B.Hash_{Hub}`) seguido del filtro `WHERE (B.Hash_{Hub} IS NULL) OR (A.Hash_Diferenciador != B.Hash_Diferenciador)`. El JOIN por `Hash_{Hub}` garantiza que todos los registros de datos nuevos sean relacionados con su último estado conocido en la Streaming Table; el WHERE determina si se trata de una entidad nueva (NULL) o un cambio en atributos (Hash_Diferenciador difiere).

### Cambios en Plata — Acumulación Histórica en Satellites Transaccionales (Hub_Transaccion)
- Los Satellites del Hub_Transaccion (`Sat_Transaccion_DatosEstables`, `Sat_Transaccion_Montos`) acumulan la historia completa de TRXPFL sin aplicar ROW_NUMBER.
- La deduplicación se realiza mediante `LEFT ANTI JOIN` por la combinación `Hash_Transaccion` + `fecha_transaccion` (derivada de TRXDT).
- La columna `Hash_Diferenciador` se mantiene en la Streaming Table para trazabilidad y auditoría, pero no participa en la lógica de deduplicación.
- El procesamiento usa la nueva función `procesar_satellite_transaccional()`, diferenciada de `procesar_satellite()`.

### Actualización de Documentación y Steering
- El archivo SYSTEM.md debe ser actualizado completamente con la nueva arquitectura (sin histórico de la estrategia original).
- El Steering del cc-sdd (.kiro/steering/) debe actualizarse con trazabilidad de cambios, referenciando la arquitectura original y la nueva.
- Todos los diagramas (ASCII, Mermaid), modelos de datos, reglas y patrones de código en SYSTEM.md deben reflejar la nueva arquitectura.

## Requirements

### Requirement 1: Simplificación de la Capa de Bronce — Streaming Tables Persistentes

**Objetivo:** Como ingeniero de datos, quiero que cada fuente de Bronce tenga una única Streaming Table registrada en Unity Catalog (sin Materialized Views intermedias de snapshot), para simplificar la arquitectura y eliminar la capa redundante de snapshot más reciente.

#### Criterios de Aceptación

1. When el pipeline LSDP de Bronce se ejecuta, the Pipeline shall registrar cada Streaming Table de ingesta directamente en Unity Catalog con nombre de tres partes (`{catalogo}.{esquema}.{Origen}`), sin usar `temporary=True`.

2. When se define la Streaming Table de Bronce para cada fuente (CMSTFL, TRXPFL, BLNCFL), the Pipeline shall utilizar el decorador `@dp.table()` con `name=f"{catalogo}.{esquema}.{Origen}"` y sin el parámetro `temporary=True`.

3. When se renombran las Streaming Tables de Bronce, the Pipeline shall adoptar los nombres que antes tenían las Materialized Views de snapshot: `CMSTFL`, `TRXPFL`, `BLNCFL` (eliminando el sufijo `_temp`).

4. The Pipeline shall eliminar completamente las funciones de las Materialized Views de snapshot (`cmstfl()`, `trxpfl()`, `blncfl()`) que leían la Streaming Table temporal y filtraban por `FechaRegistroParquet` más reciente.

5. When el AutoLoader ingesta nuevos archivos Parquet, the Streaming Table shall continuar generando la columna `FechaRegistroParquet` de tipo `DATE` a partir de las columnas de partición `año`, `mes`, `dia`.

6. The Streaming Table de Bronce shall mantener la columna `_rescued_data` generada automáticamente por AutoLoader con evolución de esquema.

7. The Streaming Table de Bronce shall mantener el Liquid Clustering exclusivamente por la columna `FechaRegistroParquet`.

8. The Pipeline de Bronce shall mantener la configuración de `cloudFiles.schemaEvolutionMode = "addNewColumns"` y `cloudFiles.schemaLocation` para cada fuente.

---

### Requirement 2: Consumo desde Plata — Lectura Directa de Streaming Tables de Bronce

**Objetivo:** Como ingeniero de datos, quiero que todas las tablas de Plata consuman directamente de las Streaming Tables de Bronce registradas en Unity Catalog, para que las dependencias entre medallas sean explícitas y trazables.

#### Criterios de Aceptación

1. When una tabla de Plata necesita leer datos de Bronce, the Notebook de Plata shall referenciar la tabla con nombre completo de tres partes (`{catalogo}.{esquema}.{Origen}`) en Unity Catalog.

2. When los Hubs, Links y Satellites de Plata leen datos de Bronce, the Pipeline shall utilizar `dp.read_stream(f"{catalogo}.{esquema}.{Origen}")` como mecanismo de lectura uniforme para todas las entidades Data Vault.

3. When los Notebooks de Plata referencian datos de Bronce, the Pipeline shall utilizar exclusivamente `dp.read_stream()` referenciando la Streaming Table registrada en Unity Catalog.

4. The Pipeline de Plata shall eliminar todas las referencias al nombre temporal anterior (`CMSTFL_temp`, `TRXPFL_temp`, `BLNCFL_temp`) en cualquier llamada a `dp.read_stream()`, `dp.read()` o `spark.read.table()`.

---

### Requirement 3: Hubs como Streaming Tables con Append-Only

**Objetivo:** Como ingeniero de datos, quiero que las tablas Hub de Data Vault sean Streaming Tables con comportamiento Append-Only (en lugar de Materialized Views), para que las llaves de negocio se acumulen incrementalmente sin recalcular la tabla completa en cada ejecución.

#### Criterios de Aceptación

1. When se define una tabla Hub, the Pipeline shall utilizar el patrón `dp.create_streaming_table()` + `@dp.append_flow()` en lugar de `@dp.materialized_view()`.

2. When el `@dp.append_flow()` procesa datos para un Hub, the Pipeline shall insertar únicamente las llaves de negocio que **no existan** previamente en la Streaming Table del Hub.

3. When se determina si una llave de negocio ya existe en el Hub, the Pipeline shall usar la(s) columna(s) de llave de negocio como llave diferenciadora para la detección de duplicados (por ejemplo: `IdentificadorCliente` para Hub_Cliente, `IdentificadorCliente` + `SecuenciaSaldo` para Hub_Operacion, `IdentificadorTransaccion` (TRXID) para Hub_Transaccion).

4. When se detecta una llave de negocio que ya existe en el Hub, the Pipeline shall omitir el registro sin actualizarlo ni eliminarlo (comportamiento estrictamente Append-Only e inmutable).

5. The Streaming Table del Hub shall conservar la estructura de columnas estándar: `{Campo(s)_LlaveNegocio}`, `Hash_{NombreHub}`, `FechaRegistro`, `FuenteDatos`.

6. The Streaming Table del Hub shall mantener las expectations de calidad de datos (por ejemplo, `expect_or_drop`, `expect_or_fail`) que estaban definidas en la Materialized View original, ahora aplicadas en `dp.create_streaming_table()`.

7. The Streaming Table del Hub shall mantener el Liquid Clustering por `FechaRegistro`, `Hash_{NombreHub}` (en ese orden).

8. When la Streaming Table del Hub no existe (primera ejecución), the Pipeline shall insertar todos los registros de llaves de negocio únicas procedentes de la fuente de Bronce.

---

### Requirement 4: Links como Streaming Tables con Append-Only

**Objetivo:** Como ingeniero de datos, quiero que las tablas Link de Data Vault sean Streaming Tables con comportamiento Append-Only (en lugar de Materialized Views), para que las relaciones entre entidades se acumulen incrementalmente sin recalcular la tabla completa.

#### Criterios de Aceptación

1. When se define una tabla Link, the Pipeline shall utilizar el patrón `dp.create_streaming_table()` + `@dp.append_flow()` en lugar de `@dp.materialized_view()`.

2. When el `@dp.append_flow()` procesa datos para un Link, the Pipeline shall insertar únicamente las combinaciones de Hashes de Hubs que **no existan** previamente en la Streaming Table del Link.

3. When se determina si una relación ya existe en el Link, the Pipeline shall usar la combinación de `Hash_{NombreHub1}` + `Hash_{NombreHub2}` como llave diferenciadora para la detección de duplicados.

4. When se detecta que la combinación de hashes de los dos Hubs ya existe en el Link, the Pipeline shall omitir el registro sin actualizarlo ni eliminarlo (comportamiento estrictamente Append-Only e inmutable).

5. The Streaming Table del Link shall conservar la estructura de columnas estándar: `Hash_{NombreLink}`, `Hash_{NombreHub1}`, `Hash_{NombreHub2}`, `FechaRegistro`, `FuenteDatos`.

6. The Streaming Table del Link shall mantener el Liquid Clustering por `FechaRegistro`, `Hash_{NombreHub1}`, `Hash_{NombreHub2}` (en ese orden).

7. The Streaming Table del Link no requiere expectations de calidad de datos (`expect_or_drop`, `expect_or_fail`), dado que la integridad referencial se garantiza por la existencia previa de los Hubs referenciados.

8. When la Streaming Table del Link no existe (primera ejecución), the Pipeline shall insertar todas las combinaciones únicas de relaciones entre Hubs procedentes de la fuente de Bronce.

---

### Requirement 5: Lógica de Detección de Cambios en Satellites Estándar (Cliente, Operación) — Detección de Cambios con JOIN Exclusivo por Hash_{Hub}

**Objetivo:** Como ingeniero de datos, quiero que la detección de cambios en los Satellites estándar (vinculados a Hub_Cliente y Hub_Operacion) utilice un LEFT JOIN exclusivamente por `Hash_{HubAlQuePertenece}` y evalúe `Hash_Diferenciador` en la cláusula WHERE, para garantizar una comparación precisa que identifique tanto entidades nuevas como cambios en atributos existentes.

#### Criterios de Aceptación

1. When el `@dp.append_flow()` procesa datos para un Satellite estándar (Cliente u Operación), the Pipeline shall obtener el registro más actualizado de la Streaming Table existente por cada `Hash_{HubAlQuePertenece}` mediante la función de ventana `ROW_NUMBER() OVER(PARTITION BY Hash_{Hub} ORDER BY FechaRegistro DESC) = 1`.

2. When se comparan los datos procesados contra los datos existentes del Satellite, the Pipeline shall realizar un `LEFT JOIN` exclusivamente por `Hash_{HubAlQuePertenece}` como única condición de unión (`ON A.Hash_{Hub} = B.Hash_{Hub}`), asegurando que cada registro de datos nuevos sea relacionado con el último estado conocido de esa entidad en la Streaming Table.

3. When el `LEFT JOIN` produce un `NULL` en la columna `Hash_{HubAlQuePertenece}` del lado derecho (la Streaming Table existente), the Pipeline shall insertar el registro como nueva entidad que no existía previamente en el Satellite.

4. When el `LEFT JOIN` por `Hash_{Hub}` produce un match (la entidad existe en la Streaming Table) pero el `Hash_Diferenciador` del dato procesado difiere del `Hash_Diferenciador` del último registro existente (`A.Hash_Diferenciador != B.Hash_Diferenciador`), the Pipeline shall insertar el registro como cambio detectado en los atributos de la entidad.

5. When ambos `Hash_{HubAlQuePertenece}` y `Hash_Diferenciador` coinciden entre los datos procesados y la Streaming Table existente (match por `Hash_{Hub}` en el JOIN y coincidencia de `Hash_Diferenciador` en el filtro WHERE), the Pipeline shall omitir el registro sin insertarlo (no hay cambios).

6. The función `procesar_satellite()` en `LSDPUtilidadPrincipal.py` shall mantener la lógica de `LEFT JOIN` exclusivamente por `Hash_{Hub}` (`ON A.Hash_{Hub} = B.Hash_{Hub}`) seguida del filtro `WHERE (B.Hash_{Hub} IS NULL) OR (A.Hash_Diferenciador != B.Hash_Diferenciador)`.

7. When la Streaming Table del Satellite no existe (primera ejecución), the Pipeline shall insertar todos los registros procesados, manteniendo el comportamiento actual de fallback por `AnalysisException`.

---

### Requirement 6: Actualización Integral de SYSTEM.md

**Objetivo:** Como responsable del proyecto, quiero que el archivo SYSTEM.md refleje completamente la nueva arquitectura (Bronce simplificado + Plata con Streaming Tables uniformes), para que sirva como fuente de verdad actualizada del Spec-Driven Development.

#### Criterios de Aceptación

1. When se actualiza SYSTEM.md, the Documento shall reemplazar completamente la descripción de la Medalla de Bronce, eliminando la arquitectura de dos capas (Streaming Table temporal + Materialized View snapshot) y documentando la arquitectura de una sola Streaming Table persistente por fuente.

2. When se actualiza SYSTEM.md, the Documento shall reemplazar la estrategia de tipos de tabla LSDP en Plata para reflejar que Hubs, Links y Satellites son todos Streaming Tables con patrón `dp.create_streaming_table()` + `@dp.append_flow()`.

3. When se actualiza SYSTEM.md, the Documento shall documentar la llave diferenciadora de cada tipo de entidad Data Vault: llaves de negocio para Hubs, combinación de Hashes de Hubs para Links, y combinación `Hash_{Hub}` + `Hash_Diferenciador` para Satellites.

4. When se actualiza SYSTEM.md, the Documento shall actualizar todos los bloques de código de ejemplo (patrones de ingesta, patrones de Hub, Link, Satellite) para reflejar la nueva arquitectura.

5. When se actualiza SYSTEM.md, the Documento shall actualizar la sección de la API de decoradores LSDP para reflejar los nuevos usos: `@dp.table()` sin `temporary=True` en Bronce, y `dp.create_streaming_table()` + `@dp.append_flow()` para Hubs y Links en Plata.

6. The Documento SYSTEM.md shall **no contener** registros históricos ni comparaciones con la arquitectura anterior; todo el contenido será reemplazado por la nueva estrategia.

7. When se actualiza SYSTEM.md, the Documento shall actualizar todos los diagramas (ASCII text y Mermaid) para reflejar los nuevos flujos de datos y tipos de tabla.

---

### Requirement 7: Actualización del Steering del cc-sdd

**Objetivo:** Como responsable del proyecto, quiero que los archivos de Steering (`.kiro/steering/`) reflejen la nueva arquitectura con trazabilidad de cambios, para que el contexto del AI-DLC documenta tanto la evolución como el estado actual de la solución.

#### Criterios de Aceptación

1. When se actualiza `product.md`, the Steering shall describir las capacidades actuales del producto reflejando la nueva arquitectura de Bronce (Streaming Tables persistentes) y Plata (todas las entidades Data Vault como Streaming Tables Append-Only).

2. When se actualiza `tech.md`, the Steering shall actualizar la sección de Decisiones Técnicas Clave, incluyendo la nueva justificación para Hubs y Links como Streaming Tables en lugar de Materialized Views, y documentar la trazabilidad del cambio (estrategia original vs. nueva).

3. When se actualiza `structure.md`, the Steering shall actualizar la tabla de Objetos de Base de Datos para reflejar que: en Bronce solo hay Streaming Tables (sin MV ni tablas temporales), y en Plata los Hubs y Links son Streaming Tables (no Materialized Views).

4. When se documenta la trazabilidad en los archivos de Steering, the Steering shall incluir una sección de evolución que describa brevemente la arquitectura original y la razón del cambio a la nueva arquitectura, sin eliminar el contexto del cambio.

---

### Requirement 8: Consistencia de los Notebooks de Bronce Existentes

**Objetivo:** Como ingeniero de datos, quiero que los tres notebooks de Bronce existentes (`LSDPBronceCMSTFL`, `LSDPBronceTRXPFL`, `LSDPBronceBLNCFL`) sean refactorizados conforme a la nueva arquitectura, para que el código fuente sea coherente con la especificación.

#### Criterios de Aceptación

1. When se refactoriza cada notebook de Bronce, the Notebook shall contener una única función decorada con `@dp.table()` (sin `temporary=True`) que registre la Streaming Table directamente en Unity Catalog.

2. When se refactoriza cada notebook de Bronce, the Notebook shall eliminar la función de Materialized View de snapshot y toda la lógica de filtrado por `FechaRegistroParquet` más reciente.

3. When se refactoriza cada notebook de Bronce, the Notebook shall usar el nombre definitivo de la tabla como `name=f"{catalogo}.{esquema}.{Origen}"` (por ejemplo: `CMSTFL`, `TRXPFL`, `BLNCFL`).

4. The Notebook de Bronce refactorizado shall mantener las cabeceras descriptivas actualizándolas para reflejar que ya no hay Materialized View de snapshot.

---

### Requirement 9: Consistencia de los Notebooks de Plata Existentes (Hubs y Links)

**Objetivo:** Como ingeniero de datos, quiero que los notebooks de Hubs y Links en Plata sean refactorizados para usar el patrón Streaming Table + Append Flow, para que el código sea coherente con la nueva arquitectura Data Vault completamente basada en Streaming Tables.

#### Criterios de Aceptación

1. When se refactoriza un notebook de Hub, the Notebook shall reemplazar el decorador `@dp.materialized_view()` por el patrón `dp.create_streaming_table()` + `@dp.append_flow()`.

2. When se refactoriza un notebook de Hub, the Notebook shall implementar la lógica de detección de duplicados por llaves de negocio: leer los datos de Bronce vía `dp.read_stream()`, leer la Streaming Table existente del Hub (vía `spark.read.table()`) para obtener las llaves de negocio existentes, y filtrar para insertar solo las llaves nuevas.

3. When se refactoriza un notebook de Link, the Notebook shall reemplazar el decorador `@dp.materialized_view()` por el patrón `dp.create_streaming_table()` + `@dp.append_flow()`.

4. When se refactoriza un notebook de Link, the Notebook shall implementar la lógica de detección de duplicados por combinación de Hashes de Hubs: leer la Streaming Table existente del Link, obtener las combinaciones de hashes existentes, y filtrar para insertar solo las relaciones nuevas.

5. When los notebooks de Hub y Link leen datos de Bronce, the Notebook shall usar el nombre completo de tres partes de la Streaming Table registrada en Unity Catalog (sin referencias a tablas temporales).

---

### Requirement 10: Consistencia de los Notebooks de Plata Existentes (Satellites Estándar — Cliente, Operación)

**Objetivo:** Como ingeniero de datos, quiero que los notebooks de Satellites estándar (Cliente, Operación) en Plata sean actualizados para reflejar la nueva lógica de detección de cambios con llave diferenciadora compuesta y la lectura desde Streaming Tables de Bronce registradas. Los Satellites transaccionales (Hub_Transaccion) se rigen por el Requirement 11.

#### Criterios de Aceptación

1. When se actualiza un notebook de Satellite estándar (Cliente u Operación), the Notebook shall cambiar las llamadas `dp.read_stream("CMSTFL_temp")` y `dp.read_stream("BLNCFL_temp")` por `dp.read_stream(f"{catalogo}.{esquema}.{Origen}")` referenciando la Streaming Table registrada en Unity Catalog.

2. When se actualiza la función `procesar_satellite()`, the Función shall mantener la lógica de `LEFT JOIN` exclusivamente por `Hash_{Hub}` (`ON A.Hash_{Hub} = B.Hash_{Hub}`) y filtro `WHERE (B.Hash_{Hub} IS NULL) OR (A.Hash_Diferenciador != B.Hash_Diferenciador)` para detectar tanto entidades nuevas como cambios en atributos. Esta función aplica exclusivamente a los Satellites estándar (Cliente, Operación); los Satellites transaccionales utilizan `procesar_satellite_transaccional()` (ver Req 11).

3. The Satellite actualizado shall mantener el patrón `dp.create_streaming_table()` + `@dp.append_flow()` y las expectations de calidad de datos existentes sin modificación.

4. The Satellite actualizado shall mantener las `table_properties`, `cluster_by` y demás configuraciones existentes sin cambios.

5. The Notebooks de Satellites transaccionales (`LSDPPlataSatTransaccion.py`) shall **no** utilizar `procesar_satellite()` sino `procesar_satellite_transaccional()` conforme al Requirement 11, quedando explícitamente excluidos del alcance de este requerimiento en lo referente a la lógica de detección de cambios.

---

### Requirement 11: Acumulación Histórica Completa en Satellites del Hub_Transaccion

**Objetivo:** Como ingeniero de datos, quiero que los Satellites vinculados al Hub_Transaccion (`Sat_Transaccion_DatosEstables`, `Sat_Transaccion_Montos`) acumulen la historia completa de la Streaming Table de Bronce TRXPFL sin aplicar la regla del último registro (ROW_NUMBER), utilizando la combinación `Hash_Transaccion` + `fecha_transaccion` como llave de deduplicación, para preservar todo el historial transaccional evitando duplicados.

#### Criterios de Aceptación

1. When el `@dp.append_flow()` procesa datos para un Satellite del Hub_Transaccion, the Pipeline shall **no aplicar** la función de ventana `ROW_NUMBER() OVER(PARTITION BY Hash_Transaccion ORDER BY FechaRegistro DESC) = 1` ni ninguna otra lógica que reduzca los datos al último registro por entidad.

2. When el `@dp.append_flow()` procesa datos para un Satellite del Hub_Transaccion, the Pipeline shall acumular **todos** los registros históricos provenientes de la Streaming Table de Bronce TRXPFL, reflejando fielmente la historia completa de transacciones.

3. When se determina si un registro ya existe en un Satellite del Hub_Transaccion, the Pipeline shall utilizar la combinación de las columnas `Hash_Transaccion` + `fecha_transaccion` como llave compuesta de deduplicación para evitar la inserción de registros duplicados.

4. The Pipeline shall derivar la columna `fecha_transaccion` de tipo `DATE` a partir del campo `TRXDT` de la Streaming Table de Bronce TRXPFL (heredando el tipo de la fuente sin cast adicional si `TRXDT` ya es `DATE`), e incluirla en **todos** los Satellites del Hub_Transaccion (`Sat_Transaccion_DatosEstables` y `Sat_Transaccion_Montos`).

5. When se comparan los datos procesados contra los datos existentes del Satellite del Hub_Transaccion, the Pipeline shall realizar un `LEFT ANTI JOIN` usando `Hash_Transaccion` + `fecha_transaccion` como condiciones de unión, insertando únicamente los registros cuya combinación no exista previamente en la Streaming Table del Satellite.

6. If un registro con la misma combinación `Hash_Transaccion` + `fecha_transaccion` ya existe en el Satellite, then the Pipeline shall omitir el registro sin insertarlo (prevención de duplicados).

7. When la Streaming Table del Satellite del Hub_Transaccion no existe (primera ejecución), the Pipeline shall insertar todos los registros procesados sin aplicar deduplicación contra tabla existente, manteniendo el fallback por `AnalysisException`.

8. The nueva función `procesar_satellite_transaccional()` en `LSDPUtilidadPrincipal.py` shall implementar una lógica diferenciada respecto a `procesar_satellite()`: sin ROW_NUMBER, usando LEFT ANTI JOIN por `Hash_Transaccion` + `fecha_transaccion`.

9. The columna `Hash_Diferenciador` shall mantenerse en la Streaming Table de los Satellites transaccionales para fines de trazabilidad y auditoría, pero shall **no participar** en la lógica de deduplicación ni en el `LEFT ANTI JOIN` de `procesar_satellite_transaccional()`.

10. When se actualiza SYSTEM.md, the Documento shall documentar la excepción de los Satellites del Hub_Transaccion: acumulación histórica completa sin ROW_NUMBER, llave de deduplicación `Hash_Transaccion` + `fecha_transaccion`, la inclusión obligatoria del campo `fecha_transaccion` (derivado de TRXDT) en todos los Satellites transaccionales, y la permanencia de `Hash_Diferenciador` para trazabilidad sin participación en deduplicación.

11. When se actualizan los archivos de Steering (`.kiro/steering/`), the Steering shall documentar la regla especial de los Satellites transaccionales como parte de la evolución arquitectónica, distinguiéndola de la lógica estándar de detección de cambios con ROW_NUMBER aplicable a los Satellites de Cliente y Operación.
