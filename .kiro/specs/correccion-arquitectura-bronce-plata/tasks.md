# Plan de Implementación

- [x] 1. Funciones helper de deduplicación para Hubs y Links
- [x] 1.1 (P) Implementar la función de detección de duplicados por llaves de negocio para Hubs
  - Crear la función que recibe el SparkSession, los identificadores del catálogo/esquema/tabla, la lista de columnas de llave y el DataFrame de datos nuevos
  - Leer la tabla existente del Hub para obtener las llaves registradas, ejecutar un LEFT ANTI JOIN por las columnas de llave, y retornar solo los registros con llaves nuevas
  - Manejar la primera ejecución (tabla inexistente) retornando todos los registros mediante fallback por AnalysisException
  - _Requirements: 3.2, 3.3, 3.4, 3.8_

- [x] 1.2 (P) Implementar la función de detección de duplicados por combinación de hashes para Links
  - Crear la función que recibe el SparkSession, los identificadores del catálogo/esquema/tabla, la lista de columnas de hash y el DataFrame de datos nuevos
  - Leer la tabla existente del Link para obtener las combinaciones de hashes registradas, ejecutar un LEFT ANTI JOIN por las columnas de hash de los dos Hubs, y retornar solo las combinaciones nuevas
  - Manejar la primera ejecución (tabla inexistente) retornando todos los registros mediante fallback por AnalysisException
  - _Requirements: 4.2, 4.3, 4.4, 4.8_

- [x] 2. Función de acumulación histórica para Satellites transaccionales
- [x] 2.1 Implementar la función de deduplicación histórica por hash + fecha para Satellites transaccionales
  - Crear la función que recibe el SparkSession, los identificadores del catálogo/esquema/tabla, la columna de hash, la columna de fecha y el DataFrame de datos nuevos
  - Leer la tabla existente del Satellite, ejecutar un LEFT ANTI JOIN por la combinación de hash + fecha, y retornar todos los registros cuya combinación no exista previamente — sin aplicar ROW_NUMBER ni ninguna reducción al último registro
  - Preservar la columna Hash_Diferenciador en el DataFrame de salida sin usarla para la deduplicación
  - Manejar la primera ejecución (tabla inexistente) retornando todos los registros mediante fallback por AnalysisException
  - _Requirements: 11.3, 11.5, 11.6, 11.7, 11.8, 11.9_

- [x] 3. Simplificación de los notebooks de Bronce
- [x] 3.1 (P) Refactorizar el notebook de ingesta de Maestro de Clientes (CMSTFL)
  - Reemplazar las dos funciones existentes (ST temporal + MV snapshot) por una única función decorada que registre la Streaming Table directamente en Unity Catalog con nombre de tres partes
  - Eliminar el parámetro temporary=True, adoptar el nombre definitivo CMSTFL, y eliminar toda la lógica de filtrado por FechaRegistroParquet más reciente
  - Mantener AutoLoader con schema evolution, generación de FechaRegistroParquet, _rescued_data, Liquid Clustering por FechaRegistroParquet, y cabeceras actualizadas
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 8.1, 8.2, 8.3, 8.4_

- [x] 3.2 (P) Refactorizar el notebook de ingesta de Transacciones (TRXPFL)
  - Aplicar la misma simplificación: una única función decorada con nombre definitivo TRXPFL registrada en Unity Catalog, eliminando la MV de snapshot
  - Mantener AutoLoader, generación de FechaRegistroParquet, _rescued_data, Liquid Clustering y schema evolution
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 8.1, 8.2, 8.3, 8.4_

- [x] 3.3 (P) Refactorizar el notebook de ingesta de Saldos (BLNCFL)
  - Aplicar la misma simplificación: una única función decorada con nombre definitivo BLNCFL registrada en Unity Catalog, eliminando la MV de snapshot
  - Mantener AutoLoader, generación de FechaRegistroParquet, _rescued_data, Liquid Clustering y schema evolution
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 8.1, 8.2, 8.3, 8.4_

- [x] 4. Migración de notebooks de Hubs a Streaming Tables Append-Only
- [x] 4.1 (P) Migrar el notebook de Hub_Cliente al patrón Streaming Table + Append Flow
  - Reemplazar el decorador de vista materializada por la creación programática de la Streaming Table con expectations y Liquid Clustering, más un flujo de append
  - Leer la fuente de Bronce CMSTFL vía dp.read_stream con nombre de tres partes en UC, calcular el hash y seleccionar las columnas estándar del Hub
  - Invocar la función de detección de duplicados por llave de negocio (IdentificadorCliente) y reordenar columnas para Liquid Clustering
  - _Requirements: 2.1, 2.2, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 9.1, 9.2, 9.5_

- [x] 4.2 (P) Migrar el notebook de Hub_Operacion al patrón Streaming Table + Append Flow
  - Reemplazar el decorador de vista materializada por la creación programática de la Streaming Table con expectations y Liquid Clustering, más un flujo de append
  - Leer la fuente de Bronce BLNCFL vía dp.read_stream con nombre de tres partes en UC, calcular el hash y seleccionar las columnas estándar del Hub
  - Invocar la función de detección de duplicados por llaves de negocio compuestas (IdentificadorCliente + SecuenciaSaldo) y reordenar columnas para Liquid Clustering
  - _Requirements: 2.1, 2.2, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 9.1, 9.2, 9.5_

- [x] 4.3 (P) Migrar el notebook de Hub_Transaccion al patrón Streaming Table + Append Flow
  - Reemplazar el decorador de vista materializada por la creación programática de la Streaming Table con expectations y Liquid Clustering, más un flujo de append
  - Leer la fuente de Bronce TRXPFL vía dp.read_stream con nombre de tres partes en UC, calcular el hash y seleccionar las columnas estándar del Hub
  - Invocar la función de detección de duplicados por llave de negocio (IdentificadorTransaccion) y reordenar columnas para Liquid Clustering
  - _Requirements: 2.1, 2.2, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 9.1, 9.2, 9.5_

- [x] 5. Migración de notebooks de Links a Streaming Tables Append-Only
- [x] 5.1 (P) Migrar el notebook de Link_Cliente_Operacion al patrón Streaming Table + Append Flow
  - Reemplazar el decorador de vista materializada por la creación programática de la Streaming Table sin expectations y con Liquid Clustering, más un flujo de append
  - Leer la fuente de Bronce BLNCFL vía dp.read_stream con nombre de tres partes en UC, calcular los hashes de los dos Hubs (Hash_Cliente y Hash_Operacion) y el hash del Link
  - Invocar la función de detección de duplicados por combinación de hashes y reordenar columnas para Liquid Clustering
  - _Requirements: 2.1, 2.2, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 9.3, 9.4, 9.5_

- [x] 5.2 (P) Migrar el notebook de Link_Cliente_Transaccion al patrón Streaming Table + Append Flow
  - Reemplazar el decorador de vista materializada por la creación programática de la Streaming Table sin expectations y con Liquid Clustering, más un flujo de append
  - Leer la fuente de Bronce TRXPFL vía dp.read_stream con nombre de tres partes en UC, calcular los hashes de los dos Hubs (Hash_Cliente y Hash_Transaccion) y el hash del Link
  - Invocar la función de detección de duplicados por combinación de hashes y reordenar columnas para Liquid Clustering
  - _Requirements: 2.1, 2.2, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 9.3, 9.4, 9.5_

- [x] 6. Actualización de notebooks de Satellites estándar (Cliente, Operación)
- [x] 6.1 (P) Actualizar las referencias de lectura de Bronce en el notebook de Satellites de Cliente
  - Cambiar las llamadas de lectura de Bronce para usar el nombre de tres partes en Unity Catalog con dp.read_stream en lugar de la referencia temporal
  - Eliminar cualquier referencia al nombre temporal anterior (CMSTFL_temp) en el notebook
  - Mantener sin cambios la definición de Streaming Tables, los decoradores de append flow, expectations, table_properties y cluster_by
  - Verificar que la lógica de detección de cambios sigue delegándose a la función existente sin modificación
  - _Requirements: 2.1, 2.3, 2.4, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 10.1, 10.2, 10.3, 10.4_

- [x] 6.2 (P) Actualizar las referencias de lectura de Bronce en el notebook de Satellites de Operación
  - Cambiar las llamadas de lectura de Bronce para usar el nombre de tres partes en Unity Catalog con dp.read_stream en lugar de la referencia temporal
  - Eliminar cualquier referencia al nombre temporal anterior (BLNCFL_temp) en el notebook
  - Mantener sin cambios la definición de Streaming Tables, los decoradores de append flow, expectations, table_properties y cluster_by
  - Verificar que la lógica de detección de cambios sigue delegándose a la función existente sin modificación
  - _Requirements: 2.1, 2.3, 2.4, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 10.1, 10.2, 10.3, 10.4_

- [x] 7. Actualización del notebook de Satellites transaccionales (Hub_Transaccion)
- [x] 7.1 Actualizar las referencias de lectura y la lógica de procesamiento en el notebook de Satellites de Transacción
  - Cambiar las llamadas de lectura de Bronce para usar el nombre de tres partes en Unity Catalog con dp.read_stream en lugar de la referencia temporal (TRXPFL_temp)
  - Reemplazar la invocación de la función de detección de cambios estándar por la nueva función de acumulación histórica, pasando la columna de hash y la columna de fecha como parámetros
  - Añadir la columna fecha_transaccion de tipo DATE (derivada de TRXDT) tanto en Sat_Transaccion_DatosEstables como en Sat_Transaccion_Montos
  - Mantener la columna Hash_Diferenciador para trazabilidad sin participación en deduplicación
  - Mantener sin cambios las definiciones de Streaming Tables, expectations, table_properties y cluster_by
  - _Requirements: 2.1, 2.3, 10.5, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8, 11.9_

- [x] 8. Actualización integral de SYSTEM.md
- [x] 8.1 Reescribir las secciones de arquitectura de Bronce y Plata en SYSTEM.md
  - Reemplazar la descripción de Bronce: eliminar la arquitectura de dos capas (ST temporal + MV snapshot) y documentar la arquitectura de una sola Streaming Table persistente por fuente
  - Reemplazar la estrategia de tipos de tabla en Plata: documentar que Hubs, Links y Satellites son todos Streaming Tables con patrón de creación programática + append flow
  - Documentar la llave diferenciadora de cada tipo de entidad: llaves de negocio para Hubs, combinación de hashes para Links, Hash_Hub + Hash_Diferenciador para Satellites estándar, Hash_Transaccion + fecha_transaccion para Satellites transaccionales
  - Actualizar la sección de API de decoradores LSDP para reflejar los nuevos usos
  - _Requirements: 6.1, 6.2, 6.3, 6.5_

- [x] 8.2 Actualizar bloques de código y diagramas en SYSTEM.md
  - Actualizar todos los bloques de código de ejemplo: patrón de ingesta Bronce, patrón de Hub, patrón de Link, patrón de Satellite estándar, patrón de Satellite transaccional
  - Actualizar todos los diagramas (ASCII text y Mermaid) para reflejar los nuevos flujos de datos y tipos de tabla
  - Documentar la excepción de los Satellites transaccionales: acumulación histórica sin ROW_NUMBER, llave Hash_Transaccion + fecha_transaccion, y permanencia de Hash_Diferenciador para trazabilidad
  - Garantizar que no queden registros históricos ni comparaciones con la arquitectura anterior
  - _Requirements: 6.4, 6.6, 6.7, 11.10_

- [x] 9. Actualización del Steering con trazabilidad de evolución
- [x] 9.1 (P) Actualizar product.md con las capacidades de la nueva arquitectura
  - Actualizar las capacidades principales para reflejar Bronce con Streaming Tables persistentes y Plata con todas las entidades Data Vault como Streaming Tables Append-Only
  - Mantener el enfoque en propósito y patrones, no en listas exhaustivas
  - _Requirements: 7.1_

- [x] 9.2 (P) Actualizar tech.md con decisiones técnicas y trazabilidad
  - Actualizar la sección de Decisiones Técnicas Clave: nueva justificación para Hubs y Links como Streaming Tables en lugar de Materialized Views
  - Incluir una sección de evolución que describa brevemente la arquitectura original y la razón del cambio, con trazabilidad del cambio
  - Documentar la regla especial de los Satellites transaccionales como parte de la evolución
  - _Requirements: 7.2, 7.4, 11.11_

- [x] 9.3 (P) Actualizar structure.md con la nueva tabla de objetos de base de datos
  - Actualizar la tabla de Objetos de Base de Datos: en Bronce solo Streaming Tables (sin MV ni tablas temporales), en Plata los Hubs y Links son Streaming Tables (no Materialized Views)
  - Incluir una sección de evolución que documente brevemente la arquitectura original vs. la nueva
  - _Requirements: 7.3, 7.4_

- [x] 10. Tests unitarios de las funciones helper
- [x] 10.1 (P) Implementar tests para la función de detección de duplicados de Hubs
  - Validar que retorna solo llaves nuevas cuando la tabla existe con llaves previas
  - Validar el fallback de primera ejecución (tabla inexistente) retornando todos los registros
  - Validar que el esquema del DataFrame resultado es idéntico al de entrada
  - _Requirements: 3.2, 3.3, 3.4, 3.8_

- [x] 10.2 (P) Implementar tests para la función de detección de duplicados de Links
  - Validar que retorna solo combinaciones nuevas de hashes cuando la tabla existe
  - Validar el fallback de primera ejecución retornando todos los registros
  - _Requirements: 4.2, 4.3, 4.4, 4.8_

- [x] 10.3 (P) Implementar tests para la función de acumulación histórica transaccional
  - Validar que retorna solo registros cuya combinación hash + fecha no existe en la tabla
  - Validar que no aplica ROW_NUMBER ni reduce al último registro
  - Validar que Hash_Diferenciador se preserva en el resultado sin participar en la deduplicación
  - Validar el fallback de primera ejecución retornando todos los registros
  - _Requirements: 11.3, 11.5, 11.6, 11.8, 11.9_
