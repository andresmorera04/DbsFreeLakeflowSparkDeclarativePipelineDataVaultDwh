# Plan de Implementación

- [x] 1. Crear la estructura de directorios del proyecto
- [x] 1.1 Crear los directorios base del pipeline
  - Crear `src/LSDP_Lab_DataVault_DWH/transformations/`, `src/LSDP_Lab_DataVault_DWH/utilities/` y `src/LSDP_Lab_DataVault_DWH/explorations/`
  - Cada directorio debe contener un archivo placeholder (`.gitkeep` o `__init__.py` según corresponda) para que Git lo rastree
  - El directorio `utilities/` debe incluir `__init__.py` vacío para ser importable como paquete Python
  - _Requirements: 1.1, 1.2, 1.3, 1.4_

- [x] 2. Implementar el módulo de configuración centralizada
- [x] 2.1 Implementar la función `obtener_configuracion` y las constantes de negocio
  - Crear el módulo Python puro que exponga una función que reciba `spark` como parámetro y retorne un diccionario con los 13 parámetros del pipeline leídos de `spark.conf.get()`
  - Los 13 parámetros son: `catalogo`, `esquema`, `volumen`, `catalogo_plata`, `esquema_plata`, `catalogo_oro`, `esquema_oro`, `ruta_cmstfl`, `ruta_trxpfl`, `ruta_blncfl`, `schema_location_cmstfl`, `schema_location_trxpfl`, `schema_location_blncfl`
  - Definir las constantes de negocio a nivel de módulo: tipos ATM (`DATM`, `CATM`), bits de hash (256/512), separador pipe
  - Definir los 7 diccionarios de umbrales de campos calculados con rangos numéricos como tuplas `(min, max)`
  - No incluir imports de LSDP; no incluir valores por defecto en la lectura de parámetros
  - Propagar errores nativos de `spark.conf.get()` si un parámetro no está configurado
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 10.4_

- [x] 3. Implementar el módulo de funciones helper reutilizables
- [x] 3.1 (P) Implementar las funciones de cálculo de hash y reordenamiento de columnas
  - Implementar `calcular_hash_hub`: recibe lista de columnas, bits y separador; convierte cada columna a STRING, concatena con `F.concat_ws()` si hay múltiples columnas, y aplica `F.sha2()` con los bits indicados
  - Implementar `calcular_hash_diferenciador`: recibe el hash de la entidad y campos adicionales; concatena todo con separador pipe `|` y aplica SHA2-512
  - Implementar `reordenar_columnas_lc`: recibe un DataFrame y lista de columnas LC; retorna el DataFrame con las columnas LC al inicio seguidas del resto en orden original
  - Usar exclusivamente funciones nativas de `pyspark.sql.functions`, nunca UDFs
  - Importar constantes de hash (`HASH_HUB_LINK_BITS`, `HASH_SATELLITE_BITS`, `HASH_SEPARATOR`) desde el módulo de configuración
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 7.3, 7.5, 7.6_

- [x] 4. Implementar el notebook de ingesta Bronce para CMSTFL
- [x] 4.1 Implementar la Streaming Table temporal y la Materialized View de CMSTFL
  - Importar `obtener_configuracion` desde el módulo de configuración e invocarla pasando `spark` para obtener el diccionario de parámetros
  - Definir una Streaming Table temporal con AutoLoader que lea Parquets desde `ruta_cmstfl`, con schema location en `schema_location_cmstfl`, inferencia de tipos habilitada y evolución de esquema `addNewColumns`
  - Generar la columna derivada `FechaRegistroParquet` (DATE) a partir de las columnas de partición `año`, `mes`, `dia` inferidas por AutoLoader
  - Usar `FechaRegistroParquet` como única clave de Liquid Clustering en la ST temporal
  - Definir una Materialized View registrada en Unity Catalog con nombre de 3 partes, que lea la ST temporal, calcule la fecha máxima de `FechaRegistroParquet`, y filtre solo los registros de esa fecha usando broadcast join
  - Incluir `_rescued_data` en ambas tablas. Usar `from pyspark import pipelines as dp` y nombre de 3 partes en `name=`
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 7.7, 7.8, 8.1, 8.2, 8.3, 8.5, 8.6, 10.1, 10.2, 10.3_

- [x] 5. Implementar el notebook de ingesta Bronce para TRXPFL
- [x] 5.1 (P) Implementar la Streaming Table temporal y la Materialized View de TRXPFL
  - Seguir el mismo patrón de ingesta que CMSTFL: importar configuración, definir ST temporal con AutoLoader, generar `FechaRegistroParquet`, definir MV de snapshot con broadcast join
  - Usar `ruta_trxpfl` como ruta de datos y `schema_location_trxpfl` como schema location, ambos del diccionario de configuración
  - Usar `TRXPFL_temp` como nombre de la ST temporal y `{catalogo}.{esquema}.TRXPFL` como nombre de 3 partes de la MV
  - Mantener consistencia exacta de patrón: mismas opciones de AutoLoader, misma lógica de snapshot, misma estructura de imports
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 7.7, 7.8, 8.1, 8.2, 8.3, 8.5, 8.6, 10.1, 10.2, 10.3_

- [x] 6. Implementar el notebook de ingesta Bronce para BLNCFL
- [x] 6.1 (P) Implementar la Streaming Table temporal y la Materialized View de BLNCFL
  - Seguir el mismo patrón de ingesta que CMSTFL: importar configuración, definir ST temporal con AutoLoader, generar `FechaRegistroParquet`, definir MV de snapshot con broadcast join
  - Usar `ruta_blncfl` como ruta de datos y `schema_location_blncfl` como schema location, ambos del diccionario de configuración
  - Usar `BLNCFL_temp` como nombre de la ST temporal y `{catalogo}.{esquema}.BLNCFL` como nombre de 3 partes de la MV
  - Mantener consistencia exacta de patrón con CMSTFL y TRXPFL
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 7.7, 7.8, 8.1, 8.2, 8.3, 8.5, 8.6, 10.1, 10.2, 10.3_

- [x] 7. Importar los notebooks generadores de Parquets al repositorio
- [x] 7.1 (P) Copiar los notebooks existentes al directorio de explorations
  - Importar los notebooks generadores de Parquets ya desarrollados al directorio `explorations/` sin modificar su código fuente
  - Verificar que no forman parte del pipeline LSDP de producción (no son invocados desde `transformations/`)
  - Confirmar que generan Parquets en la estructura de particionamiento `año=YYYY/mes=MM/dia=DD/`
  - _Requirements: 9.1, 9.2, 9.3, 9.4_

- [x] 8. Validar compatibilidad Serverless y consistencia entre fuentes
- [x] 8.1 Verificar que todo el código cumple las restricciones de Serverless y es consistente
  - Revisar que ningún archivo use `.cache()`, `.persist()`, `sparkContext`, `sc.`, operaciones RDD, UDFs, threading ni multiprocessing
  - Verificar que todos los hashes usen `F.sha2()` con `.cast("string")` previo, y que cualquier uso de `F.hash()` aplique `.cast("long")` antes de `F.abs()`
  - Confirmar que la concatenación de strings use `F.concat()` o `F.concat_ws()`, nunca operador `+`
  - Verificar import correcto de LSDP (`from pyspark import pipelines as dp`) y nombre de 3 partes en MV
  - Confirmar que las columnas `año`, `mes`, `dia`, `FechaRegistroParquet` y `_rescued_data` son exclusivas de Bronce
  - Verificar que los 3 notebooks siguen el mismo patrón idéntico: 2 tablas LSDP cada uno (ST temporal + MV snapshot), misma lógica de snapshot, mismas opciones de AutoLoader
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_
