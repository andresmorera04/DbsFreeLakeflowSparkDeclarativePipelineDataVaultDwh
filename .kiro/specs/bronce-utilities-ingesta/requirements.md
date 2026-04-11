# Documento de Requisitos — bronce-utilities-ingesta

## Introducción

Este documento define los requisitos para el primer incremento del pipeline LSDP del Data Warehouse sobre Databricks Free Edition Serverless. El alcance cubre dos áreas funcionales:

1. **Utilities**: Módulos Python puro con configuración centralizada (parámetros, constantes de negocio, funciones helper) que serán consumidos por todas las medallas del pipeline.
2. **Medalla de Bronce**: Notebooks LSDP que implementan la ingesta incremental de 3 fuentes Parquet AS400 (CMSTFL, TRXPFL, BLNCFL) mediante Streaming Tables temporales con AutoLoader y Materialized Views de snapshot más reciente.
3. **Notebooks de exploración (importación)**: Los notebooks generadores de archivos Parquet para la Landing Zone ya están desarrollados externamente y serán importados al repositorio sin modificaciones, únicamente para trazabilidad.

Todo el código debe cumplir al 100% las restricciones de Databricks Free Edition Serverless Compute.

---

## Requisitos

### Requisito 1: Estructura de Directorios del Proyecto

**Objetivo:** Como ingeniero de datos, quiero que el repositorio tenga la estructura de carpetas estándar del proyecto, para que cada artefacto resida en su ubicación convencional y el pipeline LSDP localice los notebooks correctamente.

#### Criterios de Aceptación

1. The Pipeline LSDP shall crear los directorios `src/LSDP_Lab_DataVault_DWH/transformations/`, `src/LSDP_Lab_DataVault_DWH/utilities/` y `src/LSDP_Lab_DataVault_DWH/explorations/` siguiendo la convención definida en el steering `structure.md`.
2. When se cree un archivo en `utilities/`, the Pipeline LSDP shall nombrarlo con el patrón `LSDP{NombreUtilidad}.py`.
3. When se cree un notebook de transformación de Bronce, the Pipeline LSDP shall nombrarlo con el patrón `LSDPBronce{NombreOrigen}` (ejemplo: `LSDPBronceCMSTFL`).
4. The Pipeline LSDP shall incluir el directorio `explorations/` como ubicación para los notebooks generadores de Parquets importados.

---

### Requisito 2: Módulo de Configuración Centralizada

**Objetivo:** Como ingeniero de datos, quiero un módulo Python puro externo al source_code LSDP que centralice todos los parámetros del pipeline, constantes de negocio y rutas de datos, para que ningún notebook de transformación contenga valores hard-coded.

#### Criterios de Aceptación

1. The módulo `LSDPConfiguracion.py` shall exponer una función `obtener_configuracion(spark)` que reciba el objeto `spark` como parámetro (ya que el módulo no es source_code LSDP y no tiene acceso a la variable global `spark` del runtime) y retorne un diccionario con los 13 parámetros del pipeline leídos mediante `spark.conf.get()`: `pipeline.catalogo`, `pipeline.esquema`, `pipeline.volumen`, `pipeline.catalogo_plata`, `pipeline.esquema_plata`, `pipeline.catalogo_oro`, `pipeline.esquema_oro`, `pipeline.ruta_cmstfl`, `pipeline.ruta_trxpfl`, `pipeline.ruta_blncfl`, `pipeline.schema_location_cmstfl`, `pipeline.schema_location_trxpfl` y `pipeline.schema_location_blncfl`.
2. The módulo `LSDPConfiguracion.py` shall exponer las rutas de datos y schema locations como parámetros específicos por fuente dentro del diccionario retornado: `ruta_cmstfl`, `ruta_trxpfl`, `ruta_blncfl` (Landing Zone) y `schema_location_cmstfl`, `schema_location_trxpfl`, `schema_location_blncfl` (checkpoints de AutoLoader), sin derivar rutas a partir de un parámetro base.
3. The módulo `LSDPConfiguracion.py` shall definir las constantes de negocio como variables Python inmutables a nivel de módulo (no dependen de `spark`): `TIPO_DATM`, `TIPO_CATM`, `TIPOS_ATM`, `HASH_HUB_LINK_BITS` (256), `HASH_SATELLITE_BITS` (512), `HASH_SEPARATOR` (`"|"`).
4. The módulo `LSDPConfiguracion.py` shall definir los diccionarios de umbrales de campos calculados a nivel de módulo: `UMBRAL_RANGO_ETARIO`, `UMBRAL_CATEGORIA_INGRESOS`, `UMBRAL_CATEGORIA_SALDO`, `UMBRAL_UTILIZACION_CREDITO`, `UMBRAL_SOBREGIRO`, `UMBRAL_RANGO_MONTO`, `UMBRAL_RIESGO_FRAUDE`, cada uno con sus rangos numéricos como tuplas `(min, max)`.
5. The módulo `LSDPConfiguracion.py` shall no contener ninguna sentencia de import de LSDP (`from pyspark import pipelines as dp`) ya que es Python puro, no un notebook del pipeline. No forma parte del source_code del pipeline LSDP — es un módulo externo importado por los notebooks.

---

### Requisito 3: Funciones Helper Reutilizables

**Objetivo:** Como ingeniero de datos, quiero funciones helper centralizadas para cálculos repetitivos (hashes, reordenamiento de columnas), para que las transformaciones de Plata y Oro las reutilicen sin duplicar lógica.

#### Criterios de Aceptación

1. The módulo de utilidades shall exponer una función `calcular_hash_hub(columnas, bits, separador)` que: (a) si recibe una sola columna, la convierta a STRING y aplique `F.sha2()` con los bits indicados; (b) si recibe múltiples columnas, las convierta a STRING, las concatene con el separador y aplique `F.sha2()` sobre la cadena resultante.
2. The módulo de utilidades shall exponer una función `calcular_hash_diferenciador(hash_entidad, *campos)` que concatene el hash de la entidad con todos los campos (convertidos a STRING) usando el separador pipe `|` y aplique `F.sha2()` con 512 bits.
3. The módulo de utilidades shall exponer una función `reordenar_columnas_lc(df, columnas_lc)` que reordene las columnas del DataFrame colocando las columnas de Liquid Clustering en las primeras posiciones, seguidas del resto en su orden original.
4. If la función `calcular_hash_hub` recibe una columna no-STRING, the función shall aplicar `.cast("string")` antes del cálculo del hash para evitar errores de tipo.
5. The módulo de utilidades shall usar exclusivamente funciones nativas de `pyspark.sql.functions` (nunca UDFs) en todas las funciones helper.

---

### Requisito 4: Notebook de Ingesta Bronce — CMSTFL (Maestro de Clientes)

**Objetivo:** Como ingeniero de datos, quiero un notebook LSDP que ingeste incrementalmente los archivos Parquet del Maestro de Clientes (CMSTFL, 4,000,000 registros) y exponga un snapshot con la fecha más reciente, para que la medalla de Plata consuma datos actualizados sin filtrar por fecha.

#### Criterios de Aceptación

1. The notebook `LSDPBronceCMSTFL` shall definir una Streaming Table temporal (`temporary=True`) con nombre `CMSTFL_temp` que use AutoLoader (`cloudFiles`) con formato `parquet`, `cloudFiles.inferColumnTypes = true`, `cloudFiles.schemaEvolutionMode = addNewColumns` y `cloudFiles.schemaLocation` apuntando a `schema_location_cmstfl` (parámetro específico obtenido del diccionario de configuración).
2. The notebook `LSDPBronceCMSTFL` shall cargar los datos desde la ruta `ruta_cmstfl` proporcionada por el diccionario retornado por `obtener_configuracion(spark)`, sin incluir subcarpetas de partición (`año=/mes=/dia=`) en la ruta para permitir la inferencia automática por lazy evaluation de Spark.
3. When AutoLoader infiera las columnas de partición `año`, `mes` y `dia`, the Streaming Table shall generar una columna derivada `FechaRegistroParquet` de tipo `DATE` mediante `F.to_date(F.concat_ws("-", F.col("año"), F.col("mes"), F.col("dia")))`.
4. The Streaming Table temporal `CMSTFL_temp` shall usar exclusivamente `FechaRegistroParquet` como clave de Liquid Clustering, sin columnas adicionales.
5. The notebook `LSDPBronceCMSTFL` shall definir una Materialized View registrada en Unity Catalog con nombre de 3 partes `{catalogo}.{esquema}.CMSTFL`, con `cluster_by=["FechaRegistroParquet"]`.
6. The Materialized View `CMSTFL` shall leer de `CMSTFL_temp`, calcular la fecha máxima de `FechaRegistroParquet`, y retornar únicamente los registros cuyo `FechaRegistroParquet` coincida con dicha fecha máxima, utilizando `F.broadcast()` para el join con el DataFrame de la fecha máxima.
7. The notebook `LSDPBronceCMSTFL` shall incluir la columna `_rescued_data` generada automáticamente por AutoLoader en ambas tablas (Streaming Table y Materialized View).

---

### Requisito 5: Notebook de Ingesta Bronce — TRXPFL (Transacciones)

**Objetivo:** Como ingeniero de datos, quiero un notebook LSDP que ingeste incrementalmente los archivos Parquet Transaccionales (TRXPFL, 7,000,000 registros) y exponga un snapshot con la fecha más reciente, para que la medalla de Plata consuma datos actualizados de transacciones.

#### Criterios de Aceptación

1. The notebook `LSDPBronceTRXPFL` shall definir una Streaming Table temporal (`temporary=True`) con nombre `TRXPFL_temp` que use AutoLoader (`cloudFiles`) con formato `parquet`, `cloudFiles.inferColumnTypes = true`, `cloudFiles.schemaEvolutionMode = addNewColumns` y `cloudFiles.schemaLocation` apuntando a `schema_location_trxpfl` (parámetro específico obtenido del diccionario de configuración).
2. The notebook `LSDPBronceTRXPFL` shall cargar los datos desde la ruta `ruta_trxpfl` proporcionada por el diccionario retornado por `obtener_configuracion(spark)`.
3. When AutoLoader infiera las columnas de partición `año`, `mes` y `dia`, the Streaming Table shall generar una columna derivada `FechaRegistroParquet` de tipo `DATE` mediante `F.to_date(F.concat_ws("-", F.col("año"), F.col("mes"), F.col("dia")))`.
4. The Streaming Table temporal `TRXPFL_temp` shall usar exclusivamente `FechaRegistroParquet` como clave de Liquid Clustering.
5. The notebook `LSDPBronceTRXPFL` shall definir una Materialized View registrada en Unity Catalog con nombre de 3 partes `{catalogo}.{esquema}.TRXPFL`, con `cluster_by=["FechaRegistroParquet"]`.
6. The Materialized View `TRXPFL` shall leer de `TRXPFL_temp`, calcular la fecha máxima y retornar únicamente los registros del snapshot más reciente, utilizando `F.broadcast()` para el join.
7. The notebook `LSDPBronceTRXPFL` shall incluir la columna `_rescued_data` en ambas tablas.

---

### Requisito 6: Notebook de Ingesta Bronce — BLNCFL (Saldos/Operaciones)

**Objetivo:** Como ingeniero de datos, quiero un notebook LSDP que ingeste incrementalmente los archivos Parquet de Saldos (BLNCFL, 4,000,000 registros) y exponga un snapshot con la fecha más reciente, para que la medalla de Plata consuma datos actualizados de saldos y operaciones.

#### Criterios de Aceptación

1. The notebook `LSDPBronceBLNCFL` shall definir una Streaming Table temporal (`temporary=True`) con nombre `BLNCFL_temp` que use AutoLoader (`cloudFiles`) con formato `parquet`, `cloudFiles.inferColumnTypes = true`, `cloudFiles.schemaEvolutionMode = addNewColumns` y `cloudFiles.schemaLocation` apuntando a `schema_location_blncfl` (parámetro específico obtenido del diccionario de configuración).
2. The notebook `LSDPBronceBLNCFL` shall cargar los datos desde la ruta `ruta_blncfl` proporcionada por el diccionario retornado por `obtener_configuracion(spark)`.
3. When AutoLoader infiera las columnas de partición `año`, `mes` y `dia`, the Streaming Table shall generar una columna derivada `FechaRegistroParquet` de tipo `DATE` mediante `F.to_date(F.concat_ws("-", F.col("año"), F.col("mes"), F.col("dia")))`.
4. The Streaming Table temporal `BLNCFL_temp` shall usar exclusivamente `FechaRegistroParquet` como clave de Liquid Clustering.
5. The notebook `LSDPBronceBLNCFL` shall definir una Materialized View registrada en Unity Catalog con nombre de 3 partes `{catalogo}.{esquema}.BLNCFL`, con `cluster_by=["FechaRegistroParquet"]`.
6. The Materialized View `BLNCFL` shall leer de `BLNCFL_temp`, calcular la fecha máxima y retornar únicamente los registros del snapshot más reciente, utilizando `F.broadcast()` para el join.
7. The notebook `LSDPBronceBLNCFL` shall incluir la columna `_rescued_data` en ambas tablas.

---

### Requisito 7: Compatibilidad Serverless

**Objetivo:** Como ingeniero de datos, quiero que todo el código generado sea 100% compatible con Databricks Free Edition Serverless Compute, para que el pipeline ejecute sin errores de runtime.

#### Criterios de Aceptación

1. The Pipeline LSDP shall no utilizar `.cache()` ni `.persist()` en ningún notebook ni módulo de utilidades.
2. The Pipeline LSDP shall no referenciar `spark.sparkContext`, `sc.`, ni operaciones RDD (`.rdd`, `.parallelize()`, `.mapPartitions()`, `.foreachPartition()`, `.toLocalIterator()`).
3. The Pipeline LSDP shall no utilizar UDFs (User Defined Functions); todas las transformaciones deben usar funciones nativas de `pyspark.sql.functions`.
4. The Pipeline LSDP shall no utilizar threading ni multiprocessing.
5. If se necesita un hash con `F.hash()`, the Pipeline LSDP shall aplicar `.cast("long")` antes de `F.abs()` para evitar overflow ANSI con `Integer.MIN_VALUE`.
6. The Pipeline LSDP shall usar `F.concat()` o `F.concat_ws()` para concatenar strings, nunca el operador `+` en columnas (que es suma aritmética en ANSI mode).
7. The Pipeline LSDP shall usar `from pyspark import pipelines as dp` como import de LSDP; nunca `import databricks.sdk.pipelines as dp`.
8. When se defina una Materialized View, the Pipeline LSDP shall pasar el nombre completo de 3 partes en el parámetro `name=`; nunca usar `catalog=` ni `schema=` como kwargs separados.

---

### Requisito 8: Patrón de Ingesta Consistente entre Fuentes

**Objetivo:** Como ingeniero de datos, quiero que las 3 fuentes de Bronce sigan un patrón de ingesta idéntico (Streaming Table temporal → Materialized View snapshot), para mantener consistencia arquitectónica y facilitar el mantenimiento.

#### Criterios de Aceptación

1. The Pipeline LSDP shall implementar exactamente 2 tablas LSDP por cada fuente de origen: una Streaming Table temporal (Capa 1) y una Materialized View de snapshot (Capa 2).
2. While se ejecute el pipeline, the Streaming Table temporal shall acumular historia incremental de todos los archivos Parquet procesados por AutoLoader, sin eliminar registros previos.
3. While se ejecute el pipeline, the Materialized View de snapshot shall contener únicamente los registros correspondientes a la `FechaRegistroParquet` más reciente, recalculándose completamente en cada ejecución.
4. The Pipeline LSDP shall garantizar que las columnas `año`, `mes`, `dia`, `FechaRegistroParquet` y `_rescued_data` sean exclusivas de Bronce y no se propaguen a medallas superiores.
5. When se defina la Streaming Table temporal, the Pipeline LSDP shall usar el parámetro `temporary=True` para que la tabla no se registre en Unity Catalog.
6. When se defina la Materialized View, the Pipeline LSDP shall registrarla en Unity Catalog con nombre de 3 partes (`{catalogo}.{esquema}.{nombre_origen}`) para que sea accesible por las medallas de Plata y Oro.

---

### Requisito 9: Importación de Notebooks Generadores de Parquets

**Objetivo:** Como ingeniero de datos, quiero importar los notebooks generadores de archivos Parquet ya desarrollados al directorio `explorations/` del proyecto, para que la trazabilidad del proyecto contemple estos artefactos como parte del repositorio.

#### Criterios de Aceptación

1. The repositorio shall contener el directorio `src/LSDP_Lab_DataVault_DWH/explorations/` como ubicación estándar para los notebooks importados.
2. The notebooks generadores de Parquets shall importarse sin modificaciones al código fuente, preservando su funcionalidad original.
3. The notebooks importados shall no formar parte del pipeline LSDP de producción (no son invocados por los notebooks de `transformations/`).
4. The notebooks importados shall ser capaces de generar archivos Parquet en la estructura de particionamiento `año=YYYY/mes=MM/dia=DD/` dentro de la ruta de Landing Zone configurada en los parámetros del pipeline.

---

### Requisito 10: Imports y Dependencias entre Notebooks

**Objetivo:** Como ingeniero de datos, quiero que los notebooks de Bronce importen correctamente el módulo de configuración centralizada, para que los parámetros, constantes y funciones helper estén disponibles sin duplicación de código.

#### Criterios de Aceptación

1. When se ejecute un notebook de Bronce en el pipeline LSDP, the notebook shall importar la función `obtener_configuracion` desde `utilities.LSDPConfiguracion`, invocarla pasando `spark` como parámetro (`config = obtener_configuracion(spark)`), y acceder a los valores del diccionario retornado (`config["catalogo"]`, `config["ruta_cmstfl"]`, etc.).
2. The notebooks de Bronce shall importar el framework LSDP mediante `from pyspark import pipelines as dp` y PySpark mediante `from pyspark.sql import functions as F`.
3. The notebooks de Bronce shall no redefinir parámetros ni constantes que ya existan en el módulo de configuración centralizada.
4. If un parámetro del pipeline no está configurado, the función `obtener_configuracion(spark)` shall propagar el error nativo de `spark.conf.get()` sin enmascararlo con valores por defecto.
