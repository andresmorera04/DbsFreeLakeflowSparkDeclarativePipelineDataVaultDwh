# Documento de Requisitos — plata-data-vault-notebooks

## Introducción

Este documento define los requisitos para el **Incremento 2** del pipeline LSDP del Data Warehouse sobre Databricks Free Edition Serverless. El alcance cubre la **Medalla de Plata** completa: implementación del modelo **Data Vault 2.0** compuesto por 3 Hubs, 2 Links y 9 Satellites, consumiendo las tablas de Bronce (CMSTFL, TRXPFL, BLNCFL) ya entregadas en el Incremento 1. Incluye también la creación de funciones de utilidad nuevas necesarias para el procesamiento de Satellites (detección de cambios con `Hash_Diferenciador`) en la carpeta `utilities/`.

Todo el código debe cumplir al 100% las restricciones de Databricks Free Edition Serverless Compute y seguir los patrones definidos en SYSTEM.md.

---

## Descripción del Proyecto (Input)

Incremento 2 del proyecto LSDP: Desarrollo de los Notebooks de Plata (Medalla de Plata) para el pipeline Lakeflow Spark Declarative Pipelines (LSDP). Este incremento implementa la capa de Plata siguiendo el modelo Data Vault 2.0, que incluye Hubs, Links y Satellites. Las transformaciones parten de las tablas de Bronce (CMSTFL, TRXPFL, BLNCFL) ya implementadas en el incremento 1 y producen las entidades Data Vault registradas en el Unity Catalog bajo el catálogo y esquema de Plata (pipeline.catalogo_plata / pipeline.esquema_plata). Cualquier función de utilidad nueva requerida debe crearse en la carpeta `src/LSDP_Lab_DataVault_DWH/utilities/`. El archivo SYSTEM.md contiene todas las definiciones del modelo de datos, patrones de código seguros, restricciones de compatibilidad Serverless y la parametrización del pipeline que rigen este desarrollo.

---

## Requisitos

### Requisito 1: Organización y Nombrado de Notebooks de Plata

**Objetivo:** Como ingeniero de datos, quiero que los notebooks de transformación de Plata sigan las convenciones de nombrado y organización del proyecto, para que la estructura del repositorio sea consistente con la medalla de Bronce y las demás medallas.

#### Criterios de Aceptación

1. The Pipeline LSDP shall crear un notebook de transformación independiente por cada entidad Data Vault (Hub, Link o grupo de Satellites de un mismo Hub) dentro de `src/LSDP_Lab_DataVault_DWH/transformations/`.
2. When se cree un notebook de transformación de Plata, the Pipeline LSDP shall nombrarlo con el patrón `LSDPPlata{NombreEntidad}` (ejemplos: `LSDPPlataHubCliente`, `LSDPPlataLinkClienteOperacion`, `LSDPPlataSatCliente`).
3. The notebooks de Plata shall importar la configuración centralizada desde `utilities.LSDPConfiguracion` mediante `obtener_configuracion(spark)` y acceder a `config["catalogo_plata"]` y `config["esquema_plata"]` para construir los nombres de 3 partes de las tablas en Unity Catalog.
4. The notebooks de Plata shall importar las funciones helper `calcular_hash_hub`, `calcular_hash_diferenciador` y `reordenar_columnas_lc` desde `utilities.LSDPUtilidadPrincipal`, sin redefinir lógica de hash ni constantes que ya existan en los módulos de utilidades.
5. The notebooks de Plata shall leer sus datos fuente exclusivamente de las Materialized Views de Bronce registradas en Unity Catalog (`{catalogo}.{esquema}.CMSTFL`, `{catalogo}.{esquema}.TRXPFL`, `{catalogo}.{esquema}.BLNCFL`) mediante `spark.read.table()`, nunca directamente de las Streaming Tables temporales.
6. The notebooks de Plata shall no propagar las columnas exclusivas de Bronce (`año`, `mes`, `dia`, `FechaRegistroParquet`, `_rescued_data`) hacia las tablas de Plata.

---

### Requisito 2: Hub_Cliente

**Objetivo:** Como ingeniero de datos, quiero una tabla Hub en Plata que registre cada llave de negocio de cliente única (IdentificadorCliente) con su hash determinístico, para que los Satellites y Links de cliente puedan referenciarse a través de `Hash_Cliente`.

#### Criterios de Aceptación

1. The notebook de Plata shall definir una Materialized View registrada en Unity Catalog con nombre de 3 partes `{catalogo_plata}.{esquema_plata}.Hub_Cliente`, con `cluster_by=["FechaRegistro", "Hash_Cliente"]`.
2. The Hub_Cliente shall contener exactamente 4 columnas: `FechaRegistro` (TimestampType, `current_timestamp()`), `Hash_Cliente` (StringType, SHA2-256 de `CUSTID` casteado a string), `IdentificadorCliente` (LongType, valor de `CUSTID`) y `FuenteDatos` (StringType, nombre de 3 partes de la tabla fuente de Bronce).
3. The Hub_Cliente shall leer de la tabla de Bronce `{catalogo}.{esquema}.CMSTFL` y eliminar duplicados por `IdentificadorCliente`, reteniendo una sola fila por llave de negocio.
4. The Hub_Cliente shall usar la función `reordenar_columnas_lc()` para colocar las columnas de Liquid Clustering (`FechaRegistro`, `Hash_Cliente`) en las primeras posiciones del esquema.

---

### Requisito 3: Hub_Operacion

**Objetivo:** Como ingeniero de datos, quiero una tabla Hub en Plata que registre cada llave de negocio compuesta de operación/saldo (IdentificadorCliente + SecuenciaSaldo) con su hash determinístico, para que los Satellites y Links de operación puedan referenciarse a través de `Hash_Operacion`.

#### Criterios de Aceptación

1. The notebook de Plata shall definir una Materialized View registrada en Unity Catalog con nombre de 3 partes `{catalogo_plata}.{esquema_plata}.Hub_Operacion`, con `cluster_by=["FechaRegistro", "Hash_Operacion"]`.
2. The Hub_Operacion shall contener exactamente 5 columnas: `FechaRegistro` (TimestampType), `Hash_Operacion` (StringType, SHA2-256 de `CONCAT_WS("|", CAST(CUSTID AS STRING), CAST(BLSQ AS STRING))`), `IdentificadorCliente` (LongType), `SecuenciaSaldo` (LongType) y `FuenteDatos` (StringType).
3. The Hub_Operacion shall leer de la tabla de Bronce `{catalogo}.{esquema}.BLNCFL` y eliminar duplicados por la combinación (`IdentificadorCliente`, `SecuenciaSaldo`).
4. The Hub_Operacion shall usar la función `reordenar_columnas_lc()` para colocar las columnas de Liquid Clustering en las primeras posiciones del esquema.

---

### Requisito 4: Hub_Transaccion

**Objetivo:** Como ingeniero de datos, quiero una tabla Hub en Plata que registre cada llave de negocio de transacción única (IdentificadorTransaccion) con su hash determinístico, para que los Satellites y Links de transacción puedan referenciarse a través de `Hash_Transaccion`.

#### Criterios de Aceptación

1. The notebook de Plata shall definir una Materialized View registrada en Unity Catalog con nombre de 3 partes `{catalogo_plata}.{esquema_plata}.Hub_Transaccion`, con `cluster_by=["FechaRegistro", "Hash_Transaccion"]`.
2. The Hub_Transaccion shall contener exactamente 4 columnas: `FechaRegistro` (TimestampType), `Hash_Transaccion` (StringType, SHA2-256 de `TRXID` sin cast ya que es nativo StringType), `IdentificadorTransaccion` (StringType) y `FuenteDatos` (StringType).
3. The Hub_Transaccion shall leer de la tabla de Bronce `{catalogo}.{esquema}.TRXPFL` y eliminar duplicados por `IdentificadorTransaccion`.
4. The Hub_Transaccion shall usar la función `reordenar_columnas_lc()` para colocar las columnas de Liquid Clustering en las primeras posiciones del esquema.

---

### Requisito 5: Link_Cliente_Operacion

**Objetivo:** Como ingeniero de datos, quiero una tabla Link en Plata que capture la relación entre Hub_Cliente y Hub_Operacion, para que el modelo Data Vault represente la asociación cliente-operación con su hash de enlace determinístico.

#### Criterios de Aceptación

1. The notebook de Plata shall definir una Materialized View registrada en Unity Catalog con nombre de 3 partes `{catalogo_plata}.{esquema_plata}.Link_Cliente_Operacion`, con `cluster_by=["FechaRegistro", "Hash_Cliente", "Hash_Operacion"]`.
2. The Link_Cliente_Operacion shall contener exactamente 5 columnas: `FechaRegistro` (TimestampType), `Hash_Link_Cliente_Operacion` (StringType, SHA2-256 de `CONCAT_WS("|", Hash_Cliente, Hash_Operacion)`), `Hash_Cliente` (StringType), `Hash_Operacion` (StringType) y `FuenteDatos` (StringType).
3. The Link_Cliente_Operacion shall leer de la tabla de Bronce `{catalogo}.{esquema}.BLNCFL`, calcular `Hash_Cliente` a partir de `CUSTID` y `Hash_Operacion` a partir de la combinación `CUSTID` + `BLSQ`, y eliminar duplicados por la combinación de hashes.
4. The Link_Cliente_Operacion shall usar la función `reordenar_columnas_lc()` para colocar las columnas de Liquid Clustering en las primeras posiciones del esquema.

---

### Requisito 6: Link_Cliente_Transaccion

**Objetivo:** Como ingeniero de datos, quiero una tabla Link en Plata que capture la relación entre Hub_Cliente y Hub_Transaccion, para que el modelo Data Vault represente la asociación cliente-transacción con su hash de enlace determinístico.

#### Criterios de Aceptación

1. The notebook de Plata shall definir una Materialized View registrada en Unity Catalog con nombre de 3 partes `{catalogo_plata}.{esquema_plata}.Link_Cliente_Transaccion`, con `cluster_by=["FechaRegistro", "Hash_Cliente", "Hash_Transaccion"]`.
2. The Link_Cliente_Transaccion shall contener exactamente 5 columnas: `FechaRegistro` (TimestampType), `Hash_Link_Cliente_Transaccion` (StringType, SHA2-256 de `CONCAT_WS("|", Hash_Cliente, Hash_Transaccion)`), `Hash_Cliente` (StringType), `Hash_Transaccion` (StringType) y `FuenteDatos` (StringType).
3. The Link_Cliente_Transaccion shall leer de la tabla de Bronce `{catalogo}.{esquema}.TRXPFL`, calcular `Hash_Cliente` a partir de `CUSTID` y `Hash_Transaccion` a partir de `TRXID`, y eliminar duplicados por la combinación de hashes.
4. The Link_Cliente_Transaccion shall usar la función `reordenar_columnas_lc()` para colocar las columnas de Liquid Clustering en las primeras posiciones del esquema.

---

### Requisito 7: Satellites de Hub_Cliente (4 Satellites)

**Objetivo:** Como ingeniero de datos, quiero 4 tablas Satellite en Plata que almacenen los atributos del cliente agrupados por tasa de cambio, con detección de cambios Append-Only, para que el Data Vault mantenga historial inmutable de cada variación detectada.

#### Criterios de Aceptación

1. The Pipeline LSDP shall definir 4 Streaming Tables Acumulativas de tipo Satellite (definidas con `dp.create_streaming_table()` + `@dp.append_flow()`) para Hub_Cliente: `Sat_Cliente_DatosEstables`, `Sat_Cliente_Contacto`, `Sat_Cliente_Clasificacion` y `Sat_Cliente_Financiero`, registradas en Unity Catalog bajo `{catalogo_plata}.{esquema_plata}`.
2. The Sat_Cliente_DatosEstables shall contener los campos de identidad y atributos estables del cliente (sexo, tratamiento, fecha de nacimiento, año de nacimiento, edad, país de residencia, nacionalidad, número de licencia de conducir, tipo de documento, cantidad de pasaportes, idioma preferido) más los campos calculados `RangoEtario` y `CategoriaIngresos`, además de las columnas obligatorias `Hash_Cliente`, `Hash_Diferenciador`, `FechaRegistro` y `FuenteDatos`.
3. The Sat_Cliente_Contacto shall contener los campos de contacto y datos personales del cliente (nombre, apellido, nombre medio, nombre completo, dirección calle, dirección apartamento, ciudad, estado/provincia, código postal, teléfono principal, teléfono móvil, correo electrónico, estado civil, ocupación, nivel educativo) además de las columnas obligatorias `Hash_Cliente`, `Hash_Diferenciador`, `FechaRegistro` y `FuenteDatos`.
4. The Sat_Cliente_Clasificacion shall contener los campos de clasificación y segmentación bancaria del cliente (tipo de cliente, segmento, región geográfica, sucursal principal, gerente asignado, referencia interna, fuente de referencia, grupo de afinidad, preferencia de comunicación, nivel de riesgo, indicador VIP, estado del perfil, estado KYC, indicador de flags, último canal, calificación crediticia, cuenta activa, clasificación interna, nota del cliente) además de las columnas obligatorias `Hash_Cliente`, `Hash_Diferenciador`, `FechaRegistro` y `FuenteDatos`.
5. The Sat_Cliente_Financiero shall contener los campos numéricos financieros (cantidad de cuentas, cantidad de transacciones, score de cliente, ranking de préstamos, cantidad de registros, ingresos de cliente, saldo disponible maestro) y las 18 fechas de evento del cliente, además de las columnas obligatorias `Hash_Cliente`, `Hash_Diferenciador`, `FechaRegistro` y `FuenteDatos`.
6. The 4 Satellites de Cliente shall usar `cluster_by=["FechaRegistro", "Hash_Cliente"]` como columnas de Liquid Clustering.
7. The 4 Satellites de Cliente shall leer de la tabla de Bronce `{catalogo}.{esquema}.CMSTFL` y renombrar los campos AS400 a sus nombres en español según el modelo de datos definido en SYSTEM.md.
8. When se invoque `calcular_hash_diferenciador()` para cualquier Satellite de Cliente, the Pipeline LSDP shall excluir las columnas `FechaRegistro` y `FuenteDatos` del listado de campos enviados a la función, ya que son columnas obligatorias del modelado Data Vault 2.0 que no representan atributos de negocio y no deben influir en la detección de cambios.

---

### Requisito 8: Satellites de Hub_Operacion (3 Satellites)

**Objetivo:** Como ingeniero de datos, quiero 3 tablas Satellite en Plata que almacenen los atributos de operación/saldo agrupados por tasa de cambio, con detección de cambios Append-Only, para que el Data Vault mantenga historial inmutable de cada variación en saldos y cuentas.

#### Criterios de Aceptación

1. The Pipeline LSDP shall definir 3 Streaming Tables Acumulativas de tipo Satellite (definidas con `dp.create_streaming_table()` + `@dp.append_flow()`) para Hub_Operacion: `Sat_Operacion_DatosEstables`, `Sat_Operacion_Montos` y `Sat_Operacion_FechasEvento`, registradas en Unity Catalog bajo `{catalogo_plata}.{esquema_plata}`.
2. The Sat_Operacion_DatosEstables shall contener los 31 campos cualitativos de la cuenta (tipo de cuenta, número de cuenta, moneda, estado, sucursal, producto, subproducto, nombre, clase, riesgo, tipo de producto, gerente, referencia, centro de costos, grupo de afinidad, plan, región, sufijo, nota, último canal, perfil, autorización, texto, grupo, email, frecuencia, clave de seguridad, VIP, factor de clasificación) más los campos calculados `CategoriaSaldo`, `EstadoUtilizacionCredito` e `IndicadorSobregiro`, además de las columnas obligatorias `Hash_Operacion`, `Hash_Diferenciador`, `FechaRegistro` y `FuenteDatos`.
3. The Sat_Operacion_Montos shall contener los 34 campos monetarios y ratios financieros (saldo disponible, saldo total, saldo reservado, saldo bloqueado, límite de crédito, crédito utilizado, crédito disponible, valor sobregiro, límite sobregiro, depósitos pendientes, cargos pendientes, ajustes pendientes, depósitos ingreso, retenciones, transferencias ingreso, cargos transferencia, comisiones anuales, intereses mensuales, reembolsos, penalidades, bonificaciones, ajustes positivos, ajustes misceláneos, ajustes anuales, marca alta saldo, marca baja saldo, varianza saldo, ratio cuenta, porcentaje aporte, ingresos aporte, saldo mínimo, saldo máximo, tasa de interés, multiplicador penalidad) además de las columnas obligatorias `Hash_Operacion`, `Hash_Diferenciador`, `FechaRegistro` y `FuenteDatos`.
4. The Sat_Operacion_FechasEvento shall contener las fechas de eventos de la cuenta (apertura, expiración, actualización, último movimiento, cambio de estado, penalidad, renovación, maduración, cierre, bloqueo, fondeo, gracia, histórica, interés, ajuste, KYC, notificación, transferencia, verificación) además de las columnas obligatorias `Hash_Operacion`, `Hash_Diferenciador`, `FechaRegistro` y `FuenteDatos`.
5. The 3 Satellites de Operación shall usar `cluster_by=["FechaRegistro", "Hash_Operacion"]` como columnas de Liquid Clustering.
6. The 3 Satellites de Operación shall leer de la tabla de Bronce `{catalogo}.{esquema}.BLNCFL` y renombrar los campos AS400 a sus nombres en español según el modelo de datos definido en SYSTEM.md.
7. When se invoque `calcular_hash_diferenciador()` para cualquier Satellite de Operación, the Pipeline LSDP shall excluir las columnas `FechaRegistro` y `FuenteDatos` del listado de campos enviados a la función, ya que son columnas obligatorias del modelado Data Vault 2.0 que no representan atributos de negocio y no deben influir en la detección de cambios.

---

### Requisito 9: Satellites de Hub_Transaccion (2 Satellites)

**Objetivo:** Como ingeniero de datos, quiero 2 tablas Satellite en Plata que almacenen los atributos de transacción agrupados por tasa de cambio, con detección de cambios Append-Only, para que el Data Vault mantenga historial inmutable de cada variación en transacciones financieras.

#### Criterios de Aceptación

1. The Pipeline LSDP shall definir 2 Streaming Tables Acumulativas de tipo Satellite (definidas con `dp.create_streaming_table()` + `@dp.append_flow()`) para Hub_Transaccion: `Sat_Transaccion_DatosEstables` y `Sat_Transaccion_Montos`, registradas en Unity Catalog bajo `{catalogo_plata}.{esquema_plata}`.
2. The Sat_Transaccion_DatosEstables shall contener los campos categóricos estables de la transacción (tipo de transacción, moneda, estado, canal, descripción, referencia externa, secuencia, monto máximo, monto mínimo, y las 19 fechas auxiliares y 2 timestamps) más el campo calculado `ClasificacionCanalATM`, además de las columnas obligatorias `Hash_Transaccion`, `Hash_Diferenciador`, `FechaRegistro` y `FuenteDatos`.
3. The Sat_Transaccion_Montos shall contener los campos monetarios y de riesgo de la transacción (identificador de cliente, fecha de transacción, monto principal, comisión, saldo posterior, saldo anterior, cargo fiscal, monto local, monto pago, beneficio, pérdida tasa, monto promedio, desviación monto, riesgo transacción, riesgo fraude, límite transacción, porcentaje límite, cargo plataforma, cargo institución, cargo extranjero, cargo varianza, subtotal, total, residuo, margen interés, monto neto, monto original, monto inversión, descuento, monto principal préstamo) más los campos calculados `RangoMontoTransaccion` y `NivelRiesgoFraude`, además de las columnas obligatorias `Hash_Transaccion`, `Hash_Diferenciador`, `FechaRegistro` y `FuenteDatos`.
4. The 2 Satellites de Transacción shall usar `cluster_by=["FechaRegistro", "Hash_Transaccion"]` como columnas de Liquid Clustering.
5. The 2 Satellites de Transacción shall leer de la tabla de Bronce `{catalogo}.{esquema}.TRXPFL` y renombrar los campos AS400 a sus nombres en español según el modelo de datos definido en SYSTEM.md.
6. When se invoque `calcular_hash_diferenciador()` para cualquier Satellite de Transacción, the Pipeline LSDP shall excluir las columnas `FechaRegistro` y `FuenteDatos` del listado de campos enviados a la función, ya que son columnas obligatorias del modelado Data Vault 2.0 que no representan atributos de negocio y no deben influir en la detección de cambios.

---

### Requisito 10: Detección de Cambios en Satellites (Append-Only)

**Objetivo:** Como ingeniero de datos, quiero que todos los Satellites implementen detección de cambios basada en `Hash_Diferenciador` con procesamiento Append-Only, para que solo se inserten registros nuevos cuando se detecte una variación real en los atributos del Satellite.

#### Criterios de Aceptación

1. The Pipeline LSDP shall calcular un `Hash_Diferenciador` SHA2-512 para cada Satellite, concatenando el hash de la entidad padre (ej: `Hash_Cliente`) con todos los campos de negocio del Satellite (incluidos los campos calculados) mediante el separador pipe `|`, excluyendo explícitamente las columnas obligatorias del modelado Data Vault 2.0 (`FechaRegistro` y `FuenteDatos`) que no representan atributos de negocio y no deben participar en la detección de cambios.
2. When el Pipeline LSDP procese un Satellite, the Pipeline LSDP shall comparar el `Hash_Diferenciador` del registro entrante con el `Hash_Diferenciador` del último registro existente para la misma llave hash de entidad, e insertar el registro únicamente si el hash difiere o si no existe registro previo para esa llave.
3. The Pipeline LSDP shall no actualizar ni eliminar registros existentes en ningún Satellite; solo se permite la operación de inserción (Append-Only).
4. If la tabla del Satellite no existe en la primera ejecución del pipeline, the Pipeline LSDP shall insertar todos los registros entrantes sin comparación de hashes.
5. The Pipeline LSDP shall implementar la lógica de detección de cambios como una función reutilizable en `utilities/` para evitar duplicar código en cada notebook de Satellite.

---

### Requisito 11: Campos Calculados en Satellites

**Objetivo:** Como ingeniero de datos, quiero que los Satellites contengan campos derivados calculados a partir de umbrales de negocio centralizados, para que las medallas superiores (Oro) y los consumidores analíticos dispongan de clasificaciones predeterminadas sin recomputarlas.

#### Criterios de Aceptación

1. The Sat_Cliente_DatosEstables shall calcular el campo `RangoEtario` clasificando `edad_cliente` según los umbrales definidos en `UMBRAL_RANGO_ETARIO` de `LSDPConfiguracion.py` (JOVEN_ADULTO: 18-25, ADULTO: 26-35, ADULTO_MEDIO: 36-45, ADULTO_MAYOR: 46-55, SENIOR: 56+).
2. The Sat_Cliente_DatosEstables shall calcular el campo `CategoriaIngresos` clasificando `ingresos_cliente` según los umbrales definidos en `UMBRAL_CATEGORIA_INGRESOS` de `LSDPConfiguracion.py` (BAJO: 0-15000, MEDIO: 15001-35000, ALTO: 35001-65000, MUY_ALTO: 65001-85000, PREMIUM: 85001+).
3. The Sat_Operacion_DatosEstables shall calcular los campos `CategoriaSaldo` (según `UMBRAL_CATEGORIA_SALDO`), `EstadoUtilizacionCredito` (según `UMBRAL_UTILIZACION_CREDITO`) y `IndicadorSobregiro` (según `UMBRAL_SOBREGIRO`), todos a partir de los umbrales definidos en `LSDPConfiguracion.py`.
4. The Sat_Transaccion_DatosEstables shall calcular el campo `ClasificacionCanalATM` categorizado como RETIRO_ATM (tipo DATM), DEPOSITO_ATM (tipo CATM), OTRA_OP_ATM (canal ATM pero tipo distinto) o NO_ATM (canal diferente a ATM), usando las constantes `TIPO_DATM` y `TIPO_CATM` de `LSDPConfiguracion.py`.
5. The Sat_Transaccion_Montos shall calcular los campos `RangoMontoTransaccion` (según `UMBRAL_RANGO_MONTO`) y `NivelRiesgoFraude` (según `UMBRAL_RIESGO_FRAUDE`, escala 0-100), ambos a partir de los umbrales definidos en `LSDPConfiguracion.py`.
6. The Pipeline LSDP shall construir los campos calculados exclusivamente con funciones nativas de `pyspark.sql.functions` (como `F.when().otherwise()`), sin UDFs, y referenciando las constantes de umbrales de `LSDPConfiguracion.py`.
7. The campos calculados shall incluirse en el cálculo del `Hash_Diferenciador` del Satellite correspondiente, de modo que un cambio en un campo calculado (porque cambió el valor fuente) genere una nueva inserción.

---

### Requisito 12: Calidad de Datos (Expectations) en Plata

**Objetivo:** Como ingeniero de datos, quiero que las tablas de Plata apliquen reglas de calidad de datos mediante expectations de LSDP, para que se detecten y gestionen registros inválidos según la severidad definida por regla.

#### Criterios de Aceptación

1. The Hub_Cliente y Hub_Operacion shall aplicar la expectation `id_cliente_positivo` con severidad DROP (`@dp.expect_or_drop`) que valide `IdentificadorCliente > 0`.
2. The Hub_Transaccion shall aplicar la expectation `id_transaccion_no_nulo` con severidad FAIL (`@dp.expect_or_fail`) que valide `IdentificadorTransaccion IS NOT NULL`.
3. The Sat_Cliente_Financiero shall aplicar la expectation `score_cliente_en_rango` con severidad WARN (`@dp.expect`) que valide `score_cliente BETWEEN 300 AND 1150`.
4. The Sat_Transaccion_Montos shall aplicar la expectation `monto_transaccion_positivo` con severidad DROP (`@dp.expect_or_drop`) que valide `monto_principal > 0`.
5. The todos los Satellites shall aplicar la expectation `hash_diferenciador_no_nulo` con severidad FAIL (`@dp.expect_or_fail`) que valide `Hash_Diferenciador IS NOT NULL`.
6. The todos los Hubs shall aplicar una expectation de hash no nulo con severidad FAIL para validar que el hash de la entidad (`Hash_Cliente`, `Hash_Operacion` o `Hash_Transaccion`) no sea nulo.

---

### Requisito 13: Compatibilidad Serverless y Reglas Técnicas

**Objetivo:** Como ingeniero de datos, quiero que todo el código de Plata sea 100% compatible con Databricks Free Edition Serverless Compute, para que el pipeline ejecute sin errores de runtime y siga los patrones técnicos validados.

#### Criterios de Aceptación

1. The Pipeline LSDP shall no utilizar `.cache()`, `.persist()`, `spark.sparkContext`, `sc.`, operaciones RDD, UDFs, threading ni multiprocessing en ningún notebook ni módulo de utilidades de Plata.
2. The Pipeline LSDP shall usar `from pyspark import pipelines as dp` como import de LSDP; nunca `import databricks.sdk.pipelines as dp`.
3. When se defina una Materialized View de Plata, the Pipeline LSDP shall pasar el nombre completo de 3 partes (`{catalogo_plata}.{esquema_plata}.{nombre_tabla}`) en el parámetro `name=`; nunca usar `catalog=` ni `schema=` como kwargs separados.
4. The Pipeline LSDP shall usar SHA2-256 (vía `F.sha2()`) para hashes de Hubs y Links, y SHA2-512 para `Hash_Diferenciador` de Satellites, siguiendo los patrones de hash definidos en SYSTEM.md.
5. The Pipeline LSDP shall usar `F.concat_ws()` con el separador pipe `|` (constante `HASH_SEPARATOR`) para concatenar múltiples columnas antes de calcular hashes; nunca el operador `+` en columnas.
6. The Pipeline LSDP shall aplicar `.cast("string")` a toda columna no-StringType antes de incluirla en un cálculo de hash, excepto para columnas que ya sean StringType nativo (como `TRXID`).
7. If se requiere `F.hash()` en algún procesamiento auxiliar, the Pipeline LSDP shall aplicar `.cast("long")` antes de `F.abs()` para evitar overflow ANSI con `Integer.MIN_VALUE`.

---

### Requisito 14: Integración con Utilidades Existentes y Nuevas Funciones

**Objetivo:** Como ingeniero de datos, quiero que los notebooks de Plata reutilicen las utilidades existentes del Incremento 1 y que cualquier función nueva necesaria se cree en la carpeta `utilities/`, para mantener la centralización del código y evitar duplicación.

#### Criterios de Aceptación

1. The notebooks de Plata shall reutilizar las funciones `calcular_hash_hub()`, `calcular_hash_diferenciador()` y `reordenar_columnas_lc()` ya implementadas en `utilities/LSDPUtilidadPrincipal.py`.
2. The notebooks de Plata shall reutilizar todas las constantes de negocio (`HASH_HUB_LINK_BITS`, `HASH_SATELLITE_BITS`, `HASH_SEPARATOR`, `TIPO_DATM`, `TIPO_CATM`, `TIPOS_ATM`, y todos los diccionarios `UMBRAL_*`) ya definidas en `utilities/LSDPConfiguracion.py`.
3. When se necesite la función de detección de cambios para Satellites (comparación de `Hash_Diferenciador` entre registro entrante y último existente), the Pipeline LSDP shall implementarla como una nueva función en `utilities/LSDPUtilidadPrincipal.py` (o en un nuevo módulo de utilidades si la complejidad lo justifica) y no duplicar esa lógica en cada notebook de Satellite.
4. When se necesite una nueva función para construir campos calculados basados en umbrales (como `RangoEtario` o `CategoriaSaldo`), the Pipeline LSDP shall implementarla como función genérica reutilizable en `utilities/LSDPUtilidadPrincipal.py` que reciba el diccionario de umbrales y la columna fuente como parámetros.
5. The funciones nuevas en `utilities/` shall no contener imports de LSDP (`from pyspark import pipelines as dp`) ya que son módulos Python puro externos al source_code del pipeline.
