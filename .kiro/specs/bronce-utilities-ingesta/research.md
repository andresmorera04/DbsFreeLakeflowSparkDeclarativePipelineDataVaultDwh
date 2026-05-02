# Investigación y Decisiones de Diseño — bronce-utilities-ingesta

---

## Resumen

- **Feature**: `bronce-utilities-ingesta`
- **Alcance del Discovery**: Extensión — los patrones arquitectónicos están definidos en SYSTEM.md; este incremento los materializa en código.
- **Hallazgos Clave**:
  1. El patrón de ingesta Bronce de 2 capas (ST temporal + MV snapshot) está completamente especificado y verificado con código de referencia en SYSTEM.md.
  2. El módulo de configuración (`LSDPConfiguracion.py`) opera como Python puro ejecutado dentro del contexto del pipeline LSDP, donde `spark` ya existe como variable global — no requiere instanciación propia.
  3. Las funciones helper de hash reutilizables (`calcular_hash_hub`, `calcular_hash_diferenciador`) serán consumidas por Plata y Oro pero deben residir en el módulo de utilidades desde este incremento.

---

## Log de Investigación

### Mecanismo de Compartición de Código en Pipelines LSDP

- **Contexto**: Los notebooks de `transformations/` necesitan acceder a parámetros y funciones definidos en `utilities/LSDPConfiguracion.py`. En Databricks, los notebooks de un pipeline LSDP residen en la misma carpeta raíz del pipeline y comparten el mismo proceso Spark.
- **Fuentes Consultadas**: SYSTEM.md secciones 1.6, 6.1, 6.2, 6.3; documentación oficial de Databricks sobre LSDP source code configuration.
- **Hallazgos**:
  - En un pipeline LSDP, los notebooks configurados como `source_code` se ejecutan en el mismo proceso Python/Spark y `spark` es una variable global inyectada por el runtime.
  - Los archivos Python puro de `utilities/` **NO forman parte del source_code del pipeline LSDP**. Son módulos externos que los notebooks (que sí son source_code) importan explícitamente.
  - Al no ser source_code, los módulos de utilities **no tienen acceso directo** a la variable global `spark` del runtime LSDP. Deben recibir `spark` (y `dbutils` si se necesita) como parámetro de sus funciones.
  - Las funciones helper del módulo de utilidades se importan con `import` estándar de Python desde los notebooks.
- **Implicaciones**: `LSDPConfiguracion.py` debe exponer una función que reciba `spark` como parámetro para leer los `spark.conf.get()`. Los notebooks invocan esta función pasándole su `spark` local. Las constantes de negocio que no dependen de `spark` permanecen a nivel de módulo.

### AutoLoader — Opciones y Configuración de Schema Evolution

- **Contexto**: Las Streaming Tables de Bronce usan AutoLoader para ingesta incremental. Se necesita verificar las opciones exactas compatibles con Serverless.
- **Fuentes Consultadas**: SYSTEM.md secciones 1.4, 1.8; steering `tech.md`.
- **Hallazgos**:
  - `cloudFiles.inferColumnTypes = true`: Infiere tipos de datos del Parquet (en vez de tratar todo como STRING).
  - `cloudFiles.schemaEvolutionMode = addNewColumns`: Permite agregar columnas nuevas sin romper el pipeline.
  - `cloudFiles.schemaLocation`: Ruta en Volumes para almacenar el checkpoint del esquema. Necesario para schema evolution.
  - Las columnas de partición (`año`, `mes`, `dia`) se infieren automáticamente desde la estructura `clave=valor` del directorio como `StringType`.
  - `_rescued_data` (StringType) se genera automáticamente para capturar datos incompatibles con el esquema.
- **Implicaciones**: Cada fuente necesita su propia `schemaLocation` para evitar conflictos entre checkpoints. La ruta sigue el patrón `{ruta_base_autoloader}/{NOMBRE_ORIGEN}`.

### Patrón de Snapshot Más Reciente — Estrategia de Join con Broadcast

- **Contexto**: La Capa 2 de Bronce filtra por la fecha más reciente. Con 4-7M de registros, la estrategia de join importa para el rendimiento.
- **Fuentes Consultadas**: SYSTEM.md sección 1.8; patrones de código verificados en sección 5.3.
- **Hallazgos**:
  - La Materialized View lee la ST temporal completa, calcula `F.max("FechaRegistroParquet")` como un DataFrame de una fila, y hace un broadcast join.
  - `F.broadcast()` es seguro en Serverless (reemplaza `sc.broadcast()` que está prohibido). Es un join hint, no una broadcast variable.
  - El patrón está verificado: `st.join(F.broadcast(max_fecha), st.FechaRegistroParquet == max_fecha.max_fecha).drop("max_fecha")`.
  - La MV se recalcula completamente en cada ejecución (overwrite semántico implícito de LSDP).
- **Implicaciones**: El patrón es idéntico para las 3 fuentes. Solo cambia el nombre de la tabla temporal y la tabla destino.

### Liquid Clustering en Streaming Tables Temporales

- **Contexto**: Verificar si `cluster_by` es soportado en Streaming Tables temporales con `@dp.table()`.
- **Fuentes Consultadas**: SYSTEM.md sección 1.3; documentación de Databricks CLI/LSDP.
- **Hallazgos**:
  - `@dp.table()` soporta `cluster_by` como parámetro. Funciona tanto para tablas temporales como permanentes.
  - Para las ST temporales de Bronce, se usa exclusivamente `FechaRegistroParquet` como clave LC.
  - Para las MV de Bronce, también se usa exclusivamente `FechaRegistroParquet`.
  - La función helper `reordenar_columnas_lc()` no se necesita en Bronce (las columnas LC son derivadas, no del esquema original), pero se implementa en utilidades para uso de Plata y Oro.
- **Implicaciones**: El parámetro `cluster_by=["FechaRegistroParquet"]` se incluye tanto en `@dp.table()` como en `@dp.materialized_view()`.

---

## Evaluación de Patrones Arquitectónicos

| Opción | Descripción | Fortalezas | Riesgos / Limitaciones | Notas |
|--------|-------------|-----------|------------------------|-------|
| Un notebook por fuente | Cada fuente (CMSTFL, TRXPFL, BLNCFL) tiene su propio notebook con ST + MV | Clara separación funcional; fallos aislados por fuente; fácil mantenimiento | Duplicación del patrón de ingesta (mitigable con helper) | **Seleccionado** — alineado con SYSTEM.md y `structure.md` |
| Notebook único para toda la Bronce | Una sola archivo define las 6 tablas (3 ST + 3 MV) | Menos archivos; todo en un lugar | Archivo largo; difícil debugging; violación del principio "un notebook por unidad funcional" | Descartado — viola convención del steering |
| Configuración por fuente (3 módulos) | Un `LSDPConfiguracion{Fuente}.py` por cada fuente | Aislamiento total | Duplicación innecesaria; divergencia de constantes compartidas | Descartado — sobre-ingeniería para este alcance |

---

## Decisiones de Diseño

### Decisión: Módulo único de configuración vs. múltiples módulos — ✅ CONFIRMADA

- **Contexto**: Los parámetros del pipeline se comparten entre las 3 medallas. Las constantes de negocio son transversales.
- **Alternativas Consideradas**:
  1. Un solo `LSDPConfiguracion.py` con todo (parámetros + constantes + helpers).
  2. Separar en `LSDPConfiguracion.py` (parámetros + constantes) y `LSDPUtilidadPrincipal.py` (funciones helper).
- **Enfoque Seleccionado**: Dos módulos separados con responsabilidad clara.
- **Justificación**: Separación de responsabilidades. `LSDPConfiguracion.py` centraliza parámetros del pipeline y constantes de negocio. `LSDPUtilidadPrincipal.py` encapsula funciones helper reutilizables de hash y LC.
- **Ajuste Post-Aprobación**: Los archivos Python puro de `utilities/` **NO son source_code del pipeline LSDP**. Son módulos externos importados desde los notebooks. Por tanto, toda función que necesite acceso al runtime debe recibir `spark` (y `dbutils` si aplica) como parámetro explícito. Las constantes inmutables permanecen a nivel de módulo.
- **Trade-offs**: Dos archivos en vez de uno, pero cada uno tiene responsabilidad clara. Las funciones reciben `spark` como parámetro en vez de accederlo como global.
- **Seguimiento**: N/A — confirmada por el stakeholder.

### Decisión: Streaming Table temporal vs. tabla permanente para Capa 1 — ✅ CONFIRMADA

- **Contexto**: La Capa 1 almacena la historia incremental de AutoLoader. ¿Debe registrarse en Unity Catalog?
- **Alternativas Consideradas**:
  1. ST temporal (`temporary=True`) — no visible en UC, solo accesible dentro del pipeline.
  2. ST permanente — visible en UC, accesible desde fuera del pipeline.
- **Enfoque Seleccionado**: ST temporal (`temporary=True`).
- **Justificación**: La ST de Capa 1 es un artefacto intermedio. Solo la MV de Capa 2 (snapshot) debe ser visible para Plata/Oro. Alineado con SYSTEM.md y patrón aprobado.
- **Trade-offs**: La historia incremental completa no es directamente consultable fuera del pipeline. Ventaja: menos tablas en UC, menos permisos que gestionar.
- **Seguimiento**: N/A — confirmada por el stakeholder. Si en el futuro se necesita acceso a la historia completa, considerar cambiar a `temporary=False`.

### Decisión: Nombre de la función Python de la MV vs. nombre de la tabla — ✅ CONFIRMADA

- **Contexto**: En LSDP, el nombre de la función Python que define una MV no tiene que coincidir con el nombre de la tabla en UC. ¿Qué convención seguir?
- **Alternativas Consideradas**:
  1. Nombre de función en snake_case descriptivo: `def mv_cmstfl_snapshot():`
  2. Nombre de función igual al nombre de la tabla: `def cmstfl():`
- **Enfoque Seleccionado**: Nombre simple en minúsculas siguiendo el origen: `def cmstfl():`, `def trxpfl():`, `def blncfl():`. Para las ST temporales: `def cmstfl_temp():`, etc.
- **Justificación**: Consistencia con los ejemplos de código de SYSTEM.md sección 1.8. El nombre de 3 partes se define en `name=` del decorador, así que el nombre de la función es solo un identificador interno.
- **Trade-offs**: Nombres cortos pero menos descriptivos. Mitigado por el decorador que lleva el nombre completo.
- **Seguimiento**: N/A — confirmada por el stakeholder.

### Decisión: Parámetros de ruta específicos por fuente vs. rutas derivadas — ✅ NUEVA

- **Contexto**: El diseño original derivaba `ruta_cmstfl` de `ruta_base` y la `schemaLocation` de `ruta_base_autoloader`. Esto genera acoplamiento y riesgo de corrupción si se comparten checkpoints.
- **Alternativas Consideradas**:
  1. 2 parámetros base (`ruta_base`, `ruta_base_autoloader`) con derivación en código.
  2. 6 parámetros específicos: 3 para rutas de datos + 3 para schema locations.
- **Enfoque Seleccionado**: 6 parámetros específicos del pipeline, uno por fuente y tipo.
- **Parámetros de Ruta de Datos (Landing Zone)**:
  - `pipeline.ruta_cmstfl` → ej: `archivo/LSDP_DataVault_DWH/cmstfl/`
  - `pipeline.ruta_trxpfl` → ej: `archivo/LSDP_DataVault_DWH/trxpfl/`
  - `pipeline.ruta_blncfl` → ej: `archivo/LSDP_DataVault_DWH/blncfl/`
- **Parámetros de Schema Location (AutoLoader checkpoint)**:
  - `pipeline.schema_location_cmstfl` → ej: `AutoLoader/schema/cmstfl/`
  - `pipeline.schema_location_trxpfl` → ej: `AutoLoader/schema/trxpfl/`
  - `pipeline.schema_location_blncfl` → ej: `AutoLoader/schema/blncfl/`
- **Justificación**: Elimina riesgo de corrupción de checkpoint por schemaLocation compartida. Cada fuente es completamente independiente en su configuración de ruta y checkpoint. Los parámetros se configuran manualmente al crear el pipeline LSDP (entorno de laboratorio).
- **Trade-offs**: 6 parámetros en vez de 2 derivados. Mayor configuración manual, pero mayor seguridad y aislamiento. Total de parámetros del pipeline: 13 (antes 9).
- **Seguimiento**: N/A — decisión del stakeholder.

---

## Riesgos Identificados

| Riesgo | Probabilidad | Impacto | Mitigación | Estado |
|--------|-------------|---------|------------|--------|
| `schemaLocation` compartida entre fuentes causa corrupción de checkpoint | Baja | Alto | 3 parámetros independientes de schema location (uno por fuente): `schema_location_cmstfl`, `schema_location_trxpfl`, `schema_location_blncfl` | ✅ Mitigado — parámetros específicos confirmados |
| Evolución de esquema en Parquet rompe la ST temporal | Baja | Medio | `schemaEvolutionMode = addNewColumns` permite agregar columnas. `_rescued_data` captura incompatibilidades | ✅ Aceptado — mecanismo nativo de AutoLoader |
| Parámetro del pipeline no configurado en el JSON del job | Media | Alto | Los 13 parámetros se configuran manualmente al crear el pipeline LSDP. `spark.conf.get()` lanza error nativo si falta alguno (Req 10.4). Entorno de laboratorio con configuración manual. | ✅ Aceptado — configuración manual en laboratorio |
| `F.broadcast()` en snapshot con muchos registros causa OOM | Muy Baja | Medio | El broadcast es solo del DataFrame de 1 fila (la fecha máxima), nunca de los datos completos | ✅ Aceptado — patrón verificado en SYSTEM.md |
