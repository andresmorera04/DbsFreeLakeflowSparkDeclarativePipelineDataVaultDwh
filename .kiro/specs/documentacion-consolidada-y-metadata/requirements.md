# Requirements Document

## Introduction

Este incremento consolida la documentación oficial del laboratorio LSDP Data Vault DWH y materializa los metadatos descriptivos de las tablas del pipeline en Unity Catalog. Cubre cuatro entregables interdependientes: (1) revisión exhaustiva de `SYSTEM.md` para eliminar ambigüedades y alinearlo con el código real de `src/`; (2) un plan documentado para actualizar las especificaciones (`.kiro/specs/`) a la realidad del código preservando historial de cambios y mejoras; (3) generación de los tres documentos oficiales en `docs/` (Modelado de Datos, Manual Técnico, Quickstart); y (4) creación del notebook `NbComentariosTablas.py` bajo `src/LSDP_Lab_DataVault_DWH/explorations/Metadata/` que aplica `COMMENT` vía `spark.sql` a todas las tablas (Streaming Tables, Materialized Views y Views) y a cada una de sus columnas.

El alcance es estrictamente documental y de metadatos: **no** modifica la lógica de transformación de los notebooks productivos en `transformations/` ni cambia el contrato de salida del pipeline LSDP. Las restricciones técnicas de Databricks Free Edition Serverless aplican plenamente al notebook de comentarios.

## Project Description (Input)

Necesito iniciar el nuevo incremento que según el archivo adjunto SYSTEM.md corresponde a:

- Revisión exhaustiva del archivo SYSTEM.md para eliminar ambigüedades y alinearlo con el código desarrollado para que esté consistente.
- Generar un Plan para actualizar el spec de la solución a la realidad del código desarrollado y dejarlos consistentes pero con historial de cambios y mejoras.
- Generar los archivos de la carpeta `docs`: Modelado de Datos, Manual Técnico y Quickstart.
- Generar el notebook `NbComentariosTablas.py` en `src/LSDP_Lab_DataVault_DWH/explorations/Metadata/` que aplique `COMMENT` a tablas y columnas vía `spark.sql`.

## Requirements

### Requirement 1: Revisión y Consolidación de SYSTEM.md

**Objective:** Como mantenedor del proyecto, quiero que `SYSTEM.md` esté libre de ambigüedades y consistente con el código real, para que el flujo cc-sdd produzca artefactos precisos sin contradicciones con la implementación.

#### Acceptance Criteria

1. The Documentation System shall mantener `SYSTEM.md` como Single Source of Truth, sincronizado con los notebooks de `src/LSDP_Lab_DataVault_DWH/transformations/` y `utilities/` existentes en el repositorio.
2. When se detecte una discrepancia entre `SYSTEM.md` y el código (nombres de tablas, decoradores LSDP, patrones de hash, parámetros del pipeline o estrategias de deduplicación), the Documentation System shall corregir el documento para reflejar la realidad del código y registrar el cambio en una sección de historial.
3. If una sección de `SYSTEM.md` describe estrategias contradictorias para una misma entidad (por ejemplo, `auto_cdc_flow` vs `append_flow`), the Documentation System shall conservar únicamente la estrategia implementada en el código y eliminar la alternativa obsoleta.
4. The Documentation System shall preservar la estructura de secciones obligatorias de `SYSTEM.md` (Propósito, Dinámica con cc-sdd, Stack Técnico, Modelo de Datos, Reglas, Research) declaradas como insumos para los slash commands `/kiro-steering` y `/kiro-spec-init`.
5. While se actualiza `SYSTEM.md`, the Documentation System shall mantener idioma español en todo el contenido nuevo y conservar las palabras clave EARS y nombres técnicos (decoradores, APIs LSDP) en su forma original en inglés.
6. The Documentation System shall incluir una sección "Historial de Cambios" en `SYSTEM.md` con entradas fechadas (ISO 8601) que enumeren las correcciones aplicadas en este incremento.

### Requirement 2: Plan de Alineación de Specs con el Código

**Objective:** Como gestor del flujo SDD, quiero un plan formal que actualice los specs existentes a la realidad del código preservando trazabilidad histórica, para que cualquier nuevo incremento parta de una base coherente sin perder el registro de decisiones previas.

#### Acceptance Criteria

1. The Documentation System shall generar un Plan de Alineación que cubra los specs existentes en `.kiro/specs/` (`bronce-utilities-ingesta`, `correccion-arquitectura-bronce-plata`, `oro-modelo-estrella-mv-tiempo`, `plata-data-vault-notebooks`).
2. When el plan identifique divergencias entre `requirements.md`/`design.md`/`tasks.md` de un spec y el código actual, the Documentation System shall registrar la divergencia con: ruta del archivo, descripción del desajuste, impacto y acción correctiva propuesta.
3. The Documentation System shall preservar las versiones originales de los artefactos (`requirements.md`, `design.md`, `tasks.md`) sin sobrescribirlas, anexando un changelog o sección de historial dentro de cada spec en lugar de borrarlas.
4. If un spec ya está marcado `ready_for_implementation: true` y el código se desvió de su diseño, the Documentation System shall documentar el desvío como "decisión post-implementación" sin invalidar las aprobaciones históricas.
5. The Documentation System shall entregar el plan como documento navegable (Markdown) ubicado dentro del propio spec activo (`.kiro/specs/documentacion-consolidada-y-metadata/`), no en los specs históricos.
6. Where existan correcciones o mejoras aplicadas al código no reflejadas en specs (p. ej. OPT-001, correcciones B.1/B.2 de `procesar_satellite_transaccional`), the Documentation System shall enumerarlas explícitamente en el plan con referencia al notebook involucrado.

### Requirement 3: Documento de Modelado de Datos (`docs/ModeloDatos.md`)

**Objective:** Como analista o ingeniero de datos, quiero un documento único que describa exhaustivamente todos los modelos de datos del laboratorio, para entender entidades, columnas, relaciones y linaje sin leer el código fuente.

#### Acceptance Criteria

1. The Documentation System shall producir el archivo `docs/ModeloDatos.md` cubriendo las tres medallas: Bronce (Streaming Tables), Plata (Data Vault 2.0 Raw Vault) y Oro (Modelo Estrella).
2. The Documentation System shall incluir, por cada tabla del pipeline (todas las definidas en `src/LSDP_Lab_DataVault_DWH/transformations/`), un catálogo completo de columnas con **todos los campos sin excepción** (sin resúmenes ni muestras parciales): nombre de columna, tipo de dato, descripción de negocio, origen y, cuando aplique, indicador de llave (PK, FK, hash, columna técnica de Data Vault). Las Streaming Tables de Bronce deben documentar la totalidad de columnas AS400 inferidas por AutoLoader más las columnas de infraestructura (particiones físicas y `_rescued_data`).
3. The Documentation System shall presentar un diagrama relacional por medalla (Bronce, Plata, Oro) usando Mermaid `erDiagram`, mostrando llaves y cardinalidades entre Hubs, Links y Satellites en Plata, y entre Dimensiones y Hecho en Oro.
4. The Documentation System shall presentar un diagrama de linaje macro (Mermaid `flowchart`) que muestre el flujo Bronce → Plata (Hubs/Links/Satellites) → Oro (Dimensiones/Hecho), sin necesidad de detallar columnas en este diagrama.
5. When una tabla en Plata sea Hub, Link o Satellite, the Documentation System shall identificar explícitamente su tipo Data Vault, su patrón LSDP (`auto_cdc_flow` vs `append_flow`) y las columnas técnicas obligatorias (`Hash_*`, `FechaRegistro`, `FuenteDatos`, `Hash_Diferenciador` cuando aplique).
6. The Documentation System shall describir las dimensiones y la tabla de hechos de Oro indicando granularidad, llaves subrogadas, columnas degeneradas y origen en Plata.
7. The Documentation System shall escribir el documento en español con estilo de catálogo técnico, listo para ser navegado por desarrolladores y stakeholders.

### Requirement 4: Manual Técnico (`docs/ManualTecnico.md`)

**Objective:** Como ingeniero de datos que mantiene o extiende el pipeline, quiero un manual técnico explicativo, para entender por qué el LSDP se comporta como lo hace y aplicar las mismas estrategias en nuevas entidades.

#### Acceptance Criteria

1. The Documentation System shall producir el archivo `docs/ManualTecnico.md` con estilo explicativo y didáctico, alineado a prácticas de documentación técnica de primer mundo (estructura jerárquica, ejemplos de código, justificaciones de diseño).
2. The Documentation System shall explicar el uso de `dp.create_auto_cdc_flow(stored_as_scd_type=1)` para Hub_Cliente, Hub_Operacion y Link_Cliente_Operacion, incluyendo: motivación (deduplicación cross-batch O(delta) gestionada por motor), restricciones (`except_column_list` no se usa) y el patrón `@dp.view` que alimenta el flujo.
3. The Documentation System shall explicar el uso de `@dp.append_flow()` con `procesar_hub()`, `procesar_link()`, `procesar_satellite()` y `procesar_satellite_transaccional()`, incluyendo cuándo aplicar cada uno y por qué `procesar_satellite_transaccional()` deduplica solo por `hash_col` (primera-escritura-gana, corrección B.1).
4. The Documentation System shall explicar los conceptos de Streaming Table temporal (`@dp.table(temporary=True)`) y Vista Materializada temporal (`@dp.materialized_view(temporary=True)`) usados en el pipeline, indicando dónde se aplican, su justificación técnica y su efecto en Unity Catalog.
5. The Documentation System shall enumerar las propiedades obligatorias que deben configurarse en cada Streaming Table y cada Materialized View (por ejemplo, `name` en formato 3 partes, `cluster_by` con `FechaRegistro`, ausencia de `catalog=`/`schema=` separados, `temporary` cuando aplique), sustentadas en código real del repo.
6. The Documentation System shall documentar las restricciones de Serverless aplicables (prohibición de `cache`, `persist`, RDD, UDFs, threading, `spark.sparkContext`) y las reglas ANSI Mode (cast a `long` antes de `abs(F.hash(...))`, uso de `F.concat_ws` en lugar de `+` para strings).
7. The Documentation System shall incluir una sección de patrones de hash (SHA2-256 para Hubs/Links, SHA2-512 para `Hash_Diferenciador`, separador `|`) con ejemplos extraídos de `LSDPUtilidadPrincipal.py`.
8. The Documentation System shall escribirse en español con tono explicativo, evitando descripciones meramente enumerativas y privilegiando el "por qué" técnico de cada decisión.

### Requirement 5: Quickstart (`docs/Quickstart.md`)

**Objective:** Como nuevo usuario que acaba de clonar el repositorio, quiero una guía paso a paso, para llevar el proyecto a Databricks Free Edition y ejecutar el pipeline completo sin necesitar contexto adicional.

#### Acceptance Criteria

1. The Documentation System shall producir el archivo `docs/Quickstart.md` con una secuencia ordenada de pasos numerados desde el clon del repositorio hasta la ejecución exitosa del pipeline LSDP.
2. The Documentation System shall describir el proceso de carga del repositorio en Databricks Free Edition creando un Git Folder directamente en el directorio del workspace del usuario (la sección "Repos" es obsoleta y no debe mencionarse), junto con la configuración de Unity Catalog y la creación del catálogo y esquemas Bronce/Plata/Oro.
3. The Documentation System shall instruir como primer paso de ejecución el notebook `NbConfiguracionInicial.py` documentando **todos** sus parámetros de entrada (widgets) con una tabla que incluya: nombre del widget, valor por defecto y descripción del propósito; con justificación de cuándo cambiarlo.
4. The Documentation System shall instruir como segundo paso de ejecución el notebook `NbGenerarMaestroCliente.py` con una tabla que documente **todos** sus 13 widgets (incluyendo `porcentajeMutacion`, `porcentajeNuevos`, `camposMutacion`, `montoMinimo`, `montoMaximo`, `numeroParticiones`, `shufflePartitions`, `rutaMaestroClienteExistente`) con nombre, valor por defecto y descripción de propósito.
5. When los notebooks `NbGenerarSaldosCliente.py` y `NbGenerarTransaccionalCliente.py` puedan ejecutarse en paralelo, the Documentation System shall indicarlo explícitamente como tercer paso, listando **todos** los widgets de cada notebook (9 y 12 respectivamente) en tablas individuales con nombre, valor por defecto y descripción; resaltando que `fechaTransaccion` en `NbGenerarTransaccionalCliente.py` es obligatorio sin valor por defecto.
6. The Documentation System shall instruir como cuarto paso la configuración del pipeline LSDP, incluyendo: edición del archivo de configuración del pipeline (JSON/UI), declaración de los 13 parámetros obligatorios (`pipeline.catalogo`, `pipeline.esquema`, `pipeline.volumen`, `pipeline.catalogo_plata`, `pipeline.esquema_plata`, `pipeline.catalogo_oro`, `pipeline.esquema_oro`, `pipeline.ruta_cmstfl`, `pipeline.ruta_trxpfl`, `pipeline.ruta_blncfl`, `pipeline.schema_location_cmstfl`, `pipeline.schema_location_trxpfl`, `pipeline.schema_location_blncfl`), selección de notebooks de `transformations/` como bibliotecas y modo Serverless.
7. The Documentation System shall instruir como paso final la ejecución del pipeline LSDP en modo "Run" y los criterios de verificación de éxito (tablas creadas en cada medalla, métricas de Expectations).
8. If un paso requiere requisitos previos (cuenta Databricks Free Edition, permisos sobre Unity Catalog, archivos Parquet en Volumen), the Documentation System shall declararlos explícitamente al inicio de la guía como "Prerrequisitos".
9. The Documentation System shall escribir la guía en español con un nivel de detalle apto para usuarios que no conocen el proyecto previamente.

### Requirement 6: Notebook `NbComentariosTablas.py` para Metadatos en Unity Catalog

**Objective:** Como administrador de Unity Catalog, quiero un notebook que aplique comentarios consistentes a tablas y columnas, para que el catálogo exponga descripciones de negocio sin depender de leer código fuente.

#### Acceptance Criteria

1. The Documentation System shall crear el archivo `src/LSDP_Lab_DataVault_DWH/explorations/Metadata/NbComentariosTablas.py` (nuevo directorio `Metadata` bajo `explorations/`).
2. The NbComentariosTablas Notebook shall ejecutar comentarios usando exclusivamente `spark.sql("COMMENT ON TABLE ...")` y `spark.sql("ALTER TABLE ... ALTER COLUMN ... COMMENT ...")` (o `ALTER VIEW` cuando aplique), sin usar APIs no soportadas en Serverless.
3. The NbComentariosTablas Notebook shall cubrir todas las tablas y vistas materializables del pipeline LSDP que existen en Unity Catalog: Streaming Tables de Bronce (`CMSTFL`, `TRXPFL`, `BLNCFL`), todas las entidades Data Vault de Plata (Hubs, Links, Satellites) y todas las entidades de Oro (Dimensiones, Hecho y vistas si las hubiera).
4. The NbComentariosTablas Notebook shall obtener catálogo y esquema de cada medalla mediante `dbutils.widgets` o `spark.conf.get(...)`, sin valores hard-coded, alineado al patrón del resto del proyecto.
5. When una columna sea técnica de Data Vault (`Hash_*`, `FechaRegistro`, `FuenteDatos`, `Hash_Diferenciador`), the NbComentariosTablas Notebook shall aplicar un comentario estandarizado que explique su propósito (llave hash, fecha de carga, origen, hash de cambio).
6. When una columna sea de negocio (por ejemplo, `CUSTID`, `BLSQ`, `TRXID`, atributos de Maestro/Saldos/Transaccional), the NbComentariosTablas Notebook shall aplicar un comentario derivado del catálogo definido en `docs/ModeloDatos.md` (Requirement 3).
7. If una tabla destino no existe al momento de ejecutarse el notebook, the NbComentariosTablas Notebook shall capturar la excepción, registrar advertencia con el nombre de la tabla y continuar con la siguiente, sin abortar la ejecución completa.
8. The NbComentariosTablas Notebook shall ser idempotente: ejecuciones repetidas producen el mismo estado de comentarios sin acumulación ni errores por comentarios preexistentes.
9. The NbComentariosTablas Notebook shall respetar las restricciones Serverless (no `cache`, no RDD, no UDFs, no `spark.sparkContext`) y no requerir clúster dedicado para ejecutarse.
10. The NbComentariosTablas Notebook shall organizarse en celdas con encabezados Markdown por medalla (Bronce, Plata, Oro) para facilitar la ejecución parcial y la lectura.

### Requirement 7: Calidad Transversal y Trazabilidad de Documentación

**Objective:** Como revisor de calidad, quiero criterios transversales aplicables a todos los entregables, para garantizar coherencia, idioma y trazabilidad cruzada entre documentos y código.

#### Acceptance Criteria

1. The Documentation System shall escribir todos los archivos Markdown de este incremento (`SYSTEM.md`, `docs/*.md`, plan de alineación, `requirements.md`, `design.md`, `tasks.md`) en español, manteniendo en inglés solo nombres de APIs, decoradores y palabras clave EARS.
2. The Documentation System shall garantizar que cualquier nombre de tabla, columna, decorador o parámetro mencionado en la documentación exista en el código fuente actual del repositorio.
3. When un documento haga referencia a un notebook o utilidad, the Documentation System shall enlazarlo con ruta relativa al repo (por ejemplo `src/LSDP_Lab_DataVault_DWH/utilities/LSDPUtilidadPrincipal.py`).
4. If un entregable depende de otro (por ejemplo, comentarios del notebook `NbComentariosTablas.py` derivan del catálogo en `docs/ModeloDatos.md`), the Documentation System shall declarar la dependencia explícita en ambos documentos.
5. The Documentation System shall verificar que ningún documento contradiga las restricciones de Serverless ni los patrones declarados en `.kiro/steering/tech.md`.
6. Where existan diagramas Mermaid, the Documentation System shall validar la sintaxis (sin errores de parsing) y limitar su tamaño para que sean legibles en un visor Markdown estándar de GitHub.
7. The Documentation System shall registrar en cada documento generado un encabezado con: título, autor (proyecto), fecha de última actualización (ISO 8601) y referencia al spec `documentacion-consolidada-y-metadata`.
