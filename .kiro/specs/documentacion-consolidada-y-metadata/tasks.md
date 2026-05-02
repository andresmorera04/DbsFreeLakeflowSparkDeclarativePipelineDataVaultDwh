# Implementation Plan

- [x] 1. Preparar inventario de verdad del código y parámetros
- [x] 1.1 Levantar inventario verificable de tablas, columnas, decoradores y parámetros usados por el pipeline
  - Identificar todas las entidades Bronce, Plata y Oro declaradas en las transformaciones actuales.
  - Confirmar nombres de columnas, llaves hash, columnas técnicas Data Vault y columnas derivadas de Oro.
  - Confirmar los 13 parámetros operativos del pipeline y cualquier parámetro adicional usado por notebooks auxiliares.
  - Registrar patrones reales de deduplicación, temporalidad LSDP, expectations, clustering y restricciones Serverless observadas.
  - _Requirements: 1.1, 1.2, 1.3, 2.2, 2.6, 3.2, 3.5, 3.6, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 5.3, 5.4, 5.5, 5.6, 7.2, 7.5_

- [x] 1.2 Validar el inventario contra steering y specs históricos antes de generar entregables
  - Contrastar el inventario con las reglas vigentes de producto, estructura y tecnología.
  - Identificar divergencias relevantes entre specs históricos y comportamiento implementado.
  - Separar hallazgos que actualizan documentación vigente de decisiones post-implementación históricas.
  - _Requirements: 1.1, 1.2, 1.4, 2.1, 2.2, 2.4, 2.6, 7.2, 7.5_

- [x] 2. Consolidar fuente de verdad y trazabilidad SDD
- [x] 2.1 Actualizar SYSTEM.md como referencia consolidada del proyecto
  - Corregir discrepancias de nombres, decoradores, hashes, parámetros y estrategias de deduplicación según el inventario validado.
  - Eliminar contradicciones internas conservando sólo la estrategia realmente implementada por entidad.
  - Preservar las secciones obligatorias usadas por el flujo cc-sdd.
  - Añadir historial de cambios fechado en formato ISO 8601 con las correcciones aplicadas en este incremento.
  - Mantener español técnico y conservar nombres de APIs, decoradores y palabras clave en inglés.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 7.1, 7.2, 7.5, 7.7_

- [x] 2.2 (P) Crear el Plan de Alineación de specs con el código real
  - Cubrir los cuatro specs históricos definidos para el incremento.
  - Registrar por divergencia el artefacto afectado, descripción, impacto y acción correctiva propuesta.
  - Documentar mejoras no reflejadas históricamente como OPT-001 y correcciones B.1/B.2.
  - Ubicar el plan dentro del spec activo y hacerlo navegable para futuras revisiones.
  - _Requirements: 2.1, 2.2, 2.5, 2.6, 7.1, 7.2, 7.3, 7.7_

- [x] 2.3 Anexar changelog a cada spec histórico sin sobrescribir artefactos aprobados
  - Crear una entrada de trazabilidad por spec histórico con fecha ISO 8601 y referencia al plan activo.
  - Registrar decisiones post-implementación sin invalidar aprobaciones previas.
  - Confirmar que requirements, design y tasks históricos permanecen preservados.
  - _Requirements: 2.3, 2.4, 7.1, 7.3, 7.7_

- [x] 3. Construir el catálogo técnico de modelo de datos
- [x] 3.1 Generar el catálogo exhaustivo de tablas y columnas por medalla
  - Documentar Bronce, Plata y Oro con descripción de negocio, origen, tipo de dato e indicador de llave o hash.
  - **Todos los campos sin excepción**: las Streaming Tables de Bronce incluyen la totalidad de columnas AS400 (CMSTFL: 75 cols, TRXPFL: 65 cols, BLNCFL: 105 cols), sin resúmenes ni muestras parciales.
  - Los Satellites de Plata tienen tablas individuales completas: Sat_Cliente (4: 17+19+23+28 cols), Sat_Operacion (3: 36+38+23 cols), Sat_Transaccion (2: 37+38 cols).
  - La vista `vista_trxpfl_cdf` (67 cols) tiene sección propia (3.9) documentando todas sus columnas heredadas de TRXPFL más las de trazabilidad CDF.
  - Los datasets auxiliares temporales de Oro (Trx_ATM_Stream: 15 cols, Map_Cliente_Operacion_Dominante: 4 cols) tienen catálogos completos en sección 4.5.
  - Identificar tipo Data Vault, patrón LSDP y columnas técnicas obligatorias para Hubs, Links y Satellites.
  - Describir dimensiones y hechos de Oro con granularidad, llaves subrogadas, columnas degeneradas y origen en Plata.
  - Escribir el catálogo en español con estilo técnico navegable.
  - _Requirements: 3.1, 3.2, 3.5, 3.6, 3.7, 7.1, 7.2, 7.3, 7.7_

- [x] 3.2 Completar diagramas Mermaid y linaje macro del modelo
  - Crear un diagrama relacional independiente por medalla para mantener legibilidad.
  - Representar relaciones clave de Hubs, Links y Satellites en Plata y relaciones Dimensión-Hecho en Oro.
  - Crear un flowchart macro Bronce-Plata-Oro separado de los diagramas relacionales.
  - Validar que la sintaxis Mermaid sea renderizable en un visor Markdown estándar.
  - _Requirements: 3.3, 3.4, 7.6_

- [x] 3.3 Declarar sincronización entre catálogo documental y comentarios de Unity Catalog
  - Incluir dependencia explícita entre el catálogo de modelo de datos y el notebook de comentarios.
  - Definir cómo se revisará la paridad entre tablas documentadas y diccionarios de comentarios.
  - Preparar la lista base de tablas que alimentará la celda de validación de paridad.
  - _Requirements: 3.2, 6.5, 6.6, 7.4_

- [x] 4. Elaborar documentación operativa y técnica del laboratorio
- [x] 4.1 (P) Crear el Manual Técnico explicativo del pipeline LSDP
  - Explicar la arquitectura medallón y las decisiones técnicas del laboratorio con foco en el porqué.
  - Cubrir AUTO CDC SCD=1, append_flow y helpers de Data Vault con criterios de selección.
  - Documentar temporalidad de Streaming Tables y Materialized Views, propiedades obligatorias y decisiones de Oro.
  - Incluir restricciones Serverless, reglas ANSI Mode y patrones de hash con ejemplos sustentados en el código real.
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 7.1, 7.2, 7.3, 7.5, 7.7_

- [x] 4.2 (P) Crear el Quickstart reproducible para Databricks Free Edition
  - Describir prerrequisitos, Git Folder en el directorio del workspace del usuario y configuración de Unity Catalog sin mencionar la sección obsoleta Repos.
  - Documentar ejecución de notebooks generadores, parámetros, valores por defecto y posibilidades de paralelismo.
  - Instruir configuración LSDP con los 13 parámetros obligatorios, bibliotecas de transformaciones y modo Serverless.
  - Definir criterios de verificación de ejecución exitosa y última verificación en formato ISO 8601.
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 7.1, 7.2, 7.3, 7.5, 7.7_

- [x] 4.3 Integrar enlaces cruzados entre documentación principal y artefactos del proyecto
  - Enlazar documentos, notebooks y utilidades con rutas relativas al repositorio.
  - Asegurar que cada documento declare dependencias relevantes con otros entregables del incremento.
  - Añadir encabezado estándar con título, autor del proyecto, fecha ISO 8601 y referencia al spec activo.
  - _Requirements: 7.3, 7.4, 7.7_

- [x] 5. Implementar el notebook de metadatos en Unity Catalog
- [x] 5.1 Crear estructura y parametrización standalone del notebook de comentarios
  - Crear el notebook auxiliar de Metadata organizado en celdas por portada, parámetros, diccionarios, helper, medallas y resumen.
  - Definir exclusivamente widgets para catálogos, esquemas y rutas necesarias; no usar configuración del motor LSDP.
  - Validar parámetros no vacíos antes de ejecutar sentencias de comentario.
  - Incluir nota visible de actualización conjunta con el catálogo de modelo de datos.
  - _Requirements: 6.1, 6.4, 6.9, 6.10, 7.1, 7.4, 7.5, 7.7_

- [x] 5.2 Construir diccionarios de comentarios de tablas y columnas para todas las medallas
  - Cubrir Bronce, todas las entidades Data Vault de Plata y entidades de Oro materializables en Unity Catalog.
  - Reutilizar comentarios estandarizados para columnas técnicas Data Vault.
  - Derivar comentarios de negocio desde el catálogo de modelo de datos.
  - Mantener segmentación por medalla para facilitar revisión parcial.
  - _Requirements: 6.3, 6.5, 6.6, 7.2, 7.4_

- [x] 5.3 Implementar aplicación idempotente de comentarios y manejo tolerante de errores
  - Aplicar comentarios de tabla y columna exclusivamente con sentencias Spark SQL soportadas.
  - Escapar literales de texto antes de construir SQL dinámico.
  - Continuar ante tablas inexistentes, columnas faltantes o permisos insuficientes registrando estado por tabla.
  - Aplicar fallback para vistas o materialized views antes de marcar columnas como omitidas.
  - _Requirements: 6.2, 6.7, 6.8, 6.9, 7.5_

- [x] 5.4 Añadir validación ejecutable de paridad y resumen final del notebook
  - Comparar las tablas documentadas en el modelo de datos contra las claves de comentarios antes de ejecutar comentarios.
  - Fallar con un mensaje claro si existen tablas documentadas sin comentario o comentarios sin tabla documentada.
  - Presentar resumen final con medalla, tabla, estado de tabla, columnas correctas, columnas omitidas y mensaje.
  - _Requirements: 6.3, 6.6, 6.7, 7.4_

- [x] 6. Validar consistencia transversal de documentación y metadatos
- [x] 6.1 Verificar nombres, parámetros y restricciones contra el código actual
  - Comprobar que tablas, columnas, decoradores y parámetros mencionados existan en el código o estén justificados como entregable nuevo.
  - Confirmar que ningún documento contradiga restricciones Serverless ni patrones de steering.
  - Validar que Quickstart documente todos los parámetros usados por notebooks y pipeline LSDP.
  - _Requirements: 5.3, 5.4, 5.5, 5.6, 7.2, 7.5_

- [x] 6.2 Validar renderizado, idioma y encabezados de todos los artefactos Markdown
  - Confirmar español en todos los entregables y conservación de APIs y palabras clave técnicas en inglés.
  - Renderizar o revisar sintaxis Mermaid de cada diagrama.
  - Verificar encabezados estandarizados con fecha ISO 8601 y referencia al spec activo.
  - _Requirements: 3.3, 3.4, 7.1, 7.6, 7.7_

- [x] 6.3 Validar comportamiento esperado del notebook de comentarios
  - Ejecutar revisión Serverless para descartar APIs prohibidas como cache, persist, RDD, UDFs o sparkContext.
  - Probar escenario de tablas inexistentes y confirmar finalización con estados SKIPPED controlados.
  - Probar idempotencia mediante ejecuciones repetidas cuando exista un catálogo de prueba disponible.
  - Confirmar fallback de comentarios de columna sobre al menos un objeto de Oro compatible con la prueba.
  - _Requirements: 6.2, 6.7, 6.8, 6.9, 7.5_

- [x] 7. Integrar entregables y cerrar trazabilidad del incremento
- [x] 7.1 Consolidar revisión cruzada final entre SYSTEM.md, docs, plan, changelogs y notebook
  - Confirmar que las dependencias documentadas sean bidireccionales cuando un entregable alimente a otro.
  - Verificar que el plan de alineación y changelogs no sobrescriban historial aprobado.
  - Revisar que el alcance se mantenga documental y de metadatos sin modificar lógica productiva de transformaciones.
  - _Requirements: 1.1, 2.3, 2.4, 7.3, 7.4, 7.5_

- [x] 7.2 Actualizar referencias de entrada para que los usuarios encuentren la nueva documentación
  - Incorporar enlaces desde la documentación raíz hacia los documentos generados y el Quickstart.
  - Confirmar que las rutas relativas sean navegables dentro del repositorio.
  - Mantener consistencia con la estructura existente del proyecto.
  - _Requirements: 5.1, 7.3_

- [x] 7.3 Realizar chequeo de cobertura completa de requisitos antes de aprobar tareas
  - Revisar que cada criterio de aceptación de los requisitos 1 a 7 tenga al menos una tarea asociada.
  - Confirmar que las validaciones cubran documentación, metadatos, trazabilidad, Serverless y Mermaid.
  - Registrar cualquier exclusión justificada antes de pasar a implementación.
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9, 6.10, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7_