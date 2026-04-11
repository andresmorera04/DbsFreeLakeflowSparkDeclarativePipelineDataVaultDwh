# Visión General del Producto

Laboratorio avanzado de ingeniería de datos que construye un **Data Warehouse** end-to-end sobre **Databricks Free Edition** con cómputo Serverless. Demuestra la ingesta incremental de archivos Parquet (Landing Zone) hasta la exposición de un **Modelo Estrella** listo para consumo analítico, utilizando **Lakeflow Spark Declarative Pipelines (LSDP)** como motor de orquestación.

## Capacidades Principales

1. **Ingesta incremental** con AutoLoader (Medalla de Bronce) — detección automática de archivos nuevos sin reprocesar anteriores.
2. **Modelado Data Vault 2.0 (Raw Vault)** en Medalla de Plata — Hubs, Links y Satellites con procesamiento Append Only y detección de cambios (CDC lógico).
3. **Modelo Estrella dimensional** en Medalla de Oro — dimensiones Tipo 1 y tabla de hechos para análisis transaccional en ATMs.
4. **Parametrización completa** — cero valores hard-coded; todo se configura vía parámetros del pipeline LSDP.
5. **Compatibilidad total** con Databricks Free Edition Serverless Compute.

## Caso de Uso Principal

El área de negocio de Clientes de una entidad bancaria necesita un producto de datos analítico que permita:

- Monitorear el **comportamiento transaccional en cajeros automáticos (ATMs)**: retiros (DATM) y depósitos (CATM).
- Analizar la **evolución y estado de saldos** por cliente, cuenta y período.
- Responder preguntas de negocio: cantidad y monto de transacciones por cliente, distribución por segmento, comportamientos atípicos.

## Propuesta de Valor

- **Arquitectura Medallón de 3 capas** (Bronce → Plata → Oro) con linaje completo en Unity Catalog.
- **Data Vault 2.0** como capa de integración — separación de llaves de negocio (Hubs), relaciones (Links) y atributos por tasa de cambio (Satellites).
- **Reproducible y educativo** — sirve como laboratorio de referencia para patrones de ingeniería de datos en Databricks Free Edition.

## Dominio de Datos

Tres fuentes de datos Parquet provenientes de un sistema AS400 bancario:

| Fuente | Entidad | Registros | Llave Primaria |
|--------|---------|-----------|----------------|
| **CMSTFL** | Maestro de Clientes | 4,000,000 | `CUSTID` |
| **TRXPFL** | Transacciones | 7,000,000 | `TRXID` |
| **BLNCFL** | Saldos/Operaciones | 4,000,000 | `CUSTID` + `BLSQ` |

---
_Enfocado en patrones y propósito, no en listas exhaustivas de features._
