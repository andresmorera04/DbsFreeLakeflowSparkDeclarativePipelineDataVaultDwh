# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataVistaTRXPFLCDF — Vista compartida sobre Change Data Feed de TRXPFL
# ---------------------------------------------------------------------------
# @dp.view (no materializada) que expone TRXPFL leído por Change Data Feed.
# Consumidores: Hub_Transaccion, Link_Cliente_Transaccion, Sat_Transaccion_*.
#
# Beneficios respecto a leer TRXPFL directamente con dp.read_stream:
#   1. Una sola lectura del CDF planificada en común para los 4 consumidores
#      (vs. 4 DeltaSource[TRXPFL] independientes).
#   2. Cada microbatch entrega únicamente las filas del commit nuevo, no el
#      snapshot completo. En cargas incrementales se evitan rescaneos.
#   3. Las columnas técnicas _commit_version y _commit_timestamp se promueven
#      a VersionCarga y FechaCargaBronce, aportando trazabilidad end-to-end
#      a los Satellites Data Vault.
#   4. El filtro _change_type IN ('insert','update_postimage') deja el camino
#      abierto para futuras correcciones por upsert sin afectar este lab,
#      donde TRXPFL es append-only.
#
# Premisa del dominio: TRXID es globalmente único entre ejecuciones, por lo
# que la deduplicación cross-batch (anti-join contra el destino) es
# estructuralmente innecesaria en el linaje transaccional.
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion

config = obtener_configuracion(spark)

_fuente = f"{config['catalogo']}.{config['esquema']}.TRXPFL"


@dp.view(name="vista_trxpfl_cdf")
def vista_trxpfl_cdf():
    """Vista streaming sobre el Change Data Feed de TRXPFL.

    Filtra solo eventos de inserción / post-imagen de update y promueve las
    columnas técnicas del CDF (`_commit_version`, `_commit_timestamp`) a
    columnas de negocio (`VersionCarga`, `FechaCargaBronce`) para que los
    Satellites las puedan auditar.
    """
    df = (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(_fuente)
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
    )
    df = (
        df
        .withColumn("VersionCarga", F.col("_commit_version"))
        .withColumn("FechaCargaBronce", F.col("_commit_timestamp"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )
    return df
