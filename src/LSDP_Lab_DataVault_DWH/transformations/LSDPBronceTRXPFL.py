# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPBronceTRXPFL — Ingesta Bronce: Transacciones (TRXPFL)
# ---------------------------------------------------------------------------
# Streaming Table temporal (AutoLoader) + Materialized View snapshot
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion

config = obtener_configuracion(spark)


@dp.table(
    name="TRXPFL_temp",
    temporary=True,
    cluster_by=["FechaRegistroParquet"],
)
def trxpfl_temp():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", config["schema_location_trxpfl"])
        .load(config["ruta_trxpfl"])
        # _rescued_data es generada automáticamente por AutoLoader
        .withColumn(
            "FechaRegistroParquet",
            F.to_date(F.concat_ws("-", F.col("año"), F.col("mes"), F.col("dia"))),
        )
    )


@dp.materialized_view(
    name=f"{config['catalogo']}.{config['esquema']}.TRXPFL",
    cluster_by=["FechaRegistroParquet"],
)
def trxpfl():
    df = spark.read.table("TRXPFL_temp")
    max_fecha = df.select(F.max("FechaRegistroParquet").alias("max_fecha"))
    return (
        df.join(F.broadcast(max_fecha), df["FechaRegistroParquet"] == max_fecha["max_fecha"])
        .drop("max_fecha")
    )
