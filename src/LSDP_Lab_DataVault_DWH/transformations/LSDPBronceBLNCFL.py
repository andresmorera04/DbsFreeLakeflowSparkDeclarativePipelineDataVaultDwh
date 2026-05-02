# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPBronceBLNCFL — Ingesta Bronce: Saldos/Operaciones (BLNCFL)
# ---------------------------------------------------------------------------
# Streaming Table persistente (AutoLoader directo)
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadPrincipal import reordenar_columnas_lc

config = obtener_configuracion(spark)


@dp.table(
    name=f"{config['catalogo']}.{config['esquema']}.BLNCFL",
    cluster_by=["FechaRegistroParquet"],
)
def blncfl():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", config["schema_location_blncfl"])
        .load(config["ruta_blncfl"])
        # _rescued_data es generada automáticamente por AutoLoader
        .withColumn(
            "FechaRegistroParquet",
            F.to_date(F.concat_ws("-", F.col("año"), F.col("mes"), F.col("dia"))),
        )
    )
    return reordenar_columnas_lc(df, ["FechaRegistroParquet"])
