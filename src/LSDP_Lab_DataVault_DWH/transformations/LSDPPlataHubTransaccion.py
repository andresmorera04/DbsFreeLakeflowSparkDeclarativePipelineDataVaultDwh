# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataHubTransaccion — Hub: Llave de negocio de Transacción
# ---------------------------------------------------------------------------
# Materialized View: Hub_Transaccion
# Fuente: {catalogo}.{esquema}.TRXPFL  →  Plata: Hub_Transaccion
# TRXID es StringType nativo — sin cast adicional necesario
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadPrincipal import calcular_hash_hub, reordenar_columnas_lc

config = obtener_configuracion(spark)


@dp.expect_or_fail("id_transaccion_no_nulo", "IdentificadorTransaccion IS NOT NULL")
@dp.expect_or_fail("hash_transaccion_no_nulo", "Hash_Transaccion IS NOT NULL")
@dp.materialized_view(
    name=f"{config['catalogo_plata']}.{config['esquema_plata']}.Hub_Transaccion",
    cluster_by=["FechaRegistro", "Hash_Transaccion"],
)
def hub_transaccion():
    fuente = f"{config['catalogo']}.{config['esquema']}.TRXPFL"
    df = spark.read.table(fuente)
    resultado = (
        df.select(
            F.current_timestamp().alias("FechaRegistro"),
            calcular_hash_hub([F.col("TRXID")]).alias("Hash_Transaccion"),
            F.col("TRXID").alias("IdentificadorTransaccion"),
            F.lit(fuente).alias("FuenteDatos"),
        )
        .dropDuplicates(["IdentificadorTransaccion"])
    )
    return reordenar_columnas_lc(resultado, ["FechaRegistro", "Hash_Transaccion"])
