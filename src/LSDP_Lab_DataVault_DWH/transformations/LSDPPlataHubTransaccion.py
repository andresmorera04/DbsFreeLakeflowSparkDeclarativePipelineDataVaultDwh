# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataHubTransaccion — Hub: Llave de negocio de Transacción
# ---------------------------------------------------------------------------
# Streaming Table: Hub_Transaccion
# Fuente: {catalogo}.{esquema}.TRXPFL  →  Plata: Hub_Transaccion
# TRXID es StringType nativo — sin cast adicional necesario
# Deduplicación incremental: LEFT ANTI JOIN por IdentificadorTransaccion
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadPrincipal import calcular_hash_hub, procesar_hub, reordenar_columnas_lc

config = obtener_configuracion(spark)

_catalogo_plata = config["catalogo_plata"]
_esquema_plata = config["esquema_plata"]

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Hub_Transaccion",
    cluster_by=["FechaRegistro", "Hash_Transaccion"],
    expect_all_or_fail={
        "id_transaccion_no_nulo": "IdentificadorTransaccion IS NOT NULL",
        "hash_transaccion_no_nulo": "Hash_Transaccion IS NOT NULL",
    },
)


@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Hub_Transaccion")
def hub_transaccion_flow():
    fuente = f"{config['catalogo']}.{config['esquema']}.TRXPFL"
    df = dp.read_stream(fuente)
    datos = (
        df.select(
            F.current_timestamp().alias("FechaRegistro"),
            calcular_hash_hub([F.col("TRXID")]).alias("Hash_Transaccion"),
            F.col("TRXID").alias("IdentificadorTransaccion"),
            F.lit(fuente).alias("FuenteDatos"),
        )
    )
    resultado = procesar_hub(
        spark,
        _catalogo_plata,
        _esquema_plata,
        "Hub_Transaccion",
        ["IdentificadorTransaccion"],
        datos,
    )
    return reordenar_columnas_lc(resultado, ["FechaRegistro", "Hash_Transaccion"])
