# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataHubTransaccion — Hub: Llave de negocio de Transacción
# ---------------------------------------------------------------------------
# Streaming Table: Hub_Transaccion
# Fuente: vista_trxpfl_cdf  →  Plata: Hub_Transaccion
# TRXID es StringType nativo — sin cast adicional necesario
# Append-Only puro: TRXID es globalmente único entre ejecuciones; el origen
# es la vista CDF compartida (LSDPPlataVistaTRXPFLCDF), que entrega solo
# los eventos del último commit. No se requiere LEFT ANTI JOIN cross-batch.
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadPrincipal import calcular_hash_hub, reordenar_columnas_lc

config = obtener_configuracion(spark)

_catalogo_plata = config["catalogo_plata"]
_esquema_plata = config["esquema_plata"]

_PROP_TABLE = {
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days",
}

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Hub_Transaccion",
    cluster_by=["FechaRegistro", "Hash_Transaccion"],
    expect_all_or_fail={
        "id_transaccion_no_nulo": "IdentificadorTransaccion IS NOT NULL",
        "hash_transaccion_no_nulo": "Hash_Transaccion IS NOT NULL",
    },
    table_properties=_PROP_TABLE,
)


@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Hub_Transaccion")
def hub_transaccion_flow():
    fuente = f"{config['catalogo']}.{config['esquema']}.TRXPFL"
    df = dp.read_stream("vista_trxpfl_cdf")
    datos = (
        df.select(
            F.current_timestamp().alias("FechaRegistro"),
            calcular_hash_hub([F.col("TRXID")]).alias("Hash_Transaccion"),
            F.col("TRXID").alias("IdentificadorTransaccion"),
            F.lit(fuente).alias("FuenteDatos"),
        )
    )
    return reordenar_columnas_lc(datos, ["FechaRegistro", "Hash_Transaccion"])
