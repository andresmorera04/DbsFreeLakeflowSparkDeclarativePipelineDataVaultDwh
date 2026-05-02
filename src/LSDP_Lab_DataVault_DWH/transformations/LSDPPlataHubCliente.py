# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataHubCliente — Hub: Llave de negocio de Cliente
# ---------------------------------------------------------------------------
# Streaming Table: Hub_Cliente
# Fuente: {catalogo}.{esquema}.CMSTFL  →  Plata: {catalogo_plata}.{esquema_plata}.Hub_Cliente
# Deduplicación incremental: AUTO CDC SCD Type 1 por IdentificadorCliente
# El motor gestiona deduplicación cross-batch via MERGE (sin full scan del Hub).
# FechaRegistro se actualiza en cada update via MERGE (semántica "última vez vista").
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
    name=f"{_catalogo_plata}.{_esquema_plata}.Hub_Cliente",
    cluster_by=["Hash_Cliente", "FechaRegistro"],
    expect_all_or_drop={"id_cliente_positivo": "IdentificadorCliente > 0"},
    expect_all_or_fail={"hash_cliente_no_nulo": "Hash_Cliente IS NOT NULL"},
    table_properties=_PROP_TABLE,
)


@dp.view
def hub_cliente_src():
    fuente = f"{config['catalogo']}.{config['esquema']}.CMSTFL"
    df = dp.read_stream(fuente)
    datos = df.select(
        calcular_hash_hub([F.col("CUSTID")]).alias("Hash_Cliente"),
        F.col("CUSTID").alias("IdentificadorCliente"),
        F.lit(fuente).alias("FuenteDatos"),
        F.current_timestamp().alias("FechaRegistro"),
    )
    return reordenar_columnas_lc(datos, ["Hash_Cliente"])


dp.create_auto_cdc_flow(
    target=f"{_catalogo_plata}.{_esquema_plata}.Hub_Cliente",
    source="hub_cliente_src",
    keys=["IdentificadorCliente"],
    sequence_by=F.expr("current_timestamp()"),
    stored_as_scd_type=1,
)
