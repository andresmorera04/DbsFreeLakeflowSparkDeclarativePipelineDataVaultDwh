# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataHubOperacion — Hub: Llave de negocio compuesta de Operación
# ---------------------------------------------------------------------------
# Streaming Table: Hub_Operacion
# Fuente: {catalogo}.{esquema}.BLNCFL  →  Plata: Hub_Operacion
# Llave compuesta: CUSTID | BLSQ
# Deduplicación incremental: AUTO CDC SCD Type 1 por IdentificadorCliente + SecuenciaSaldo
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
    name=f"{_catalogo_plata}.{_esquema_plata}.Hub_Operacion",
    cluster_by=["Hash_Operacion", "FechaRegistro"],
    expect_all_or_drop={"id_cliente_positivo": "IdentificadorCliente > 0"},
    expect_all_or_fail={"hash_operacion_no_nulo": "Hash_Operacion IS NOT NULL"},
    table_properties=_PROP_TABLE,
)


@dp.view
def hub_operacion_src():
    fuente = f"{config['catalogo']}.{config['esquema']}.BLNCFL"
    df = dp.read_stream(fuente)
    datos = df.select(
        calcular_hash_hub([F.col("CUSTID"), F.col("BLSQ")]).alias("Hash_Operacion"),
        F.col("CUSTID").alias("IdentificadorCliente"),
        F.col("BLSQ").alias("SecuenciaSaldo"),
        F.lit(fuente).alias("FuenteDatos"),
        F.current_timestamp().alias("FechaRegistro"),
    )
    return reordenar_columnas_lc(datos, ["Hash_Operacion"])


dp.create_auto_cdc_flow(
    target=f"{_catalogo_plata}.{_esquema_plata}.Hub_Operacion",
    source="hub_operacion_src",
    keys=["IdentificadorCliente", "SecuenciaSaldo"],
    sequence_by=F.expr("current_timestamp()"),
    stored_as_scd_type=1,
)
