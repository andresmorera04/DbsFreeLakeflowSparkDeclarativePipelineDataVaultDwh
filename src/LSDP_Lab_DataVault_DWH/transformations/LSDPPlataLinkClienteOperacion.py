# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataLinkClienteOperacion — Link: Relación Cliente ↔ Operación
# ---------------------------------------------------------------------------
# Streaming Table: Link_Cliente_Operacion
# Fuente: {catalogo}.{esquema}.BLNCFL
# Hashes calculados desde campos AS400 originales (no se leen de los Hubs)
# Deduplicación incremental: AUTO CDC SCD Type 1 por Hash_Cliente + Hash_Operacion
# El motor garantiza unicidad de la combinación Hash_Hub1 + Hash_Hub2 via MERGE.
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
    name=f"{_catalogo_plata}.{_esquema_plata}.Link_Cliente_Operacion",
    cluster_by=["Hash_Cliente", "Hash_Operacion", "FechaRegistro"],
    table_properties=_PROP_TABLE,
)


@dp.view
def link_cliente_operacion_src():
    fuente = f"{config['catalogo']}.{config['esquema']}.BLNCFL"
    df = dp.read_stream(fuente)

    hash_cliente = calcular_hash_hub([F.col("CUSTID")])
    hash_operacion = calcular_hash_hub([F.col("CUSTID"), F.col("BLSQ")])

    datos = df.select(
        hash_cliente.alias("Hash_Cliente"),
        hash_operacion.alias("Hash_Operacion"),
        calcular_hash_hub([hash_cliente, hash_operacion]).alias("Hash_Link_Cliente_Operacion"),
        F.lit(fuente).alias("FuenteDatos"),
        F.current_timestamp().alias("FechaRegistro"),
    )
    return reordenar_columnas_lc(datos, ["Hash_Cliente", "Hash_Operacion"])


dp.create_auto_cdc_flow(
    target=f"{_catalogo_plata}.{_esquema_plata}.Link_Cliente_Operacion",
    source="link_cliente_operacion_src",
    keys=["Hash_Cliente", "Hash_Operacion"],
    sequence_by=F.expr("current_timestamp()"),
    stored_as_scd_type=1,
)
