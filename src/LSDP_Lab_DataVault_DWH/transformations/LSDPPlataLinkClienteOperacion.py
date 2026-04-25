# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataLinkClienteOperacion — Link: Relación Cliente ↔ Operación
# ---------------------------------------------------------------------------
# Streaming Table: Link_Cliente_Operacion
# Fuente: {catalogo}.{esquema}.BLNCFL
# Hashes calculados desde campos AS400 originales (no se leen de los Hubs)
# Deduplicación incremental: LEFT ANTI JOIN por Hash_Cliente + Hash_Operacion
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadPrincipal import calcular_hash_hub, procesar_link, reordenar_columnas_lc

config = obtener_configuracion(spark)

_catalogo_plata = config["catalogo_plata"]
_esquema_plata = config["esquema_plata"]

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Link_Cliente_Operacion",
    cluster_by=["FechaRegistro", "Hash_Cliente", "Hash_Operacion"],
)


@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Link_Cliente_Operacion")
def link_cliente_operacion_flow():
    fuente = f"{config['catalogo']}.{config['esquema']}.BLNCFL"
    df = dp.read_stream(fuente)

    hash_cliente = calcular_hash_hub([F.col("CUSTID")])
    hash_operacion = calcular_hash_hub([F.col("CUSTID"), F.col("BLSQ")])

    datos = (
        df.select(
            F.current_timestamp().alias("FechaRegistro"),
            hash_cliente.alias("Hash_Cliente"),
            hash_operacion.alias("Hash_Operacion"),
            calcular_hash_hub([hash_cliente, hash_operacion]).alias("Hash_Link_Cliente_Operacion"),
            F.lit(fuente).alias("FuenteDatos"),
        )
    )
    resultado = procesar_link(
        spark,
        _catalogo_plata,
        _esquema_plata,
        "Link_Cliente_Operacion",
        ["Hash_Cliente", "Hash_Operacion"],
        datos,
    )
    return reordenar_columnas_lc(resultado, ["FechaRegistro", "Hash_Cliente", "Hash_Operacion"])
