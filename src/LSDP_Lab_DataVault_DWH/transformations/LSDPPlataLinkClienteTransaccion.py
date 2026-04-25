# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataLinkClienteTransaccion — Link: Relación Cliente ↔ Transacción
# ---------------------------------------------------------------------------
# Streaming Table: Link_Cliente_Transaccion
# Fuente: {catalogo}.{esquema}.TRXPFL
# Hashes calculados desde campos AS400 originales (no se leen de los Hubs)
# 5.7% de clientes sin transacciones no generan registros — comportamiento correcto
# Deduplicación incremental: LEFT ANTI JOIN por Hash_Cliente + Hash_Transaccion
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadPrincipal import calcular_hash_hub, procesar_link, reordenar_columnas_lc

config = obtener_configuracion(spark)

_catalogo_plata = config["catalogo_plata"]
_esquema_plata = config["esquema_plata"]

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Link_Cliente_Transaccion",
    cluster_by=["FechaRegistro", "Hash_Cliente", "Hash_Transaccion"],
)


@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Link_Cliente_Transaccion")
def link_cliente_transaccion_flow():
    fuente = f"{config['catalogo']}.{config['esquema']}.TRXPFL"
    df = dp.read_stream(fuente)

    hash_cliente = calcular_hash_hub([F.col("CUSTID")])
    hash_transaccion = calcular_hash_hub([F.col("TRXID")])

    datos = (
        df.select(
            F.current_timestamp().alias("FechaRegistro"),
            hash_cliente.alias("Hash_Cliente"),
            hash_transaccion.alias("Hash_Transaccion"),
            calcular_hash_hub([hash_cliente, hash_transaccion]).alias("Hash_Link_Cliente_Transaccion"),
            F.lit(fuente).alias("FuenteDatos"),
        )
    )
    resultado = procesar_link(
        spark,
        _catalogo_plata,
        _esquema_plata,
        "Link_Cliente_Transaccion",
        ["Hash_Cliente", "Hash_Transaccion"],
        datos,
    )
    return reordenar_columnas_lc(resultado, ["FechaRegistro", "Hash_Cliente", "Hash_Transaccion"])
