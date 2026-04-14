# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataLinkClienteTransaccion — Link: Relación Cliente ↔ Transacción
# ---------------------------------------------------------------------------
# Materialized View: Link_Cliente_Transaccion
# Fuente: {catalogo}.{esquema}.TRXPFL
# Hashes calculados desde campos AS400 originales (no se leen de los Hubs)
# 5.7% de clientes sin transacciones no generan registros — comportamiento correcto
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadPrincipal import calcular_hash_hub, reordenar_columnas_lc

config = obtener_configuracion(spark)


@dp.materialized_view(
    name=f"{config['catalogo_plata']}.{config['esquema_plata']}.Link_Cliente_Transaccion",
    cluster_by=["FechaRegistro", "Hash_Cliente", "Hash_Transaccion"],
)
def link_cliente_transaccion():
    fuente = f"{config['catalogo']}.{config['esquema']}.TRXPFL"
    df = spark.read.table(fuente)

    hash_cliente = calcular_hash_hub([F.col("CUSTID")])
    hash_transaccion = calcular_hash_hub([F.col("TRXID")])

    resultado = (
        df.select(
            F.current_timestamp().alias("FechaRegistro"),
            hash_cliente.alias("Hash_Cliente"),
            hash_transaccion.alias("Hash_Transaccion"),
            calcular_hash_hub([hash_cliente, hash_transaccion]).alias("Hash_Link_Cliente_Transaccion"),
            F.lit(fuente).alias("FuenteDatos"),
        )
        .dropDuplicates(["Hash_Cliente", "Hash_Transaccion"])
    )
    return reordenar_columnas_lc(resultado, ["FechaRegistro", "Hash_Cliente", "Hash_Transaccion"])
