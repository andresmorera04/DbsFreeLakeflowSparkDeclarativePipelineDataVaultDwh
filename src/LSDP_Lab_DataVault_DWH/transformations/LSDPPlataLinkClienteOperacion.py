# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataLinkClienteOperacion — Link: Relación Cliente ↔ Operación
# ---------------------------------------------------------------------------
# Materialized View: Link_Cliente_Operacion
# Fuente: {catalogo}.{esquema}.BLNCFL
# Hashes calculados desde campos AS400 originales (no se leen de los Hubs)
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadPrincipal import calcular_hash_hub, reordenar_columnas_lc

config = obtener_configuracion(spark)


@dp.materialized_view(
    name=f"{config['catalogo_plata']}.{config['esquema_plata']}.Link_Cliente_Operacion",
    cluster_by=["FechaRegistro", "Hash_Cliente", "Hash_Operacion"],
)
def link_cliente_operacion():
    fuente = f"{config['catalogo']}.{config['esquema']}.BLNCFL"
    df = spark.read.table(fuente)

    hash_cliente = calcular_hash_hub([F.col("CUSTID")])
    hash_operacion = calcular_hash_hub([F.col("CUSTID"), F.col("BLSQ")])

    resultado = (
        df.select(
            F.current_timestamp().alias("FechaRegistro"),
            hash_cliente.alias("Hash_Cliente"),
            hash_operacion.alias("Hash_Operacion"),
            calcular_hash_hub([hash_cliente, hash_operacion]).alias("Hash_Link_Cliente_Operacion"),
            F.lit(fuente).alias("FuenteDatos"),
        )
        .dropDuplicates(["Hash_Cliente", "Hash_Operacion"])
    )
    return reordenar_columnas_lc(resultado, ["FechaRegistro", "Hash_Cliente", "Hash_Operacion"])
