# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataHubCliente — Hub: Llave de negocio de Cliente
# ---------------------------------------------------------------------------
# Materialized View: Hub_Cliente
# Fuente: {catalogo}.{esquema}.CMSTFL  →  Plata: {catalogo_plata}.{esquema_plata}.Hub_Cliente
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadPrincipal import calcular_hash_hub, reordenar_columnas_lc

config = obtener_configuracion(spark)


@dp.expect_or_drop("id_cliente_positivo", "IdentificadorCliente > 0")
@dp.expect_or_fail("hash_cliente_no_nulo", "Hash_Cliente IS NOT NULL")
@dp.materialized_view(
    name=f"{config['catalogo_plata']}.{config['esquema_plata']}.Hub_Cliente",
    cluster_by=["FechaRegistro", "Hash_Cliente"],
)
def hub_cliente():
    fuente = f"{config['catalogo']}.{config['esquema']}.CMSTFL"
    df = spark.read.table(fuente)
    resultado = (
        df.select(
            F.current_timestamp().alias("FechaRegistro"),
            calcular_hash_hub([F.col("CUSTID")]).alias("Hash_Cliente"),
            F.col("CUSTID").alias("IdentificadorCliente"),
            F.lit(fuente).alias("FuenteDatos"),
        )
        .dropDuplicates(["IdentificadorCliente"])
    )
    return reordenar_columnas_lc(resultado, ["FechaRegistro", "Hash_Cliente"])
