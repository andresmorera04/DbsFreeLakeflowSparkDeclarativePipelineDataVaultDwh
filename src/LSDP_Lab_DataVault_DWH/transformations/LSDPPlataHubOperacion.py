# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataHubOperacion — Hub: Llave de negocio compuesta de Operación
# ---------------------------------------------------------------------------
# Materialized View: Hub_Operacion
# Fuente: {catalogo}.{esquema}.BLNCFL  →  Plata: Hub_Operacion
# Llave compuesta: CUSTID | BLSQ
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadPrincipal import calcular_hash_hub, reordenar_columnas_lc

config = obtener_configuracion(spark)


@dp.expect_or_drop("id_cliente_positivo", "IdentificadorCliente > 0")
@dp.expect_or_fail("hash_operacion_no_nulo", "Hash_Operacion IS NOT NULL")
@dp.materialized_view(
    name=f"{config['catalogo_plata']}.{config['esquema_plata']}.Hub_Operacion",
    cluster_by=["FechaRegistro", "Hash_Operacion"],
)
def hub_operacion():
    fuente = f"{config['catalogo']}.{config['esquema']}.BLNCFL"
    df = spark.read.table(fuente)
    resultado = (
        df.select(
            F.current_timestamp().alias("FechaRegistro"),
            calcular_hash_hub([F.col("CUSTID"), F.col("BLSQ")]).alias("Hash_Operacion"),
            F.col("CUSTID").alias("IdentificadorCliente"),
            F.col("BLSQ").alias("SecuenciaSaldo"),
            F.lit(fuente).alias("FuenteDatos"),
        )
        .dropDuplicates(["IdentificadorCliente", "SecuenciaSaldo"])
    )
    return reordenar_columnas_lc(resultado, ["FechaRegistro", "Hash_Operacion"])
