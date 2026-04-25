# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataHubOperacion — Hub: Llave de negocio compuesta de Operación
# ---------------------------------------------------------------------------
# Streaming Table: Hub_Operacion
# Fuente: {catalogo}.{esquema}.BLNCFL  →  Plata: Hub_Operacion
# Llave compuesta: CUSTID | BLSQ
# Deduplicación incremental: LEFT ANTI JOIN por IdentificadorCliente + SecuenciaSaldo
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadPrincipal import calcular_hash_hub, procesar_hub, reordenar_columnas_lc

config = obtener_configuracion(spark)

_catalogo_plata = config["catalogo_plata"]
_esquema_plata = config["esquema_plata"]

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Hub_Operacion",
    cluster_by=["FechaRegistro", "Hash_Operacion"],
    expect_all_or_drop={"id_cliente_positivo": "IdentificadorCliente > 0"},
    expect_all_or_fail={"hash_operacion_no_nulo": "Hash_Operacion IS NOT NULL"},
)


@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Hub_Operacion")
def hub_operacion_flow():
    fuente = f"{config['catalogo']}.{config['esquema']}.BLNCFL"
    df = dp.read_stream(fuente)
    datos = (
        df.select(
            F.current_timestamp().alias("FechaRegistro"),
            calcular_hash_hub([F.col("CUSTID"), F.col("BLSQ")]).alias("Hash_Operacion"),
            F.col("CUSTID").alias("IdentificadorCliente"),
            F.col("BLSQ").alias("SecuenciaSaldo"),
            F.lit(fuente).alias("FuenteDatos"),
        )
    )
    resultado = procesar_hub(
        spark,
        _catalogo_plata,
        _esquema_plata,
        "Hub_Operacion",
        ["IdentificadorCliente", "SecuenciaSaldo"],
        datos,
    )
    return reordenar_columnas_lc(resultado, ["FechaRegistro", "Hash_Operacion"])
