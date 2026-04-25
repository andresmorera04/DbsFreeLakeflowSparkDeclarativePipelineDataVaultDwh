# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataHubCliente — Hub: Llave de negocio de Cliente
# ---------------------------------------------------------------------------
# Streaming Table: Hub_Cliente
# Fuente: {catalogo}.{esquema}.CMSTFL  →  Plata: {catalogo_plata}.{esquema_plata}.Hub_Cliente
# Deduplicación incremental: LEFT ANTI JOIN por IdentificadorCliente
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadPrincipal import calcular_hash_hub, procesar_hub, reordenar_columnas_lc

config = obtener_configuracion(spark)

_catalogo_plata = config["catalogo_plata"]
_esquema_plata = config["esquema_plata"]

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Hub_Cliente",
    cluster_by=["FechaRegistro", "Hash_Cliente"],
    expect_all_or_drop={"id_cliente_positivo": "IdentificadorCliente > 0"},
    expect_all_or_fail={"hash_cliente_no_nulo": "Hash_Cliente IS NOT NULL"},
)


@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Hub_Cliente")
def hub_cliente_flow():
    fuente = f"{config['catalogo']}.{config['esquema']}.CMSTFL"
    df = dp.read_stream(fuente)
    datos = (
        df.select(
            F.current_timestamp().alias("FechaRegistro"),
            calcular_hash_hub([F.col("CUSTID")]).alias("Hash_Cliente"),
            F.col("CUSTID").alias("IdentificadorCliente"),
            F.lit(fuente).alias("FuenteDatos"),
        )
    )
    resultado = procesar_hub(
        spark,
        _catalogo_plata,
        _esquema_plata,
        "Hub_Cliente",
        ["IdentificadorCliente"],
        datos,
    )
    return reordenar_columnas_lc(resultado, ["FechaRegistro", "Hash_Cliente"])
