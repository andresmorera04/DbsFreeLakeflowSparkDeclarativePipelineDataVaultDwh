# ---------------------------------------------------------------------------
# LSDPOroMapClienteOperacionDominante.py — Mapa Cliente → Operación Dominante
# ---------------------------------------------------------------------------
# Medalla de Oro · MV auxiliar para resolver DimIdCliente y DimIdOperacion
# por cliente con un único join downstream desde el hecho.
#
# Propósito (mitigación R-02 + reducción del changeset propagado):
#   Materializa el mapa `Hash_Cliente → (Hash_Operacion dominante,
#   DimIdCliente, DimIdOperacion)`. La operación dominante por cliente se
#   calcula con `groupBy().agg(max(struct(...)))` (top-1 por grupo) en
#   lugar de `row_number() OVER (...)`, dado que las funciones de ventana
#   NO son elegibles para mantenimiento incremental por Enzyme, mientras
#   que las agregaciones por grupo sí lo son. Esto permite que esta MV
#   se mantenga incrementalmente y emita un changeset acotado a clientes
#   nuevos/modificados, en lugar de un changeset = 100% de su contenido
#   en cada corrida. Sin esa contención, el cost model del hecho rechaza
#   el plan incremental con `CHANGESET_SIZE_THRESHOLD_EXCEEDED`.
#
# Visibilidad: `temporary=True` — dataset interno del pipeline, NO publicado
#   a Unity Catalog. Solo es referenciable desde otros datasets del mismo
#   pipeline mediante su nombre no calificado (`Map_Cliente_Operacion_Dominante`).
#   Se mantiene materializada y con CDF para preservar la incrementalidad de
#   `Hec_Transacciones_ATM`, que la consume vía join equi-key.
#
# Esquema cerrado:
#   - Hash_Cliente        (string)  — llave de cliente
#   - Hash_Operacion      (string)  — operación dominante (mayor SecuenciaSaldo,
#                                     desempate por Hash_Operacion ASC)
#   - DimIdCliente        (long)    — FK resuelta a Dim_Cliente
#   - DimIdOperacion      (long)    — FK resuelta a Dim_Operacion
#
# Fuente: Hub_Operacion + Link_Cliente_Operacion (Plata) + Dim_Operacion +
#         Dim_Cliente (Oro). Lectura: spark.read.table.
#
# Nota: al unificar DimIdCliente y DimIdOperacion en este único mapa por
# Hash_Cliente, el hecho `Hec_Transacciones_ATM` resuelve ambas FKs con un
# solo join equi-key (mitigación NUM_JOINS_THRESHOLD_EXCEEDED del cost model).
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadOro import (
    seleccionar_operacion_dominante,
    validar_columnas_oro,
)

config = obtener_configuracion(spark)
catalogo_plata = config["catalogo_plata"]
esquema_plata = config["esquema_plata"]
catalogo_oro = config["catalogo_oro"]
esquema_oro = config["esquema_oro"]

_validaciones_map_dom = {
    "hash_cliente_no_nulo": "Hash_Cliente IS NOT NULL",
    "hash_operacion_no_nulo": "Hash_Operacion IS NOT NULL",
    "dim_id_cliente_no_nulo": "DimIdCliente IS NOT NULL",
}

_COLUMNAS_CERRADAS_MAP_DOM = [
    "Hash_Cliente",
    "Hash_Operacion",
    "DimIdCliente",
    "DimIdOperacion",
]


@dp.materialized_view(
    name="Map_Cliente_Operacion_Dominante",
    temporary=True,
    cluster_by=["Hash_Cliente"],
    table_properties={
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days",
    },
)
@dp.expect_all_or_fail(_validaciones_map_dom)
def map_cliente_operacion_dominante():
    """Mapa Hash_Cliente → (operación dominante, DimIdCliente, DimIdOperacion).

    `seleccionar_operacion_dominante` usa `groupBy().agg(max(struct(...)))`
    (top-1 por grupo) en lugar de funciones de ventana, por lo que el plan
    de esta MV es elegible para mantenimiento incremental por Enzyme.
    Además precomputa `DimIdCliente` y `DimIdOperacion` para que el hecho
    resuelva ambas FKs con un solo join equi-key por `Hash_Cliente`
    (mitigación NUM_JOINS_THRESHOLD_EXCEEDED + CHANGESET_SIZE_THRESHOLD_EXCEEDED).
    """
    hub_op = spark.read.table(
        f"{catalogo_plata}.{esquema_plata}.Hub_Operacion"
    ).select("Hash_Operacion", "SecuenciaSaldo")

    link_cli_op = spark.read.table(
        f"{catalogo_plata}.{esquema_plata}.Link_Cliente_Operacion"
    ).select("Hash_Cliente", "Hash_Operacion")

    op_dominante = seleccionar_operacion_dominante(hub_op, link_cli_op)

    dim_op = spark.read.table(
        f"{catalogo_oro}.{esquema_oro}.Dim_Operacion"
    ).select("Hash_Operacion", "DimIdOperacion")

    dim_cli = spark.read.table(
        f"{catalogo_oro}.{esquema_oro}.Dim_Cliente"
    ).select("Hash_Cliente", "DimIdCliente")

    resultado = (
        op_dominante
        .join(F.broadcast(dim_op), on="Hash_Operacion", how="left")
        .join(F.broadcast(dim_cli), on="Hash_Cliente", how="left")
    )

    validar_columnas_oro(
        resultado,
        _COLUMNAS_CERRADAS_MAP_DOM,
        "Map_Cliente_Operacion_Dominante",
    )

    return resultado.select(
        "Hash_Cliente",
        "Hash_Operacion",
        "DimIdCliente",
        "DimIdOperacion",
    )
