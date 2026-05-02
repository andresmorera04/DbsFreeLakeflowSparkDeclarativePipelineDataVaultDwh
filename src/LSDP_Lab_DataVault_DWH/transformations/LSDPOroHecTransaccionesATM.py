# ---------------------------------------------------------------------------
# LSDPOroHecTransaccionesATM.py — Hec_Transacciones_ATM como Vista Materializada
# ---------------------------------------------------------------------------
# Medalla de Oro · Tabla de Hechos de Transacciones ATM
# Implementación: Vista Materializada (@dp.materialized_view).
# Grano: una fila por transacción ATM (DATM=retiro, CATM=depósito).
#
# Elegibilidad para refresh incremental (CDF):
#   El plan de esta MV se reduce a UNA SOLA lectura de `Trx_ATM_Stream`
#   (Streaming Table append-only que ya pre-resuelve las FKs
#   `DimIdCliente`/`DimIdOperacion`) más dos `withColumn` para derivar
#   `EsRetiro`/`EsDeposito` y proyección. Esto evita simultáneamente:
#     • el operador de ventana (eliminado del helper de operación dominante)
#     • el rechazo del cost model por NUM_JOINS_THRESHOLD_EXCEEDED
#       (los 3 joins por Hash_Transaccion y el join por Hash_Cliente
#        fueron absorbidos por Trx_ATM_Stream)
#     • el rechazo del cost model por CHANGESET_SIZE_THRESHOLD_EXCEEDED
#       (el changeset que llega al hecho es exclusivamente el delta
#        append-only de Trx_ATM_Stream — el changeset masivo de
#        Map_Cliente_Operacion_Dominante NO se propaga al hecho porque
#        el hecho ya no la consume directamente; Solución 1)
#   dejando al hecho elegible para refresh incremental por CDF.
#
# Pipeline interno:
#   1. Leer `Trx_ATM_Stream` (Streaming Table temporary): ya contiene
#      Sats + Hub + Link de transacción con filtro DATM/CATM aplicado
#      aguas arriba, MÁS las FKs `DimIdCliente` y `DimIdOperacion` ya
#      resueltas.
#   2. Derivar EsRetiro y EsDeposito (BooleanType nativo).
#   3. Proyectar esquema cerrado.
#
# Restricciones:
#   • Todas las fuentes se leen con spark.read.table() — nunca readStream.
#   • Sin operadores de ventana en este notebook.
#   • Sin joins en este notebook (Solución 1: FKs pre-resueltas en Trx_ATM_Stream).
#   • El hecho NO lee directamente Sats, Hubs, Links, Dimensiones, ni
#     Map_Cliente_Operacion_Dominante.
#   • EsRetiro y EsDeposito son BooleanType (calculados en Oro).
#   • Sin operaciones de estado en memoria, APIs de bajo nivel ni paralelismo externo.
#
# Sol. 3 (refuerzo): table_properties incluye `delta.targetFileSize=16mb`
# y `delta.tuneFileSizesForRewrites=true` para que el cost model estime
# costes más bajos para ROW_BASED al tener archivos más pequeños y
# focalizados en las reescrituras incrementales.
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.LSDPConfiguracion import (
    TIPO_CATM,
    TIPO_DATM,
    obtener_configuracion,
)
from utilities.LSDPUtilidadOro import (
    validar_columnas_oro,
)

config = obtener_configuracion(spark)
catalogo_oro = config["catalogo_oro"]
esquema_oro = config["esquema_oro"]

# ─── Expectations ────────────────────────────────────────────────────────────

_validaciones_hec_atm = {
    "dim_id_cliente_no_nulo": "DimIdCliente IS NOT NULL",
    "identificador_transaccion_no_nulo": "IdentificadorTransaccion IS NOT NULL",
    "fecha_transaccion_no_nula": "FechaClave IS NOT NULL",
    "tipo_transaccion_valido": f"TipoTransaccion IN ('{TIPO_DATM}', '{TIPO_CATM}')",
}

# ─── Columnas cerradas del esquema de Hec_Transacciones_ATM ──────────────────

_COLUMNAS_CERRADAS_HEC_ATM = [
    "FechaClave",
    "DimIdCliente",
    "DimIdOperacion",
    "IdentificadorTransaccion",
    "Hash_Transaccion",
    "TipoTransaccion",
    "MonedaTransaccion",
    "EstadoTransaccion",
    "CanalTransaccion",
    "RangoMontoTransaccion",
    "ClasificacionCanalATM",
    "MontoPrincipal",
    "ComisionTransaccion",
    "TotalTransaccion",
    "EsRetiro",
    "EsDeposito",
]

# ─── Vista Materializada — Hec_Transacciones_ATM ─────────────────────────────


@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.Hec_Transacciones_ATM",
    cluster_by=["FechaClave", "DimIdCliente"],
    table_properties={
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days",
        # Solución 3 (refuerzo): archivos pequeños + tuneo para reescrituras
        # bajan drásticamente el coste estimado de ROW_BASED por el cost model.
        "delta.targetFileSize": "16mb",
        "delta.tuneFileSizesForRewrites": "true",
    },
)
@dp.expect_all_or_fail(_validaciones_hec_atm)
@dp.expect("dim_id_operacion_presente", "DimIdOperacion IS NOT NULL")
def hec_transacciones_atm():
    """Vista Materializada de la tabla de hechos ATM (DATM=retiro, CATM=depósito).

    Plan compuesto por una única lectura de `Trx_ATM_Stream` (Streaming Table
    upstream que ya pre-resuelve las FKs `DimIdCliente`/`DimIdOperacion`
    como parte de su esquema cerrado), más dos `withColumn` para derivar
    las banderas `EsRetiro`/`EsDeposito` y proyección final.

    **Cero joins, cero agregaciones, sin operadores de ventana, sin lecturas
    a Sats/Hubs/Links/Dimensiones ni a `Map_Cliente_Operacion_Dominante`**.
    Esta arquitectura (Solución 1) elimina la dependencia del changeset
    masivo de `Map_Cliente_Operacion_Dominante` que bloqueaba al cost model
    con `CHANGESET_SIZE_THRESHOLD_EXCEEDED`. El changeset que llega al hecho
    queda acotado a las transacciones realmente nuevas que aparecen en la
    Streaming Table upstream — trivialmente elegible para `ROW_BASED`.
    """
    trx = spark.read.table("Trx_ATM_Stream")

    trx_completo = (
        trx
        .withColumn(
            "EsRetiro",
            F.when(F.col("TipoTransaccion") == TIPO_DATM, F.lit(True))
             .otherwise(F.lit(False)),
        )
        .withColumn(
            "EsDeposito",
            F.when(F.col("TipoTransaccion") == TIPO_CATM, F.lit(True))
             .otherwise(F.lit(False)),
        )
    )

    validar_columnas_oro(trx_completo, _COLUMNAS_CERRADAS_HEC_ATM, "Hec_Transacciones_ATM")

    return trx_completo.select(*_COLUMNAS_CERRADAS_HEC_ATM)
