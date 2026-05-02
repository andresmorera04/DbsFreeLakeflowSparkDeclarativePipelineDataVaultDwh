# ---------------------------------------------------------------------------
# LSDPOroTrxATMEnriquecida.py — Transacciones ATM pre-enriquecidas (Streaming)
# ---------------------------------------------------------------------------
# Medalla de Oro · Streaming Table auxiliar (temporary, NO publicada en
# Unity Catalog) que pre-compone los datos transaccionales por
# Hash_Transaccion para que `Hec_Transacciones_ATM` quede con un único
# join en su plan lógico Y reciba un changeset acotado a transacciones
# realmente nuevas.
#
# Nombre del dataset: `Trx_ATM_Stream`. Se eligió un identificador nuevo
# (distinto del antiguo `Trx_ATM_Enriquecida`) para evitar el error
# `CANNOT_CHANGE_DATASET_TYPE` que SDP arroja cuando se intenta cambiar
# el tipo de un dataset existente de MATERIALIZED_VIEW a STREAMING_TABLE
# bajo el mismo identificador. Con un nombre nuevo, SDP crea desde cero
# el storage table como ST sin colisionar con la versión MV anterior.
#
# Propósito (mitigación CHANGESET_SIZE_THRESHOLD_EXCEEDED del cost model):
#   La versión MV anterior caía a COMPLETE_RECOMPUTE en cada corrida, por
#   lo que su CDF emitía un changeset = 100% de su contenido (~21M filas)
#   al hecho. Frente a ese delta gigante, el cost model del hecho
#   rechazaba el plan incremental.
#
#   Como Streaming Table (declarada con `@dp.table(temporary=True, ...)`
#   sobre un flujo originado por `spark.readStream.table(...)`), esta tabla:
#     • procesa solo los nuevos `Hash_Transaccion` aparecidos en
#       `Sat_Transaccion_DatosEstables` (semántica append-only de SDP);
#     • emite un CDF acotado a esos appends nuevos;
#     • permite que el cost model del hecho elija el plan incremental.
#
# Stream-static join:
#   • Fuente streaming: Sat_Transaccion_DatosEstables (con filtro
#     DATM/CATM aplicado aguas arriba).
#   • Lookups batch: Sat_Transaccion_Montos, Hub_Transaccion,
#     Link_Cliente_Transaccion (joins equi-key por Hash_Transaccion;
#     Spark soporta stream-static inner/left joins).
#
# Visibilidad: `temporary=True` — dataset interno del pipeline, no
#   publicado a Unity Catalog. Se consume por nombre no calificado.
#
# Esquema cerrado:
#   - Hash_Transaccion        (string)
#   - Hash_Cliente            (string)  — para el join final con el mapa
#   - IdentificadorTransaccion (string)
#   - FechaClave              (date)
#   - TipoTransaccion         (string)  — solo DATM o CATM
#   - MonedaTransaccion       (string)
#   - EstadoTransaccion       (string)
#   - CanalTransaccion        (string)
#   - ClasificacionCanalATM   (string)
#   - MontoPrincipal          (decimal)
#   - ComisionTransaccion     (decimal)
#   - TotalTransaccion        (decimal)
#   - RangoMontoTransaccion   (string)
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.LSDPConfiguracion import (
    TIPO_CATM,
    TIPO_DATM,
    obtener_configuracion,
)
from utilities.LSDPUtilidadOro import validar_columnas_oro

config = obtener_configuracion(spark)
catalogo_plata = config["catalogo_plata"]
esquema_plata = config["esquema_plata"]

_validaciones_trx_atm = {
    "hash_transaccion_no_nulo": "Hash_Transaccion IS NOT NULL",
    "hash_cliente_no_nulo": "Hash_Cliente IS NOT NULL",
    "tipo_transaccion_valido": f"TipoTransaccion IN ('{TIPO_DATM}', '{TIPO_CATM}')",
    "dim_id_cliente_no_nulo": "DimIdCliente IS NOT NULL",
}

# table_properties: el `targetFileSize` reducido y `tuneFileSizesForRewrites`
# refuerzan la elegibilidad de mantenimiento incremental aguas abajo —
# permiten al cost model del consumidor (Hec_Transacciones_ATM) estimar
# costes más bajos para ROW_BASED al tener archivos más pequeños y
# focalizados (Solución 3 — refuerzo).
_PROP_TABLE = {
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days",
    "delta.targetFileSize": "16mb",
    "delta.tuneFileSizesForRewrites": "true",
}

# Esquema cerrado de 15 columnas: las 13 columnas transaccionales
# más las dos FKs dimensionales (DimIdCliente, DimIdOperacion).
# Pre-resolver las FKs aquí (Solución 1) elimina el único join restante
# en `Hec_Transacciones_ATM`, dejándolo con plan = read + 2 withColumn +
# select — sin joins, sin agregaciones — trivialmente elegible para
# mantenimiento incremental por CDF.
#
# Semántica: las FKs (`DimIdCliente`, `DimIdOperacion`) quedan fijadas
# al momento del append. Cambios posteriores en
# `Map_Cliente_Operacion_Dominante` NO re-enriquecen transacciones
# históricas; esto es la semántica correcta para una tabla de hechos
# transaccionales (refleja el estado del cliente al momento de la
# transacción).
_COLUMNAS_CERRADAS_TRX_ATM = [
    "Hash_Transaccion",
    "Hash_Cliente",
    "IdentificadorTransaccion",
    "FechaClave",
    "TipoTransaccion",
    "MonedaTransaccion",
    "EstadoTransaccion",
    "CanalTransaccion",
    "ClasificacionCanalATM",
    "MontoPrincipal",
    "ComisionTransaccion",
    "TotalTransaccion",
    "RangoMontoTransaccion",
    "DimIdCliente",
    "DimIdOperacion",
]


# ─── Streaming Table — Trx_ATM_Stream (temporary) ────────────────────────────
#
# Implementación: usamos el decorador `@dp.table` con `temporary=True`. Cuando
# la función decorada devuelve un DataFrame de streaming (originado por
# `spark.readStream.table(...)`), Lakeflow SDP la materializa como
# **Streaming Table** automáticamente, con semántica append-only.
#
# Nota: la API alternativa basada en `create_streaming_table` no se usa
# aquí porque, en el runtime actual de `pyspark.pipelines`, esa función
# no acepta el argumento `temporary` (lanza TypeError en FULL REFRESH).
# El decorador `@dp.table(temporary=True, ...)` sobre un flujo de
# streaming es la forma soportada para declarar una Streaming Table
# temporary.


@dp.table(
    name="Trx_ATM_Stream",
    temporary=True,
    cluster_by=["Hash_Cliente", "FechaClave"],
    table_properties=_PROP_TABLE,
)
@dp.expect_all_or_fail(_validaciones_trx_atm)
def trx_atm_stream():
    """Stream-static join sobre Hash_Transaccion + Hash_Cliente para append incremental.

    La fuente streaming (`Sat_Transaccion_DatosEstables`) entrega solo los
    nuevos `Hash_Transaccion` por micro-batch (semántica append-only del
    pipeline). Los lookups por `Hash_Transaccion` (`Sat_Transaccion_Montos`,
    `Hub_Transaccion`, `Link_Cliente_Transaccion`) y por `Hash_Cliente`
    (`Map_Cliente_Operacion_Dominante` para resolver FKs `DimIdCliente` y
    `DimIdOperacion`) se leen con `spark.read.table` como tablas estáticas
    en cada planificación (stream-static join soportado por Structured
    Streaming).

    Las FKs (`DimIdCliente`, `DimIdOperacion`) se pre-resuelven aquí
    (Solución 1) para que `Hec_Transacciones_ATM` quede sin joins,
    eliminando el bloqueo del cost model `CHANGESET_SIZE_THRESHOLD_EXCEEDED`
    que persistía al consumir el changeset masivo de
    `Map_Cliente_Operacion_Dominante`.

    Nota sobre el join con Map: NO se usa `F.broadcast(map_dom)` porque
    `Map_Cliente_Operacion_Dominante` tiene cardinalidad ≈ #clientes
    (~4M filas con `Hash_Cliente` string ~64 chars), lo que produce una
    `BroadcastHashedRelation` de ~1.4 GiB que excede la memoria del
    executor (Photon `SparkOutOfMemoryError`). Se deja al optimizer
    elegir el algoritmo (sort-merge / shuffle-hash); como ambas tablas
    están clusterizadas por `Hash_Cliente`, el shuffle es eficiente.
    """
    sat_datos = (
        spark.readStream.table(
            f"{catalogo_plata}.{esquema_plata}.Sat_Transaccion_DatosEstables"
        )
        .filter(F.col("tipo_transaccion").isin(TIPO_DATM, TIPO_CATM))
        .select(
            "Hash_Transaccion",
            F.col("tipo_transaccion").alias("TipoTransaccion"),
            F.col("moneda_transaccion").alias("MonedaTransaccion"),
            F.col("estado_transaccion").alias("EstadoTransaccion"),
            F.col("canal_transaccion").alias("CanalTransaccion"),
            F.col("ClasificacionCanalATM"),
        )
    )

    # Solución B.2 — defensa en profundidad: cada fuente estática se deduplica
    # por Hash_Transaccion antes del join para garantizar que residuos de
    # duplicados upstream (anteriores a B.1 o por reingesta excepcional) no
    # produzcan fan-out en el resultado de Trx_ATM_Stream.
    sat_montos = spark.read.table(
        f"{catalogo_plata}.{esquema_plata}.Sat_Transaccion_Montos"
    ).select(
        "Hash_Transaccion",
        F.col("fecha_transaccion").alias("FechaClave"),
        F.col("monto_principal").alias("MontoPrincipal"),
        F.col("comision_transaccion").alias("ComisionTransaccion"),
        F.col("total_transaccion").alias("TotalTransaccion"),
        F.col("RangoMontoTransaccion"),
    ).dropDuplicates(["Hash_Transaccion"])

    hub_trx = spark.read.table(
        f"{catalogo_plata}.{esquema_plata}.Hub_Transaccion"
    ).select("Hash_Transaccion", "IdentificadorTransaccion").dropDuplicates(["Hash_Transaccion"])

    link_cli_trx = spark.read.table(
        f"{catalogo_plata}.{esquema_plata}.Link_Cliente_Transaccion"
    ).select("Hash_Transaccion", "Hash_Cliente").dropDuplicates(["Hash_Transaccion"])

    map_dom = spark.read.table(
        "Map_Cliente_Operacion_Dominante"
    ).select("Hash_Cliente", "DimIdCliente", "DimIdOperacion")

    resultado = (
        sat_datos
        .join(sat_montos, on="Hash_Transaccion", how="inner")
        .join(hub_trx, on="Hash_Transaccion", how="inner")
        .join(link_cli_trx, on="Hash_Transaccion", how="left")
        .join(map_dom, on="Hash_Cliente", how="left")
    )

    validar_columnas_oro(
        resultado,
        _COLUMNAS_CERRADAS_TRX_ATM,
        "Trx_ATM_Stream",
    )

    return resultado.select(*_COLUMNAS_CERRADAS_TRX_ATM)
