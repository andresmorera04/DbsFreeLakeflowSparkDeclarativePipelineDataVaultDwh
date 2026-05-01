# ---------------------------------------------------------------------------
# LSDPOroDimTiempo.py — Dim_Tiempo como Vista Materializada Incremental
# ---------------------------------------------------------------------------
# Medalla de Oro · Dimensión de Tiempo
# Implementación: Vista Materializada con refresh incremental nativo de LSDP.
# Fuente: valores distintos de Sat_Transaccion_Montos.fecha_transaccion.
#
# Restricciones de incremental refresh (operadores soportados):
#   • select, distinct, withColumn con funciones determinísticas
#   • F.when/otherwise para derivar atributos
#   • Operadores NO soportados: joins, funciones de ventana, funciones no determinísticas
#   • Funciones de fecha del sistema prohibidas (no determinísticas por naturaleza)
#   • Operaciones de estado en memoria y APIs de bajo nivel prohibidas
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.LSDPConfiguracion import obtener_configuracion

config = obtener_configuracion(spark)
catalogo_plata = config["catalogo_plata"]
esquema_plata = config["esquema_plata"]
catalogo_oro = config["catalogo_oro"]
esquema_oro = config["esquema_oro"]

# ─── Expectations ────────────────────────────────────────────────────────────

_validaciones_dim_tiempo = {
    "fecha_clave_no_nula": "FechaClave IS NOT NULL",
    "mes_valido": "Mes BETWEEN 1 AND 12",
}

# ─── Vista Materializada — Dim_Tiempo ────────────────────────────────────────


@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.Dim_Tiempo",
    cluster_by=["FechaClave"],
    table_properties={
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days",
    },
)
@dp.expect_all_or_fail(_validaciones_dim_tiempo)
@dp.expect("anio_valido", "Anio BETWEEN 1900 AND 2100")
def dim_tiempo():
    """Vista Materializada incremental de la dimensión de tiempo.

    Lee los valores distintos de fecha_transaccion del Satellite de montos
    y deriva todos los atributos calendario con funciones determinísticas.
    No contiene lógica imperativa de fechas ni funciones de fecha del sistema.
    """
    return (
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Transaccion_Montos")
        .select(F.col("fecha_transaccion").alias("FechaClave"))
        .distinct()
        .withColumn("Anio", F.year("FechaClave"))
        .withColumn("Mes", F.month("FechaClave"))
        .withColumn("Dia", F.dayofmonth("FechaClave"))
        .withColumn("Trimestre", F.quarter("FechaClave"))
        .withColumn("Semestre",
            F.when(F.quarter("FechaClave") <= 2, 1).otherwise(2))
        .withColumn("DiaSemana", F.dayofweek("FechaClave"))
        .withColumn("NombreDia",
            F.when(F.dayofweek("FechaClave") == 2, "Lunes")
             .when(F.dayofweek("FechaClave") == 3, "Martes")
             .when(F.dayofweek("FechaClave") == 4, "Miércoles")
             .when(F.dayofweek("FechaClave") == 5, "Jueves")
             .when(F.dayofweek("FechaClave") == 6, "Viernes")
             .when(F.dayofweek("FechaClave") == 7, "Sábado")
             .otherwise("Domingo"))
        .withColumn("NombreMes",
            F.when(F.month("FechaClave") == 1, "Enero")
             .when(F.month("FechaClave") == 2, "Febrero")
             .when(F.month("FechaClave") == 3, "Marzo")
             .when(F.month("FechaClave") == 4, "Abril")
             .when(F.month("FechaClave") == 5, "Mayo")
             .when(F.month("FechaClave") == 6, "Junio")
             .when(F.month("FechaClave") == 7, "Julio")
             .when(F.month("FechaClave") == 8, "Agosto")
             .when(F.month("FechaClave") == 9, "Septiembre")
             .when(F.month("FechaClave") == 10, "Octubre")
             .when(F.month("FechaClave") == 11, "Noviembre")
             .otherwise("Diciembre"))
        .withColumn("EsFinSemana",
            F.when(F.dayofweek("FechaClave").isin(1, 7), F.lit(True))
             .otherwise(F.lit(False)))
        .withColumn("DiaDelAnio", F.dayofyear("FechaClave"))
        .withColumn("SemanaDelAnio", F.weekofyear("FechaClave"))
        # Reordenar LC primero (FechaClave ya es la primera columna del select)
        .select(
            "FechaClave",
            "Anio",
            "Mes",
            "Dia",
            "Trimestre",
            "Semestre",
            "DiaSemana",
            "NombreDia",
            "NombreMes",
            "EsFinSemana",
            "DiaDelAnio",
            "SemanaDelAnio",
        )
    )
