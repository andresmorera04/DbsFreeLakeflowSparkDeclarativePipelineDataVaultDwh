# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPOroDimOperacion.py — Dim_Operacion como Vista Materializada Tipo 1
# ---------------------------------------------------------------------------
# Medalla de Oro · Dimensión de Operación/Cuenta
# Implementación: Vista Materializada con refresh completo.
# Fuente: Hub_Operacion + Sat_Operacion_DatosEstables + Sat_Operacion_Montos
#         + Sat_Operacion_FechasEvento
#
# Patrón:
#   1. Leer Hub_Operacion como base del join.
#   2. Aplicar obtener_ultimo_por_hash a cada Satellite de estado de operación.
#   3. LEFT JOIN desde Hub hacia los 3 Sats reducidos.
#   4. Asignar DimIdOperacion estable con asignar_dim_id_estable.
#   5. Proyectar solo las columnas del esquema cerrado (PascalCase de Oro).
#
# Restricciones:
#   • Preservar como string los clasificadores de Plata:
#     CategoriaSaldo, EstadoUtilizacionCredito, IndicadorSobregiro
#   • Excluir columnas de Bronce y metadata DV
#   • Sin operaciones de estado en memoria, APIs de bajo nivel ni paralelismo externo
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.LSDPConfiguracion import obtener_configuracion
from utilities.LSDPUtilidadOro import (
    asignar_dim_id_estable,
    obtener_ultimo_por_hash,
    validar_columnas_oro,
)

config = obtener_configuracion(spark)
catalogo_plata = config["catalogo_plata"]
esquema_plata = config["esquema_plata"]
catalogo_oro = config["catalogo_oro"]
esquema_oro = config["esquema_oro"]

# ─── Expectations ────────────────────────────────────────────────────────────

_validaciones_dim_operacion = {
    "dim_id_operacion_no_nulo": "DimIdOperacion IS NOT NULL",
    "hash_operacion_no_nulo": "Hash_Operacion IS NOT NULL",
}

# ─── Columnas cerradas del esquema de Dim_Operacion ──────────────────────────

_COLUMNAS_CERRADAS_DIM_OPERACION = [
    "DimIdOperacion",
    "Hash_Operacion",
    "IdentificadorCliente",
    "SecuenciaSaldo",
    "TipoCuenta",
    "MonedaCuenta",
    "EstadoCuenta",
    "ProductoCuenta",
    "SubproductoCuenta",
    "RiesgoCuenta",
    "RegionCuenta",
    "CategoriaSaldo",
    "EstadoUtilizacionCredito",
    "IndicadorSobregiro",
    "SaldoDisponible",
    "SaldoTotal",
    "LimiteCredito",
    "CreditoUtilizado",
    "RatioCuenta",
    "TasaInteres",
    "FechaAperturaCuenta",
    "FechaUltimoMovimiento",
    "FechaCierreCuenta",
    "FechaActualizacionCuenta",
]

# ─── Vista Materializada — Dim_Operacion ─────────────────────────────────────


@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.Dim_Operacion",
    cluster_by=["DimIdOperacion"],
    table_properties={
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days",
    },
)
@dp.expect_all_or_fail(_validaciones_dim_operacion)
def dim_operacion():
    """Dimensión de Operación/Cuenta Tipo 1 con DimIdOperacion estable.

    Aplica obtener_ultimo_por_hash a cada Satellite de estado antes del join.
    El join base es LEFT JOIN desde Hub_Operacion para preservar operaciones
    sin Satellites opcionales. Los clasificadores de Plata (CategoriaSaldo,
    EstadoUtilizacionCredito, IndicadorSobregiro) se preservan como StringType.
    """
    # ── 1. Leer Hub como base ────────────────────────────────────────────────
    hub = spark.read.table(f"{catalogo_plata}.{esquema_plata}.Hub_Operacion").select(
        "Hash_Operacion",
        "IdentificadorCliente",
        "SecuenciaSaldo",
    )

    # ── 2. Leer Satellites y reducir al último estado por hash ───────────────
    sat_datos = obtener_ultimo_por_hash(
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Operacion_DatosEstables"),
        "Hash_Operacion",
    )
    sat_montos = obtener_ultimo_por_hash(
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Operacion_Montos"),
        "Hash_Operacion",
    )
    sat_fechas = obtener_ultimo_por_hash(
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Operacion_FechasEvento"),
        "Hash_Operacion",
    )

    # ── 3. LEFT JOIN desde Hub hacia cada Sat ────────────────────────────────
    df = (
        hub
        .join(sat_datos.select(
            "Hash_Operacion",
            F.col("tipo_cuenta").alias("TipoCuenta"),
            F.col("moneda_cuenta").alias("MonedaCuenta"),
            F.col("estado_cuenta").alias("EstadoCuenta"),
            F.col("producto_cuenta").alias("ProductoCuenta"),
            F.col("subproducto_cuenta").alias("SubproductoCuenta"),
            F.col("riesgo_cuenta").alias("RiesgoCuenta"),
            F.col("region_cuenta").alias("RegionCuenta"),
            # Clasificadores calculados en Plata — se preservan como string
            F.col("CategoriaSaldo"),
            F.col("EstadoUtilizacionCredito"),
            F.col("IndicadorSobregiro"),
        ), on="Hash_Operacion", how="left")
        .join(sat_montos.select(
            "Hash_Operacion",
            F.col("saldo_disponible").alias("SaldoDisponible"),
            F.col("saldo_total").alias("SaldoTotal"),
            F.col("limite_credito").alias("LimiteCredito"),
            F.col("credito_utilizado").alias("CreditoUtilizado"),
            F.col("ratio_cuenta").alias("RatioCuenta"),
            F.col("tasa_interes").alias("TasaInteres"),
        ), on="Hash_Operacion", how="left")
        .join(sat_fechas.select(
            "Hash_Operacion",
            F.col("fecha_apertura_cuenta").alias("FechaAperturaCuenta"),
            F.col("fecha_ultimo_movimiento").alias("FechaUltimoMovimiento"),
            F.col("fecha_cierre_cuenta").alias("FechaCierreCuenta"),
            F.col("fecha_actualizacion_cuenta").alias("FechaActualizacionCuenta"),
        ), on="Hash_Operacion", how="left")
    )

    # ── 4. Asignar DimIdOperacion estable ────────────────────────────────────
    df = asignar_dim_id_estable(df, "Hash_Operacion", "DimIdOperacion")

    # ── 5. Validación de esquema cerrado ─────────────────────────────────────
    validar_columnas_oro(df, _COLUMNAS_CERRADAS_DIM_OPERACION, "Dim_Operacion")

    # ── 6. Proyectar esquema cerrado con LC en primera posición ───────────────
    return df.select(
        "DimIdOperacion",
        "Hash_Operacion",
        "IdentificadorCliente",
        "SecuenciaSaldo",
        "TipoCuenta",
        "MonedaCuenta",
        "EstadoCuenta",
        "ProductoCuenta",
        "SubproductoCuenta",
        "RiesgoCuenta",
        "RegionCuenta",
        "CategoriaSaldo",
        "EstadoUtilizacionCredito",
        "IndicadorSobregiro",
        "SaldoDisponible",
        "SaldoTotal",
        "LimiteCredito",
        "CreditoUtilizado",
        "RatioCuenta",
        "TasaInteres",
        "FechaAperturaCuenta",
        "FechaUltimoMovimiento",
        "FechaCierreCuenta",
        "FechaActualizacionCuenta",
    )
