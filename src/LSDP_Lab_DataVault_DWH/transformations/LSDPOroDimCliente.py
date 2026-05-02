# ---------------------------------------------------------------------------
# LSDPOroDimCliente.py — Dim_Cliente como Vista Materializada Tipo 1
# ---------------------------------------------------------------------------
# Medalla de Oro · Dimensión de Cliente
# Implementación: Vista Materializada con refresh completo (full refresh).
# Fuente: Hub_Cliente + Sat_Cliente_DatosEstables + Sat_Cliente_Contacto
#         + Sat_Cliente_Clasificacion + Sat_Cliente_Financiero
#
# Patrón:
#   1. Leer Hub_Cliente como base del join.
#   2. Aplicar obtener_ultimo_por_hash a cada Satellite de estado.
#   3. LEFT JOIN desde Hub hacia los 4 Sats reducidos (preserva clientes sin Sat opcional).
#   4. Asignar DimIdCliente estable con asignar_dim_id_estable.
#   5. Proyectar solo las columnas del esquema cerrado (PascalCase de Oro).
#
# Restricciones:
#   • Excluir columnas de partición de Bronce (año, mes, dia) y columnas de ingestión
#   • Excluir columnas de metadata interna del Data Vault (no exponer al consumidor)
#   • Preservar como string los indicadores categóricos de Plata (IndicadorVip, etc.)
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

_validaciones_dim_cliente = {
    "dim_id_cliente_no_nulo": "DimIdCliente IS NOT NULL",
    "hash_cliente_no_nulo": "Hash_Cliente IS NOT NULL",
}

# ─── Columnas cerradas del esquema de Dim_Cliente ────────────────────────────

_COLUMNAS_CERRADAS_DIM_CLIENTE = [
    "DimIdCliente",
    "Hash_Cliente",
    "IdentificadorCliente",
    "SexoCliente",
    "EdadCliente",
    "FechaNacimiento",
    "PaisResidencia",
    "RangoEtario",
    "CategoriaIngresos",
    "NombreCompletoCliente",
    "CorreoElectronico",
    "TelefonoPrincipal",
    "CiudadResidencia",
    "EstadoCivil",
    "OcupacionCliente",
    "TipoCliente",
    "SegmentoCliente",
    "RegionGeografica",
    "NivelRiesgo",
    "IndicadorVip",
    "EstadoKyc",
    "CalificacionCrediticia",
    "ScoreCliente",
    "IngresosCliente",
    "CantidadCuentas",
    "CantidadTransacciones",
    "FechaAperturaRelacion",
    "FechaUltimaActualizacion",
]

# ─── Vista Materializada — Dim_Cliente ───────────────────────────────────────


@dp.materialized_view(
    name=f"{catalogo_oro}.{esquema_oro}.Dim_Cliente",
    cluster_by=["DimIdCliente"],
    table_properties={
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days",
        "delta.logRetentionDuration": "interval 60 days",
    },
)
@dp.expect_all_or_fail(_validaciones_dim_cliente)
def dim_cliente():
    """Dimensión de Cliente Tipo 1 con atributos vigentes y DimIdCliente estable.

    Aplica obtener_ultimo_por_hash a cada Satellite de estado antes del join.
    El join base es LEFT JOIN desde Hub_Cliente para preservar clientes sin
    Satellites opcionales. Las llaves subrogadas son estables para el mismo
    conjunto de hashes (propiedad de Tipo 1 — mitigación R-03).
    """
    # ── 1. Leer Hub como base ────────────────────────────────────────────────
    hub = spark.read.table(f"{catalogo_plata}.{esquema_plata}.Hub_Cliente").select(
        "Hash_Cliente",
        "IdentificadorCliente",
    )

    # ── 2. Leer Satellites y reducir al último estado por hash ───────────────
    sat_datos = obtener_ultimo_por_hash(
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Cliente_DatosEstables"),
        "Hash_Cliente",
    )
    sat_contacto = obtener_ultimo_por_hash(
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Cliente_Contacto"),
        "Hash_Cliente",
    )
    sat_clasif = obtener_ultimo_por_hash(
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Cliente_Clasificacion"),
        "Hash_Cliente",
    )
    sat_fin = obtener_ultimo_por_hash(
        spark.read.table(f"{catalogo_plata}.{esquema_plata}.Sat_Cliente_Financiero"),
        "Hash_Cliente",
    )

    # ── 3. LEFT JOIN desde Hub hacia cada Sat (preserva clientes sin Sat) ────
    df = (
        hub
        .join(sat_datos.select(
            "Hash_Cliente",
            F.col("sexo_cliente").alias("SexoCliente"),
            F.col("edad_cliente").alias("EdadCliente"),
            F.col("fecha_nacimiento").alias("FechaNacimiento"),
            F.col("pais_residencia").alias("PaisResidencia"),
            F.col("RangoEtario"),
            F.col("CategoriaIngresos"),
        ), on="Hash_Cliente", how="left")
        .join(sat_contacto.select(
            "Hash_Cliente",
            F.col("nombre_completo_cliente").alias("NombreCompletoCliente"),
            F.col("correo_electronico").alias("CorreoElectronico"),
            F.col("telefono_principal").alias("TelefonoPrincipal"),
            F.col("ciudad_residencia").alias("CiudadResidencia"),
            F.col("estado_civil").alias("EstadoCivil"),
            F.col("ocupacion_cliente").alias("OcupacionCliente"),
        ), on="Hash_Cliente", how="left")
        .join(sat_clasif.select(
            "Hash_Cliente",
            F.col("tipo_cliente").alias("TipoCliente"),
            F.col("segmento_cliente").alias("SegmentoCliente"),
            F.col("region_geografica").alias("RegionGeografica"),
            F.col("nivel_riesgo").alias("NivelRiesgo"),
            F.col("indicador_vip").alias("IndicadorVip"),
            F.col("estado_kyc").alias("EstadoKyc"),
            F.col("calificacion_crediticia").alias("CalificacionCrediticia"),
        ), on="Hash_Cliente", how="left")
        .join(sat_fin.select(
            "Hash_Cliente",
            F.col("score_cliente").alias("ScoreCliente"),
            F.col("ingresos_cliente").alias("IngresosCliente"),
            F.col("cantidad_cuentas").alias("CantidadCuentas"),
            F.col("cantidad_transacciones").alias("CantidadTransacciones"),
            F.col("fecha_apertura_relacion").alias("FechaAperturaRelacion"),
            F.col("fecha_ultima_actualizacion").alias("FechaUltimaActualizacion"),
        ), on="Hash_Cliente", how="left")
    )

    # ── 4. Asignar DimIdCliente estable ──────────────────────────────────────
    df = asignar_dim_id_estable(df, "Hash_Cliente", "DimIdCliente")

    # ── 5. Validación de esquema cerrado (falla rápida en CI) ─────────────────
    validar_columnas_oro(df, _COLUMNAS_CERRADAS_DIM_CLIENTE, "Dim_Cliente")

    # ── 6. Proyectar esquema cerrado con LC en primera posición ───────────────
    return df.select(
        "DimIdCliente",
        "Hash_Cliente",
        "IdentificadorCliente",
        "SexoCliente",
        "EdadCliente",
        "FechaNacimiento",
        "PaisResidencia",
        "RangoEtario",
        "CategoriaIngresos",
        "NombreCompletoCliente",
        "CorreoElectronico",
        "TelefonoPrincipal",
        "CiudadResidencia",
        "EstadoCivil",
        "OcupacionCliente",
        "TipoCliente",
        "SegmentoCliente",
        "RegionGeografica",
        "NivelRiesgo",
        "IndicadorVip",
        "EstadoKyc",
        "CalificacionCrediticia",
        "ScoreCliente",
        "IngresosCliente",
        "CantidadCuentas",
        "CantidadTransacciones",
        "FechaAperturaRelacion",
        "FechaUltimaActualizacion",
    )
