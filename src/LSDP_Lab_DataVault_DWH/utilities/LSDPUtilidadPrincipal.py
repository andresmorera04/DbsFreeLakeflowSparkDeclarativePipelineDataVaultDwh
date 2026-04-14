# ---------------------------------------------------------------------------
# LSDPUtilidadPrincipal.py — Funciones Helper Reutilizables
# ---------------------------------------------------------------------------
# Módulo Python puro (NO es source_code LSDP). Provee funciones de:
#   • Cálculo de hash SHA2 para Hubs/Links (256) y Satellites (512)
#   • Reordenamiento de columnas de Liquid Clustering
#   • Detección de cambios Append-Only para Satellites (procesar_satellite)
#   • Clasificación numérica por rangos de umbrales (clasificar_por_umbral)
# ---------------------------------------------------------------------------

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .LSDPConfiguracion import (
    HASH_HUB_LINK_BITS,
    HASH_SATELLITE_BITS,
    HASH_SEPARATOR,
)


def calcular_hash_hub(
    columnas: list[Column],
    bits: int = HASH_HUB_LINK_BITS,
    separador: str = HASH_SEPARATOR,
) -> Column:
    cols_str = [c.cast("string") for c in columnas]
    if len(cols_str) == 1:
        return F.sha2(cols_str[0], bits)
    return F.sha2(F.concat_ws(separador, *cols_str), bits)


def calcular_hash_diferenciador(
    hash_entidad: Column,
    *campos: Column,
) -> Column:
    campos_str = [c.cast("string") for c in campos]
    return F.sha2(
        F.concat_ws(HASH_SEPARATOR, hash_entidad, *campos_str),
        HASH_SATELLITE_BITS,
    )


def reordenar_columnas_lc(
    df: DataFrame,
    columnas_lc: list[str],
) -> DataFrame:
    resto = [c for c in df.columns if c not in columnas_lc]
    return df.select(*columnas_lc, *resto)


def procesar_satellite(
    spark,
    catalogo_plata: str,
    esquema_plata: str,
    nombre_sat: str,
    hash_col: str,
    datos_nuevos: DataFrame,
) -> DataFrame:
    """Detección de cambios Append-Only para Satellites Data Vault.

    Compara Hash_Diferenciador entre datos entrantes y último registro
    existente por llave hash de entidad. Retorna SOLO los registros nuevos
    o con cambios detectados.

    Primera ejecución (tabla no existe): retorna todos los registros entrantes.
    Ejecuciones posteriores: retorna solo registros donde hash difiere o son nuevos.
    """
    nombre_completo = f"{catalogo_plata}.{esquema_plata}.{nombre_sat}"
    try:
        existente = spark.read.table(nombre_completo)

        ventana = Window.partitionBy(hash_col).orderBy(F.col("FechaRegistro").desc())
        ultimo = (
            existente
            .withColumn("_rn", F.row_number().over(ventana))
            .filter(F.col("_rn") == 1)
            .select(
                F.col(hash_col).alias(f"__{hash_col}"),
                F.col("Hash_Diferenciador").alias("Hash_Existente"),
            )
        )

        resultado = (
            datos_nuevos
            .join(ultimo, datos_nuevos[hash_col] == ultimo[f"__{hash_col}"], "left")
            .filter(
                F.col("Hash_Existente").isNull()
                | (F.col("Hash_Diferenciador") != F.col("Hash_Existente"))
            )
            .drop(f"__{hash_col}", "Hash_Existente")
        )
        return resultado

    except Exception as exc:
        from pyspark.sql.utils import AnalysisException
        if isinstance(exc, AnalysisException):
            return datos_nuevos
        raise


def clasificar_por_umbral(
    columna: Column,
    umbrales: dict,
) -> Column:
    """Clasifica un valor numérico según rangos definidos en un diccionario de umbrales.

    Genera F.when().when()...otherwise("DESCONOCIDO") iterando sobre los rangos.
    Compatible con IntegerType, LongType y DoubleType.
    Valores fuera de todos los rangos retornan "DESCONOCIDO".
    """
    condicion = None
    for nombre, (minimo, maximo) in umbrales.items():
        rama = F.when(columna.between(minimo, maximo), F.lit(nombre))
        if condicion is None:
            condicion = rama
        else:
            condicion = condicion.when(columna.between(minimo, maximo), F.lit(nombre))
    return condicion.otherwise(F.lit("DESCONOCIDO"))
