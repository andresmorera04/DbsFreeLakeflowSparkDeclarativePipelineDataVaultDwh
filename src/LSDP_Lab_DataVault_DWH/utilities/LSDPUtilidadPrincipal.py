# ---------------------------------------------------------------------------
# LSDPUtilidadPrincipal.py — Funciones Helper Reutilizables
# ---------------------------------------------------------------------------
# Módulo Python puro (NO es source_code LSDP). Provee funciones de:
#   • Cálculo de hash SHA2 para Hubs/Links (256) y Satellites (512)
#   • Reordenamiento de columnas de Liquid Clustering
# ---------------------------------------------------------------------------

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from LSDPConfiguracion import (
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
