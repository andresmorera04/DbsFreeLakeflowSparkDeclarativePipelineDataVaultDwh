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

    Primera ejecución (tabla no existe): retorna todos los registros entrantes
    deduplicados intra-batch por (hash_col, Hash_Diferenciador).
    Ejecuciones posteriores: retorna solo registros donde hash difiere o son nuevos.

    Deduplicación intra-batch OBLIGATORIA por (hash_col, Hash_Diferenciador):
    el microbatch puede contener múltiples filas con la misma llave de entidad
    Y el mismo Hash_Diferenciador (snapshots maestros repetidos día a día,
    Full Refresh que re-entrega historia). Filas con MISMO Hash_Diferenciador
    son colapsadas; filas con DISTINTO Hash_Diferenciador para la misma
    entidad se preservan como historia legítima.
    """
    nombre_completo = f"{catalogo_plata}.{esquema_plata}.{nombre_sat}"
    datos_nuevos = datos_nuevos.dropDuplicates([hash_col, "Hash_Diferenciador"])
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


def procesar_hub(
    spark,
    catalogo_plata: str,
    esquema_plata: str,
    nombre_hub: str,
    columnas_llave: list[str],
    datos_nuevos: DataFrame,
) -> DataFrame:
    """Detección de duplicados Append-Only para Hubs Data Vault.

    Lee la tabla Hub existente, ejecuta LEFT ANTI JOIN por las columnas de
    llave de negocio y retorna SOLO los registros cuyas llaves no existen aún.

    Primera ejecución (tabla no existe): retorna todos los registros entrantes
    deduplicados intra-batch.

    Deduplicación intra-batch OBLIGATORIA: el microbatch entrante puede contener
    múltiples filas con la misma llave de negocio (snapshots maestros repetidos
    día a día, Full Refresh que re-entrega historia acumulada). Se aplica
    dropDuplicates ANTES del LEFT ANTI JOIN para garantizar unicidad por llave.
    """
    nombre_completo = f"{catalogo_plata}.{esquema_plata}.{nombre_hub}"
    datos_nuevos = datos_nuevos.dropDuplicates(columnas_llave)
    try:
        existente = spark.read.table(nombre_completo).select(*columnas_llave)
        return datos_nuevos.join(existente, columnas_llave, "left_anti")
    except Exception as exc:
        from pyspark.sql.utils import AnalysisException
        if isinstance(exc, AnalysisException):
            return datos_nuevos
        raise


def procesar_link(
    spark,
    catalogo_plata: str,
    esquema_plata: str,
    nombre_link: str,
    columnas_hash: list[str],
    datos_nuevos: DataFrame,
) -> DataFrame:
    """Detección de duplicados Append-Only para Links Data Vault.

    Lee la tabla Link existente, ejecuta LEFT ANTI JOIN por la combinación de
    hashes de los dos Hubs y retorna SOLO las combinaciones nuevas.

    Primera ejecución (tabla no existe): retorna todos los registros entrantes
    deduplicados intra-batch.

    Deduplicación intra-batch OBLIGATORIA: el microbatch entrante puede contener
    múltiples filas con la misma combinación Hash_{hub1} + Hash_{hub2} (p. ej.
    BLNCFL con saldos repetidos día a día). Se aplica dropDuplicates ANTES del
    LEFT ANTI JOIN para garantizar unicidad de la relación.
    """
    nombre_completo = f"{catalogo_plata}.{esquema_plata}.{nombre_link}"
    datos_nuevos = datos_nuevos.dropDuplicates(columnas_hash)
    try:
        existente = spark.read.table(nombre_completo).select(*columnas_hash)
        return datos_nuevos.join(existente, columnas_hash, "left_anti")
    except Exception as exc:
        from pyspark.sql.utils import AnalysisException
        if isinstance(exc, AnalysisException):
            return datos_nuevos
        raise


def procesar_satellite_transaccional(
    spark,
    catalogo_plata: str,
    esquema_plata: str,
    nombre_sat: str,
    hash_col: str,
    fecha_col: str,
    datos_nuevos: DataFrame,
) -> DataFrame:
    """Primera-escritura-gana Append-Only para Satellites transaccionales (B.1).

    Una transacción ATM es un hecho inmutable: su estado queda fijado en el
    momento en que se confirma. Por tanto el grano de estos Satellites es
    exactamente **una fila por** ``hash_col`` (= IdentificadorTransaccion hasheado).

    Deduplicación:
      1. **Intra-batch** — ``dropDuplicates([hash_col])``: colapsa filas
         duplicadas dentro del mismo microbatch (p. ej. AutoLoader re-entrega
         el mismo archivo parquet).
      2. **Cross-batch** — LEFT ANTI JOIN por ``[hash_col]`` contra el snapshot
         actual de la tabla: descarta transacciones ya registradas en ejecuciones
         previas, independientemente de si ``fecha_transaccion`` llegó con un
         valor distinto (re-generaciones de laboratorio, re-ingestas del mismo
         parquet con fechas diferentes).

    Motivación del cambio respecto a la versión anterior:
      La versión previa deduplicaba por ``[hash_col, fecha_col]``. Como el
      generador de lab asigna un ``TRXDT`` diferente por cada ejecución, el
      mismo ``TRXID`` con fecha nueva pasaba el ANTI JOIN como "registro
      nuevo", acumulando N versiones en el satélite y propagando ``Q = N``
      en ``Hec_Transacciones_ATM`` (Q = 11 observado en producción de lab).

    Nota: ``fecha_col`` se mantiene en la firma por compatibilidad con los
    sitios de llamada existentes; no participa en la lógica de deduplicación.

    Primera ejecución (tabla no existe): retorna todos los registros entrantes
    deduplicados intra-batch.
    """
    nombre_completo = f"{catalogo_plata}.{esquema_plata}.{nombre_sat}"
    datos_nuevos = datos_nuevos.dropDuplicates([hash_col])
    try:
        existente = spark.read.table(nombre_completo).select(hash_col)
        return datos_nuevos.join(existente, [hash_col], "left_anti")
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
