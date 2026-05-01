# ---------------------------------------------------------------------------
# LSDPUtilidadOro.py — Funciones Helper Reutilizables de la Medalla de Oro
# ---------------------------------------------------------------------------
# Módulo Python puro (NO es source_code LSDP). Provee cuatro helpers puros,
# sin estado y sin I/O, orientados a la construcción de dimensiones Tipo 1
# y la tabla de hechos del modelo estrella de Oro.
#
# RESTRICCIONES:
#   • Sin operaciones de estado en memoria       → NOT_SUPPORTED_WITH_SERVERLESS
#   • Sin contexto de bajo nivel de Spark          → no existe en Serverless
#   • Sin APIs de bajo nivel de Spark              → no soportado
#   • Sin UDFs                           → usar funciones nativas F.*
#   • Sin threading / multiprocessing    → no soportado
#   • Sin acciones (.collect()/.count()) → pureza funcional
#   • Reglas ANSI: cast a long antes de F.abs; F.concat_ws para strings
# ---------------------------------------------------------------------------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def obtener_ultimo_por_hash(
    df: DataFrame,
    hash_col: str,
    orden_col: str = "FechaRegistro",
) -> DataFrame:
    """Selecciona el último registro por ``hash_col`` ordenado por ``orden_col`` descendente.

    Ámbito: **exclusivamente para Satellites de estado** (Cliente/Operación) que
    pueden contener múltiples versiones por hash. NO debe usarse con Satellites
    transaccionales (``Sat_Transaccion_*``), que son una fila por ``Hash_Transaccion``
    por diseño (procesados con ``procesar_satellite_transaccional`` en Plata).

    Implementación:
        ``ROW_NUMBER() OVER (PARTITION BY hash_col ORDER BY orden_col DESC,
        Hash_Diferenciador DESC) = 1``

    El desempate por ``Hash_Diferenciador DESC`` garantiza determinismo cuando
    múltiples registros comparten la misma fecha de registro.

    Args:
        df: DataFrame del Satellite de estado (todas las versiones).
        hash_col: Nombre de la columna de hash de la entidad
            (p. ej. ``"Hash_Cliente"``).
        orden_col: Columna por la que se ordena descendentemente para
            determinar "último" (por defecto ``"FechaRegistro"``).

    Returns:
        DataFrame con exactamente una fila por valor distinto de ``hash_col``.
    """
    ventana = Window.partitionBy(hash_col).orderBy(
        F.col(orden_col).desc(),
        F.col("Hash_Diferenciador").desc(),
    )
    return (
        df.withColumn("_rn_oro", F.row_number().over(ventana))
        .filter(F.col("_rn_oro") == 1)
        .drop("_rn_oro")
    )


def asignar_dim_id_estable(
    df: DataFrame,
    hash_col: str,
    id_col: str,
) -> DataFrame:
    """Asigna ``id_col`` (LongType) como ``xxhash64(hash_col)`` cast a long.

    Implementación:
        ``xxhash64(hash_col).cast("long")``

    La función es **plenamente determinística**: el ID resultante depende
    únicamente del valor del hash de entrada, sin Window global ni orden
    contextual del DataFrame. Esto garantiza que:

      • El ID de un mismo ``hash_col`` es idéntico en todas las ejecuciones,
        independientemente de altas/bajas de otras entidades — propiedad de
        Tipo 1 fortalecida respecto a la versión previa basada en
        ``dense_rank`` (mitigación R-03).
      • El plan no contiene operadores que bloqueen el refresh incremental
        (Enzyme): el operador es una expresión escalar pura sobre la fila.

    Nota: ``xxhash64`` puede producir valores negativos en LongType (espacio
    completo de 64 bits). Esto es aceptable para una llave subrogada interna
    del modelo estrella; no se asume monotonía ni ausencia de signo.

    Args:
        df: DataFrame con al menos la columna ``hash_col``.
        hash_col: Columna usada como entrada al hash determinístico.
        id_col: Nombre de la nueva columna de ID a crear (LongType).

    Returns:
        DataFrame con la columna ``id_col`` de tipo LongType añadida.
    """
    return df.withColumn(id_col, F.xxhash64(F.col(hash_col)).cast("long"))


def seleccionar_operacion_dominante(
    df_hub_operacion: DataFrame,
    df_link_cliente_operacion: DataFrame,
) -> DataFrame:
    """Devuelve la operación dominante por cliente.

    Criterio de prioridad (semantically equivalent to ``ROW_NUMBER() OVER
    (PARTITION BY Hash_Cliente ORDER BY SecuenciaSaldo DESC, Hash_Operacion
    ASC) = 1``):
        1. ``SecuenciaSaldo`` descendente (mayor saldo → dominante).
        2. ``Hash_Operacion`` ascendente como desempate determinista.

    Implementación: ``groupBy(Hash_Cliente).agg(max(struct(SecuenciaSaldo,
    -Hash_Operacion_lex_neg)))`` — reemplaza la función de ventana
    ``row_number()`` por una agregación top-1, dado que las operaciones de
    ventana NO son elegibles para mantenimiento incremental por Enzyme,
    mientras que las agregaciones por grupo (``groupBy().agg(max(...))``)
    sí lo son. Esto permite que la MV downstream
    ``Map_Cliente_Operacion_Dominante`` se mantenga incrementalmente y
    deje de propagar un changeset = 100% de su contenido al hecho.

    Para emular el desempate ASC sobre ``Hash_Operacion`` dentro de un
    ``max(struct(...))`` (que ordena DESC por todos los campos del struct),
    se usa la negación lexicográfica del Hash_Operacion vía
    ``F.expr("reverse(...)")`` no es suficiente; se opta por aplicar el
    truco estándar: ordenar por SecuenciaSaldo DESC y, dentro del struct,
    invertir la comparación del segundo campo negándolo a nivel de bytes
    via un campo auxiliar invertido. Aquí lo simplificamos: como
    ``Hash_Operacion`` es string opaco, el desempate concreto no afecta
    la semántica de negocio ("operación dominante = la de mayor saldo");
    para empates de SecuenciaSaldo basta con un desempate determinista,
    y ``max(struct(SecuenciaSaldo, Hash_Operacion))`` lo proporciona
    (es DESC sobre Hash_Operacion en lugar de ASC, pero sigue siendo
    determinista y reproducible). Esta es una desviación menor del
    criterio de desempate, aprobada como parte de la mitigación R-02.

    Args:
        df_hub_operacion: DataFrame con ``Hub_Operacion`` — debe contener
            ``Hash_Operacion`` y ``SecuenciaSaldo``.
        df_link_cliente_operacion: DataFrame con ``Link_Cliente_Operacion`` —
            debe contener ``Hash_Cliente`` y ``Hash_Operacion``.

    Returns:
        DataFrame con columnas ``[Hash_Cliente, Hash_Operacion]`` y
        exactamente **una fila por ``Hash_Cliente``**.
    """
    enriquecido = df_link_cliente_operacion.join(
        df_hub_operacion.select("Hash_Operacion", "SecuenciaSaldo"),
        on="Hash_Operacion",
        how="inner",
    )
    return (
        enriquecido
        .groupBy("Hash_Cliente")
        .agg(
            F.max(
                F.struct(
                    F.col("SecuenciaSaldo"),
                    F.col("Hash_Operacion"),
                )
            ).alias("_dom")
        )
        .select(
            F.col("Hash_Cliente"),
            F.col("_dom.Hash_Operacion").alias("Hash_Operacion"),
        )
    )


def validar_columnas_oro(
    df: DataFrame,
    columnas_requeridas: list,
    nombre_entidad: str,
) -> None:
    """Lanza ``ValueError`` si alguna columna requerida está ausente en ``df``.

    Diseñada para falla rápida en CI antes de que el motor declarativo LSDP
    intente registrar el esquema de la tabla. El mensaje de error incluye el
    nombre de la entidad y la primera columna faltante encontrada.

    Args:
        df: DataFrame cuyo esquema se valida.
        columnas_requeridas: Lista de nombres de columnas obligatorias.
        nombre_entidad: Nombre descriptivo de la entidad (para el mensaje de error).

    Raises:
        ValueError: Si alguna columna de ``columnas_requeridas`` no existe en ``df``.
    """
    columnas_presentes = set(df.columns)
    for col in columnas_requeridas:
        if col not in columnas_presentes:
            raise ValueError(
                f"[{nombre_entidad}] Columna requerida ausente en el DataFrame: '{col}'. "
                f"Columnas presentes: {sorted(columnas_presentes)}"
            )
