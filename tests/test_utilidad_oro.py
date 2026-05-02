"""Tests funcionales y estáticos para LSDPUtilidadOro.py — Helpers de la Medalla de Oro."""
import ast
import importlib.util
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


# ─── Fixture de SparkSession ──────────────────────────────────────────────────


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[*]")
        .appName("test_utilidad_oro")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


# ─── Helpers de carga del módulo ──────────────────────────────────────────────


def _ruta_utilities():
    return (
        Path(__file__).resolve().parent.parent
        / "src"
        / "LSDP_Lab_DataVault_DWH"
        / "utilities"
    )


def _importar_utilidad_oro():
    ruta = _ruta_utilities() / "LSDPUtilidadOro.py"
    spec = importlib.util.spec_from_file_location("LSDPUtilidadOro", ruta)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ─── Tests estáticos del archivo ─────────────────────────────────────────────


def test_archivo_existe():
    ruta = _ruta_utilities() / "LSDPUtilidadOro.py"
    assert ruta.exists(), "LSDPUtilidadOro.py no existe en utilities/"


def test_no_contiene_udfs():
    ruta = _ruta_utilities() / "LSDPUtilidadOro.py"
    codigo = ruta.read_text(encoding="utf-8")
    assert "@udf" not in codigo, "LSDPUtilidadOro no debe usar @udf"
    assert "udf(" not in codigo.lower(), "LSDPUtilidadOro no debe usar udf()"


def test_no_usa_spark_context():
    ruta = _ruta_utilities() / "LSDPUtilidadOro.py"
    codigo = ruta.read_text(encoding="utf-8")
    assert "sparkContext" not in codigo, "LSDPUtilidadOro no debe usar sparkContext"
    assert "sc." not in codigo, "LSDPUtilidadOro no debe usar sc."


def test_no_usa_cache_persist():
    ruta = _ruta_utilities() / "LSDPUtilidadOro.py"
    codigo = ruta.read_text(encoding="utf-8")
    assert ".cache()" not in codigo, "LSDPUtilidadOro no debe usar .cache()"
    assert ".persist()" not in codigo, "LSDPUtilidadOro no debe usar .persist()"


def test_no_usa_rdd():
    ruta = _ruta_utilities() / "LSDPUtilidadOro.py"
    codigo = ruta.read_text(encoding="utf-8")
    assert ".rdd" not in codigo, "LSDPUtilidadOro no debe usar .rdd"
    assert ".parallelize(" not in codigo, "LSDPUtilidadOro no debe usar .parallelize()"


def test_no_usa_imports_lsdp():
    ruta = _ruta_utilities() / "LSDPUtilidadOro.py"
    codigo = ruta.read_text(encoding="utf-8")
    assert "from pyspark import pipelines" not in codigo, "Módulo de utilidades no debe importar LSDP"
    assert "import databricks" not in codigo, "Módulo de utilidades no debe importar databricks SDK"


def test_define_helpers_requeridos():
    ruta = _ruta_utilities() / "LSDPUtilidadOro.py"
    tree = ast.parse(ruta.read_text(encoding="utf-8"))
    funciones = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert "obtener_ultimo_por_hash" in funciones, "Falta función obtener_ultimo_por_hash"
    assert "asignar_dim_id_estable" in funciones, "Falta función asignar_dim_id_estable"
    assert "seleccionar_operacion_dominante" in funciones, "Falta función seleccionar_operacion_dominante"
    assert "validar_columnas_oro" in funciones, "Falta función validar_columnas_oro"


# ─── Tests funcionales: obtener_ultimo_por_hash ───────────────────────────────


def test_obtener_ultimo_por_hash_devuelve_ultimo_registro(spark):
    mod = _importar_utilidad_oro()
    schema = StructType([
        StructField("Hash_Cliente", StringType(), False),
        StructField("valor", StringType(), True),
        StructField("FechaRegistro", StringType(), True),
        StructField("Hash_Diferenciador", StringType(), True),
    ])
    datos = [
        ("hash_a", "v1", "2024-01-01", "diff_aaa"),
        ("hash_a", "v2", "2024-01-02", "diff_bbb"),  # más reciente
        ("hash_b", "v3", "2024-01-01", "diff_ccc"),
    ]
    df = spark.createDataFrame(datos, schema)
    resultado = mod.obtener_ultimo_por_hash(df, "Hash_Cliente")

    assert resultado.count() == 2, "Debe devolver una fila por hash único"
    fila_a = resultado.filter(F.col("Hash_Cliente") == "hash_a").first()
    assert fila_a["valor"] == "v2", "Debe devolver el registro más reciente (2024-01-02)"


def test_obtener_ultimo_por_hash_es_determinista_ante_empates(spark):
    mod = _importar_utilidad_oro()
    schema = StructType([
        StructField("Hash_Operacion", StringType(), False),
        StructField("valor", StringType(), True),
        StructField("FechaRegistro", StringType(), True),
        StructField("Hash_Diferenciador", StringType(), True),
    ])
    # Empate en FechaRegistro: desempata por Hash_Diferenciador DESC (mayor gana)
    datos = [
        ("hash_a", "v1", "2024-01-01", "diff_aaa"),
        ("hash_a", "v2", "2024-01-01", "diff_zzz"),  # mayor Hash_Diferenciador → gana
    ]
    df = spark.createDataFrame(datos, schema)
    resultado1 = mod.obtener_ultimo_por_hash(df, "Hash_Operacion")
    resultado2 = mod.obtener_ultimo_por_hash(df, "Hash_Operacion")

    fila1 = resultado1.first()
    fila2 = resultado2.first()
    assert fila1["valor"] == fila2["valor"], "El resultado debe ser determinista en llamadas repetidas"
    assert fila1["valor"] == "v2", "Mayor Hash_Diferenciador debe ganar el desempate"


def test_obtener_ultimo_por_hash_columna_orden_personalizada(spark):
    mod = _importar_utilidad_oro()
    schema = StructType([
        StructField("Hash_Cliente", StringType(), False),
        StructField("valor", StringType(), True),
        StructField("MiColumnaOrden", StringType(), True),
        StructField("Hash_Diferenciador", StringType(), True),
    ])
    datos = [
        ("hash_a", "primero", "2023-06-01", "diff_1"),
        ("hash_a", "ultimo", "2023-12-31", "diff_2"),
    ]
    df = spark.createDataFrame(datos, schema)
    resultado = mod.obtener_ultimo_por_hash(df, "Hash_Cliente", orden_col="MiColumnaOrden")
    fila = resultado.first()
    assert fila["valor"] == "ultimo", "Debe usar la columna de orden personalizada"


# ─── Tests funcionales: asignar_dim_id_estable ───────────────────────────────


def test_asignar_dim_id_estable_es_estable_para_mismo_input(spark):
    mod = _importar_utilidad_oro()
    schema = StructType([
        StructField("Hash_Cliente", StringType(), False),
        StructField("nombre", StringType(), True),
    ])
    datos = [
        ("hash_b", "cliente_b"),
        ("hash_a", "cliente_a"),
        ("hash_c", "cliente_c"),
    ]
    df = spark.createDataFrame(datos, schema)
    resultado1 = mod.asignar_dim_id_estable(df, "Hash_Cliente", "DimIdCliente")
    resultado2 = mod.asignar_dim_id_estable(df, "Hash_Cliente", "DimIdCliente")

    ids1 = {r["Hash_Cliente"]: r["DimIdCliente"] for r in resultado1.collect()}
    ids2 = {r["Hash_Cliente"]: r["DimIdCliente"] for r in resultado2.collect()}
    assert ids1 == ids2, "Los IDs deben ser iguales para el mismo conjunto de hashes"


def test_asignar_dim_id_estable_genera_id_determinista_por_hash(spark):
    mod = _importar_utilidad_oro()
    schema = StructType([StructField("Hash_Cliente", StringType(), False)])
    datos = [("hash_c",), ("hash_a",), ("hash_b",)]
    df = spark.createDataFrame(datos, schema)
    resultado = mod.asignar_dim_id_estable(df, "Hash_Cliente", "DimIdCliente")

    mapa = {r["Hash_Cliente"]: r["DimIdCliente"] for r in resultado.collect()}
    # El ID debe depender solo del hash; hashes distintos ⇒ IDs distintos
    assert len(set(mapa.values())) == len(mapa), \
        "Cada hash distinto debe producir un DimId distinto (xxhash64 sin colisiones esperadas)"


def test_asignar_dim_id_estable_id_no_depende_del_conjunto(spark):
    """El ID de un hash debe ser idéntico aun si cambia el resto del conjunto.

    Propiedad clave para refresh incremental: con xxhash64, alta/baja de otras
    entidades NO reasigna el ID de las existentes.
    """
    mod = _importar_utilidad_oro()
    schema = StructType([StructField("Hash_Cliente", StringType(), False)])
    df1 = spark.createDataFrame([("hash_a",), ("hash_b",)], schema)
    df2 = spark.createDataFrame([("hash_a",), ("hash_b",), ("hash_c",)], schema)

    res1 = mod.asignar_dim_id_estable(df1, "Hash_Cliente", "DimIdCliente")
    res2 = mod.asignar_dim_id_estable(df2, "Hash_Cliente", "DimIdCliente")

    id_a_1 = res1.filter(F.col("Hash_Cliente") == "hash_a").first()["DimIdCliente"]
    id_a_2 = res2.filter(F.col("Hash_Cliente") == "hash_a").first()["DimIdCliente"]
    assert id_a_1 == id_a_2, \
        "El ID de un hash debe ser invariante a la composición del DataFrame"


def test_asignar_dim_id_estable_tipo_long(spark):
    mod = _importar_utilidad_oro()
    schema = StructType([StructField("Hash_Cliente", StringType(), False)])
    df = spark.createDataFrame([("hash_1",)], schema)
    resultado = mod.asignar_dim_id_estable(df, "Hash_Cliente", "DimIdCliente")

    campo = next(f for f in resultado.schema.fields if f.name == "DimIdCliente")
    from pyspark.sql.types import LongType
    assert isinstance(campo.dataType, LongType), "DimIdCliente debe ser LongType"


# ─── Tests funcionales: seleccionar_operacion_dominante ──────────────────────


def test_seleccionar_operacion_dominante_elige_mayor_secuencia_saldo(spark):
    mod = _importar_utilidad_oro()
    hub_schema = StructType([
        StructField("Hash_Operacion", StringType(), False),
        StructField("SecuenciaSaldo", IntegerType(), True),
    ])
    link_schema = StructType([
        StructField("Hash_Cliente", StringType(), False),
        StructField("Hash_Operacion", StringType(), False),
    ])
    hub_datos = [
        ("op_hash_1", 100),  # mayor SecuenciaSaldo → debe ganar
        ("op_hash_2", 50),
    ]
    link_datos = [
        ("cli_hash_1", "op_hash_1"),
        ("cli_hash_1", "op_hash_2"),
    ]
    df_hub = spark.createDataFrame(hub_datos, hub_schema)
    df_link = spark.createDataFrame(link_datos, link_schema)

    resultado = mod.seleccionar_operacion_dominante(df_hub, df_link)
    assert resultado.count() == 1, "Debe devolver una fila por Hash_Cliente"
    fila = resultado.first()
    assert fila["Hash_Operacion"] == "op_hash_1", "Debe elegir la operación con mayor SecuenciaSaldo"


def test_seleccionar_operacion_dominante_desempata_de_forma_determinista(spark):
    """Ante empate en SecuenciaSaldo, el desempate debe ser determinista y reproducible.

    La implementación basada en `groupBy().agg(max(struct(SecuenciaSaldo,
    Hash_Operacion)))` ordena DESC sobre ambos campos del struct, por lo
    que en caso de empate se elige el `Hash_Operacion` mayor lexicográfico.
    Es una desviación menor del criterio original (ASC) aprobada como parte
    de la mitigación del bloqueo de incrementalidad por Window.
    """
    mod = _importar_utilidad_oro()
    hub_schema = StructType([
        StructField("Hash_Operacion", StringType(), False),
        StructField("SecuenciaSaldo", IntegerType(), True),
    ])
    link_schema = StructType([
        StructField("Hash_Cliente", StringType(), False),
        StructField("Hash_Operacion", StringType(), False),
    ])
    hub_datos = [
        ("op_hash_aaa", 100),
        ("op_hash_zzz", 100),
    ]
    link_datos = [
        ("cli_hash_1", "op_hash_aaa"),
        ("cli_hash_1", "op_hash_zzz"),
    ]
    df_hub = spark.createDataFrame(hub_datos, hub_schema)
    df_link = spark.createDataFrame(link_datos, link_schema)

    resultado = mod.seleccionar_operacion_dominante(df_hub, df_link)
    fila = resultado.first()
    # max(struct(...)) → DESC sobre Hash_Operacion en empates
    assert fila["Hash_Operacion"] == "op_hash_zzz", \
        "Ante empate en SecuenciaSaldo debe elegir el Hash_Operacion mayor (DESC, criterio max-struct)"
    # Y debe ser determinista: ejecutarlo dos veces produce el mismo resultado.
    resultado2 = mod.seleccionar_operacion_dominante(df_hub, df_link)
    assert resultado2.first()["Hash_Operacion"] == fila["Hash_Operacion"]


def test_seleccionar_operacion_dominante_devuelve_una_fila_por_cliente(spark):
    mod = _importar_utilidad_oro()
    hub_schema = StructType([
        StructField("Hash_Operacion", StringType(), False),
        StructField("SecuenciaSaldo", IntegerType(), True),
    ])
    link_schema = StructType([
        StructField("Hash_Cliente", StringType(), False),
        StructField("Hash_Operacion", StringType(), False),
    ])
    hub_datos = [
        ("op1", 10),
        ("op2", 20),
        ("op3", 5),
    ]
    link_datos = [
        ("hash_cli_a", "op1"),
        ("hash_cli_a", "op2"),
        ("hash_cli_b", "op3"),
    ]
    df_hub = spark.createDataFrame(hub_datos, hub_schema)
    df_link = spark.createDataFrame(link_datos, link_schema)

    resultado = mod.seleccionar_operacion_dominante(df_hub, df_link)
    assert resultado.count() == 2, "Debe devolver exactamente una fila por Hash_Cliente distinto"


# ─── Tests funcionales: validar_columnas_oro ─────────────────────────────────


def test_validar_columnas_oro_falla_si_falta_columna(spark):
    mod = _importar_utilidad_oro()
    schema = StructType([
        StructField("columna_a", StringType(), True),
        StructField("columna_b", StringType(), True),
    ])
    df = spark.createDataFrame([("v1", "v2")], schema)

    with pytest.raises(ValueError, match="columna_c"):
        mod.validar_columnas_oro(df, ["columna_a", "columna_b", "columna_c"], "TestEntidad")


def test_validar_columnas_oro_mensaje_incluye_entidad(spark):
    mod = _importar_utilidad_oro()
    schema = StructType([StructField("col_a", StringType(), True)])
    df = spark.createDataFrame([("v1",)], schema)

    with pytest.raises(ValueError, match="MiEntidad"):
        mod.validar_columnas_oro(df, ["col_a", "col_faltante"], "MiEntidad")


def test_validar_columnas_oro_pasa_si_todas_presentes(spark):
    mod = _importar_utilidad_oro()
    schema = StructType([
        StructField("col_a", StringType(), True),
        StructField("col_b", StringType(), True),
    ])
    df = spark.createDataFrame([("v1", "v2")], schema)
    # No debe lanzar excepción
    mod.validar_columnas_oro(df, ["col_a", "col_b"], "TestEntidad")
