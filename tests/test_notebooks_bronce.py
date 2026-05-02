"""Tests para los 3 notebooks de Bronce — Validación estática del código."""
import ast
from pathlib import Path

TRANSFORMATIONS = Path(__file__).resolve().parent.parent / "src" / "LSDP_Lab_DataVault_DWH" / "transformations"

NOTEBOOKS = {
    "CMSTFL": TRANSFORMATIONS / "LSDPBronceCMSTFL.py",
    "TRXPFL": TRANSFORMATIONS / "LSDPBronceTRXPFL.py",
    "BLNCFL": TRANSFORMATIONS / "LSDPBronceBLNCFL.py",
}


def _leer(origen):
    return NOTEBOOKS[origen].read_text(encoding="utf-8")


# ===== Existencia =====

def test_notebooks_existen():
    for origen, ruta in NOTEBOOKS.items():
        assert ruta.exists(), f"{origen}: {ruta} no existe"


# ===== Import LSDP correcto =====

def test_import_lsdp_correcto():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "from pyspark import pipelines as dp" in codigo, f"{origen}: import LSDP incorrecto"
        assert "import databricks.sdk" not in codigo, f"{origen}: import SDK prohibido"


# ===== Import de configuración =====

def test_importa_obtener_configuracion():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "obtener_configuracion" in codigo, f"{origen}: falta obtener_configuracion"
        assert "obtener_configuracion(spark)" in codigo, f"{origen}: debe invocar con spark"


# ===== Decoradores LSDP =====

def test_tiene_decorador_dp_table():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "@dp.table(" in codigo, f"{origen}: falta @dp.table"


def test_st_es_persistente():
    """La ST de Bronce debe ser PERSISTENTE (sin temporary=True)."""
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "temporary=True" not in codigo, f"{origen}: ST debe ser persistente, no temporal"


# ===== AutoLoader =====

def test_usa_autoloader():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "cloudFiles" in codigo, f"{origen}: falta cloudFiles (AutoLoader)"
        assert "readStream" in codigo, f"{origen}: falta readStream"


def test_autoloader_opciones():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "cloudFiles.inferColumnTypes" in codigo, f"{origen}: falta inferColumnTypes"
        assert "cloudFiles.schemaEvolutionMode" in codigo, f"{origen}: falta schemaEvolutionMode"
        assert "addNewColumns" in codigo, f"{origen}: falta addNewColumns"
        assert "cloudFiles.schemaLocation" in codigo, f"{origen}: falta schemaLocation"


# ===== Columna derivada FechaRegistroParquet =====

def test_genera_fecha_registro_parquet():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "FechaRegistroParquet" in codigo, f"{origen}: falta FechaRegistroParquet"
        assert "to_date" in codigo, f"{origen}: falta F.to_date"
        assert "concat_ws" in codigo, f"{origen}: falta F.concat_ws para fecha"


# ===== Materialized View =====

def test_mv_nombre_3_partes():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        # Debe contener el patrón de nombre de 3 partes
        assert f'.{origen}"' in codigo or f".{origen}\"" in codigo or f".{origen}}}" in codigo, \
            f"{origen}: MV debe tener nombre de 3 partes terminando en .{origen}"


def test_mv_no_usa_catalog_kwarg():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "catalog=" not in codigo, f"{origen}: NO usar catalog= como kwarg separado"
        assert "schema=" not in codigo.replace("schemaLocation", "").replace("schemaEvolutionMode", "").replace("schema_location", ""), \
            f"{origen}: NO usar schema= como kwarg separado"


# ===== Snapshot con broadcast =====

def test_usa_broadcast_para_snapshot():
    """La ST de Bronce ya NO usa MV de snapshot — no debe haber broadcast."""
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "broadcast" not in codigo, f"{origen}: NO debe usar F.broadcast (MV eliminada)"


# ===== Liquid Clustering =====

def test_cluster_by_fecha():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "cluster_by" in codigo, f"{origen}: falta cluster_by"
        assert "FechaRegistroParquet" in codigo, f"{origen}: cluster_by debe incluir FechaRegistroParquet"


# ===== Rescued data =====

def test_rescued_data_presente():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "_rescued_data" in codigo, f"{origen}: falta _rescued_data"


# ===== Restricciones Serverless =====

def test_no_cache_ni_persist():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert ".cache()" not in codigo, f"{origen}: NO .cache()"
        assert ".persist()" not in codigo, f"{origen}: NO .persist()"


def test_no_spark_context():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "sparkContext" not in codigo, f"{origen}: NO sparkContext"


def test_no_rdd():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert ".rdd" not in codigo, f"{origen}: NO .rdd"


def test_no_udf():
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        assert "udf(" not in codigo.lower(), f"{origen}: NO UDFs"


# ===== Rutas específicas por fuente =====

def test_rutas_especificas():
    mapping = {
        "CMSTFL": ("ruta_cmstfl", "schema_location_cmstfl"),
        "TRXPFL": ("ruta_trxpfl", "schema_location_trxpfl"),
        "BLNCFL": ("ruta_blncfl", "schema_location_blncfl"),
    }
    for origen, (ruta_key, schema_key) in mapping.items():
        codigo = _leer(origen)
        assert ruta_key in codigo, f"{origen}: debe usar {ruta_key}"
        assert schema_key in codigo, f"{origen}: debe usar {schema_key}"


# ===== Patrón consistente entre las 3 fuentes =====

def test_patron_consistente_1_funcion_lsdp():
    """Cada notebook debe tener exactamente 1 función decorada LSDP (ST persistente)."""
    for origen in NOTEBOOKS:
        codigo = _leer(origen)
        count_table = codigo.count("@dp.table(")
        count_mv = codigo.count("@dp.materialized_view(")
        assert count_table == 1, f"{origen}: debe tener exactamente 1 @dp.table, tiene {count_table}"
        assert count_mv == 0, f"{origen}: NO debe tener @dp.materialized_view, tiene {count_mv}"
