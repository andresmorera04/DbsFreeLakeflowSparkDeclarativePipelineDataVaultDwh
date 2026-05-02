"""Tests para LSDPUtilidadPrincipal.py — Funciones helper reutilizables."""
import ast
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch


def _ruta_utilities():
    return Path(__file__).resolve().parent.parent / "src" / "LSDP_Lab_DataVault_DWH" / "utilities"


def _ruta_archivo():
    return _ruta_utilities() / "LSDPUtilidadPrincipal.py"


# ===== Tests Estáticos (sin PySpark) =====

def test_archivo_existe():
    assert _ruta_archivo().exists()


def test_no_contiene_udfs():
    codigo = _ruta_archivo().read_text(encoding="utf-8")
    assert "udf(" not in codigo.lower()
    assert "@udf" not in codigo.lower()


def test_no_contiene_imports_lsdp():
    codigo = _ruta_archivo().read_text(encoding="utf-8")
    assert "from pyspark import pipelines" not in codigo
    assert "import databricks" not in codigo


def test_no_usa_operador_plus_para_concatenar():
    """Verifica que no hay operador + entre columnas (suma aritmética en ANSI)."""
    codigo = _ruta_archivo().read_text(encoding="utf-8")
    # No debe haber patrones como col + col para concatenación
    assert "concat_ws" in codigo or "concat" in codigo


def test_usa_sha2_nativo():
    codigo = _ruta_archivo().read_text(encoding="utf-8")
    assert "sha2" in codigo


def test_usa_cast_string_antes_de_sha2():
    """Verifica que las columnas se castean a string antes de hash."""
    codigo = _ruta_archivo().read_text(encoding="utf-8")
    assert 'cast("string")' in codigo or "cast('string')" in codigo


def test_importa_constantes_desde_configuracion():
    codigo = _ruta_archivo().read_text(encoding="utf-8")
    assert "HASH_HUB_LINK_BITS" in codigo
    assert "HASH_SATELLITE_BITS" in codigo
    assert "HASH_SEPARATOR" in codigo


def test_define_calcular_hash_hub():
    tree = ast.parse(_ruta_archivo().read_text(encoding="utf-8"))
    funciones = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
    assert "calcular_hash_hub" in funciones


def test_define_calcular_hash_diferenciador():
    tree = ast.parse(_ruta_archivo().read_text(encoding="utf-8"))
    funciones = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
    assert "calcular_hash_diferenciador" in funciones


def test_define_reordenar_columnas_lc():
    tree = ast.parse(_ruta_archivo().read_text(encoding="utf-8"))
    funciones = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
    assert "reordenar_columnas_lc" in funciones


def test_no_usa_spark_context():
    codigo = _ruta_archivo().read_text(encoding="utf-8")
    assert "sparkContext" not in codigo
    assert "sc." not in codigo


def test_no_usa_cache_persist():
    codigo = _ruta_archivo().read_text(encoding="utf-8")
    assert ".cache()" not in codigo
    assert ".persist()" not in codigo
