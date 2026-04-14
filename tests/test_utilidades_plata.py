"""Tests para las funciones nuevas de LSDPUtilidadPrincipal.py — Plata.

Tests estáticos (AST + lectura de código) para:
  - procesar_satellite()  (Tarea 1.1)
  - clasificar_por_umbral()  (Tarea 1.2)
"""
import ast
from pathlib import Path


def _ruta_archivo():
    return (
        Path(__file__).resolve().parent.parent
        / "src"
        / "LSDP_Lab_DataVault_DWH"
        / "utilities"
        / "LSDPUtilidadPrincipal.py"
    )


def _codigo():
    return _ruta_archivo().read_text(encoding="utf-8")


def _tree():
    return ast.parse(_codigo())


def _nombres_funciones():
    return [n.name for n in ast.walk(_tree()) if isinstance(n, ast.FunctionDef)]


# ─── Tarea 1.1 — procesar_satellite ─────────────────────────────────────────


def test_define_procesar_satellite():
    assert "procesar_satellite" in _nombres_funciones()


def test_procesar_satellite_parametros():
    """Verifica que la firma tiene los 6 parámetros indicados en el diseño."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_satellite":
            params = [a.arg for a in node.args.args]
            assert "spark" in params
            assert "catalogo_plata" in params
            assert "esquema_plata" in params
            assert "nombre_sat" in params
            assert "hash_col" in params
            assert "datos_nuevos" in params
            return
    assert False, "procesar_satellite no encontrada"


def test_procesar_satellite_captura_analysis_exception():
    """Verifica manejo de AnalysisException para primera ejecución."""
    codigo = _codigo()
    assert "AnalysisException" in codigo


def test_procesar_satellite_usa_row_number():
    """Verifica que usa ROW_NUMBER para obtener último registro por entidad."""
    codigo = _codigo()
    assert "row_number" in codigo or "ROW_NUMBER" in codigo


def test_procesar_satellite_usa_window_partition_by():
    """Verifica Window y partitionBy para detección de cambios."""
    codigo = _codigo()
    assert "partitionBy" in codigo
    assert "Window" in codigo


def test_procesar_satellite_usa_left_join_o_filtro():
    """Verifica lógica de join para detectar registros nuevos/cambiados."""
    codigo = _codigo()
    assert "join" in codigo


def test_procesar_satellite_filtra_hash_diferenciador():
    """Verifica que compara Hash_Diferenciador para detectar cambios."""
    codigo = _codigo()
    assert "Hash_Diferenciador" in codigo
    assert "Hash_Existente" in codigo


def test_procesar_satellite_no_contiene_udfs():
    codigo = _codigo()
    assert "@udf" not in codigo.lower()
    assert "udf(" not in codigo.lower()


def test_procesar_satellite_no_usa_cache():
    codigo = _codigo()
    assert ".cache()" not in codigo


def test_procesar_satellite_no_usa_spark_context():
    codigo = _codigo()
    assert "sparkContext" not in codigo
    assert "sc." not in codigo


def test_procesar_satellite_no_importa_lsdp():
    """Las funciones nuevas no deben importar LSDP."""
    codigo = _codigo()
    assert "from pyspark import pipelines" not in codigo
    assert "import databricks" not in codigo


def test_procesar_satellite_usa_order_by_fecha_registro():
    """Verifica que el orden es por FechaRegistro descendente."""
    codigo = _codigo()
    assert "FechaRegistro" in codigo
    # debe haber desc() o DESC para obtener el último registro
    assert "desc()" in codigo or ".desc" in codigo


# ─── Tarea 1.2 — clasificar_por_umbral ──────────────────────────────────────


def test_define_clasificar_por_umbral():
    assert "clasificar_por_umbral" in _nombres_funciones()


def test_clasificar_por_umbral_parametros():
    """Verifica firma: columna y umbrales."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "clasificar_por_umbral":
            params = [a.arg for a in node.args.args]
            assert "columna" in params
            assert "umbrales" in params
            return
    assert False, "clasificar_por_umbral no encontrada"


def test_clasificar_por_umbral_usa_when():
    """Verifica que construye cadena F.when / .when dinámicamente."""
    codigo = _codigo()
    assert "when" in codigo


def test_clasificar_por_umbral_usa_between():
    """Verifica que usa .between() para rangos inclusivos."""
    codigo = _codigo()
    assert "between" in codigo


def test_clasificar_por_umbral_usa_otherwise():
    """Verifica que retorna DESCONOCIDO para valores fuera de rango."""
    codigo = _codigo()
    assert "otherwise" in codigo
    assert "DESCONOCIDO" in codigo


def test_clasificar_por_umbral_no_importa_lsdp():
    """Las funciones nuevas no deben importar LSDP."""
    codigo = _codigo()
    assert "from pyspark import pipelines" not in codigo


def test_clasificar_por_umbral_construye_con_iteracion():
    """Verifica que itera sobre el diccionario de umbrales dinámicamente."""
    # La función debe iterar sobre umbrales, no tener ramas if-else hardcodeadas
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "clasificar_por_umbral":
            # Debe contener un for loop
            for child in ast.walk(node):
                if isinstance(child, ast.For):
                    return
    assert False, "clasificar_por_umbral no itera sobre umbrales"


def test_clasificar_por_umbral_no_usa_udf():
    codigo = _codigo()
    assert "@udf" not in codigo.lower()
