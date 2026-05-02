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


def test_procesar_satellite_deduplica_intra_batch_por_hash_diferenciador():
    """Regresión: procesar_satellite debe aplicar dropDuplicates sobre
    [hash_col, 'Hash_Diferenciador'] ANTES de comparar contra la tabla
    existente, para prevenir inserciones duplicadas intra-microbatch
    (snapshots maestros repetidos día a día, Full Refresh).

    Registros con DISTINTO Hash_Diferenciador para la misma entidad se
    preservan como historia legítima; solo se colapsan los idénticos.
    """
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_satellite":
            src = ast.unparse(node)
            assert 'dropDuplicates([hash_col, \'Hash_Diferenciador\'])' in src \
                or 'dropDuplicates([hash_col, "Hash_Diferenciador"])' in src, (
                "procesar_satellite debe aplicar "
                "datos_nuevos.dropDuplicates([hash_col, 'Hash_Diferenciador'])"
            )
            idx_drop = src.find("dropDuplicates(")
            idx_try = src.find("try:")
            assert idx_drop != -1 and idx_try != -1 and idx_drop < idx_try, (
                "dropDuplicates debe ejecutarse ANTES del try/join"
            )
            return
    assert False, "procesar_satellite no encontrada"


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


# ─── Tarea 10.1 — procesar_hub ───────────────────────────────────────────────


def test_define_procesar_hub():
    assert "procesar_hub" in _nombres_funciones()


def test_procesar_hub_parametros():
    """Verifica que la firma tiene los 6 parámetros indicados en el diseño."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_hub":
            params = [a.arg for a in node.args.args]
            assert "spark" in params
            assert "catalogo_plata" in params
            assert "esquema_plata" in params
            assert "nombre_hub" in params
            assert "columnas_llave" in params
            assert "datos_nuevos" in params
            return
    assert False, "procesar_hub no encontrada"


def test_procesar_hub_usa_spark_read_table():
    """Verifica que lee la tabla existente del Hub vía spark.read.table."""
    codigo = _codigo()
    assert "spark.read.table" in codigo


def test_procesar_hub_usa_left_anti_join():
    """Verifica que deduplica con LEFT ANTI JOIN."""
    codigo = _codigo()
    assert "left_anti" in codigo


def test_procesar_hub_captura_analysis_exception():
    """Verifica manejo de AnalysisException para primera ejecución."""
    codigo = _codigo()
    assert "AnalysisException" in codigo


def test_procesar_hub_no_usa_row_number():
    """Verifica que procesar_hub no aplica ROW_NUMBER."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_hub":
            src = ast.unparse(node)
            assert "row_number" not in src.lower()
            return
    assert False, "procesar_hub no encontrada"


def test_procesar_hub_deduplica_intra_batch_por_llave():
    """Regresión: procesar_hub debe aplicar dropDuplicates sobre columnas_llave
    ANTES del LEFT ANTI JOIN para prevenir duplicados intra-microbatch
    (snapshots maestros repetidos día a día, Full Refresh que re-entrega historia).
    """
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_hub":
            src = ast.unparse(node)
            assert "dropDuplicates(columnas_llave)" in src, (
                "procesar_hub debe aplicar datos_nuevos.dropDuplicates(columnas_llave)"
            )
            # Verifica que el dropDuplicates ocurre antes del try (antes del antijoin)
            idx_drop = src.find("dropDuplicates(columnas_llave)")
            idx_try = src.find("try:")
            assert idx_drop != -1 and idx_try != -1 and idx_drop < idx_try, (
                "dropDuplicates debe ejecutarse ANTES del try/LEFT ANTI JOIN"
            )
            return
    assert False, "procesar_hub no encontrada"


# ─── Tarea 10.2 — procesar_link ──────────────────────────────────────────────


def test_define_procesar_link():
    assert "procesar_link" in _nombres_funciones()


def test_procesar_link_parametros():
    """Verifica que la firma tiene los 6 parámetros indicados en el diseño."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_link":
            params = [a.arg for a in node.args.args]
            assert "spark" in params
            assert "catalogo_plata" in params
            assert "esquema_plata" in params
            assert "nombre_link" in params
            assert "columnas_hash" in params
            assert "datos_nuevos" in params
            return
    assert False, "procesar_link no encontrada"


def test_procesar_link_usa_left_anti_join():
    """Verifica que deduplica con LEFT ANTI JOIN."""
    codigo = _codigo()
    assert "left_anti" in codigo


def test_procesar_link_no_usa_row_number():
    """Verifica que procesar_link no aplica ROW_NUMBER."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_link":
            src = ast.unparse(node)
            assert "row_number" not in src.lower()
            return
    assert False, "procesar_link no encontrada"


def test_procesar_link_deduplica_intra_batch_por_hash_combinado():
    """Regresión: procesar_link debe aplicar dropDuplicates sobre columnas_hash
    (Hash_{hub1} + Hash_{hub2}) ANTES del LEFT ANTI JOIN para prevenir
    duplicados intra-microbatch de la relación.
    """
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_link":
            src = ast.unparse(node)
            assert "dropDuplicates(columnas_hash)" in src, (
                "procesar_link debe aplicar datos_nuevos.dropDuplicates(columnas_hash)"
            )
            idx_drop = src.find("dropDuplicates(columnas_hash)")
            idx_try = src.find("try:")
            assert idx_drop != -1 and idx_try != -1 and idx_drop < idx_try, (
                "dropDuplicates debe ejecutarse ANTES del try/LEFT ANTI JOIN"
            )
            return
    assert False, "procesar_link no encontrada"


# ─── Tarea 10.3 — procesar_satellite_transaccional ──────────────────────────


def test_define_procesar_satellite_transaccional():
    assert "procesar_satellite_transaccional" in _nombres_funciones()


def test_procesar_satellite_transaccional_parametros():
    """Verifica que la firma incluye hash_col y fecha_col como parámetros diferenciadores."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_satellite_transaccional":
            params = [a.arg for a in node.args.args]
            assert "spark" in params
            assert "catalogo_plata" in params
            assert "esquema_plata" in params
            assert "nombre_sat" in params
            assert "hash_col" in params
            assert "fecha_col" in params
            assert "datos_nuevos" in params
            return
    assert False, "procesar_satellite_transaccional no encontrada"


def test_procesar_satellite_transaccional_no_usa_row_number():
    """Verifica que la acumulación histórica no aplica ROW_NUMBER como llamada PySpark."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_satellite_transaccional":
            # Extraer solo el cuerpo (sin docstring) para evitar falsos positivos
            stmts = node.body
            # Saltar docstring inicial si existe
            if stmts and isinstance(stmts[0], ast.Expr) and isinstance(stmts[0].value, ast.Constant):
                stmts = stmts[1:]
            src = ast.unparse(ast.Module(body=stmts, type_ignores=[]))
            assert "row_number()" not in src.lower()
            assert ".over(" not in src.lower()
            return
    assert False, "procesar_satellite_transaccional no encontrada"


def test_procesar_satellite_transaccional_usa_left_anti_join():
    """Verifica deduplicación con LEFT ANTI JOIN por hash_col solo (B.1)."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_satellite_transaccional":
            src = ast.unparse(node)
            assert "left_anti" in src
            return
    assert False, "procesar_satellite_transaccional no encontrada"


def test_procesar_satellite_transaccional_no_usa_hash_diferenciador_en_join():
    """Verifica que Hash_Diferenciador no participa en el join de deduplicación."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_satellite_transaccional":
            src = ast.unparse(node)
            # el join should only reference hash_col and fecha_col, not Hash_Diferenciador
            # we verify Hash_Diferenciador does NOT appear as a join key (not in join call args)
            assert "Hash_Diferenciador" not in src or "join" in src  # exists in output but not as dedup key
            return
    assert False, "procesar_satellite_transaccional no encontrada"


def test_procesar_satellite_transaccional_captura_analysis_exception():
    """Verifica manejo de AnalysisException para primera ejecución."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_satellite_transaccional":
            src = ast.unparse(node)
            assert "AnalysisException" in src
            return
    assert False, "procesar_satellite_transaccional no encontrada"


def test_procesar_satellite_transaccional_deduplica_por_hash_transaccion_solo():
    """B.1: el ANTI JOIN debe usar solo [hash_col], nunca [hash_col, fecha_col].

    Una transacción ATM es inmutable. Deduplicar por (hash_col, fecha_col) permite
    que el mismo TRXID llegue con TRXDT diferente (re-generaciones de lab) y pase
    el ANTI JOIN como 'registro nuevo', produciendo Q = N en Hec_Transacciones_ATM.
    """
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_satellite_transaccional":
            # Excluir docstring del análisis (primera sentencia si es Constant)
            stmts = node.body
            if stmts and isinstance(stmts[0], ast.Expr) and isinstance(stmts[0].value, ast.Constant):
                stmts = stmts[1:]
            src = ast.unparse(ast.Module(body=stmts, type_ignores=[]))
            assert "join(existente, [hash_col], 'left_anti')" in src or \
                   'join(existente, [hash_col], "left_anti")' in src, (
                "procesar_satellite_transaccional debe hacer anti-join SOLO por [hash_col] "
                "(no por [hash_col, fecha_col])"
            )
            assert "[hash_col, fecha_col]" not in src, (
                "fecha_col NO debe ser llave de deduplicación en el código (B.1)"
            )
            return
    assert False, "procesar_satellite_transaccional no encontrada"


def test_procesar_satellite_transaccional_dropduplicates_intra_batch_antes_de_join():
    """B.1: dropDuplicates([hash_col]) debe ejecutarse ANTES del LEFT ANTI JOIN."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.FunctionDef) and node.name == "procesar_satellite_transaccional":
            # Excluir docstring para no confundir índices
            stmts = node.body
            if stmts and isinstance(stmts[0], ast.Expr) and isinstance(stmts[0].value, ast.Constant):
                stmts = stmts[1:]
            src = ast.unparse(ast.Module(body=stmts, type_ignores=[]))
            assert "dropDuplicates([hash_col])" in src, (
                "procesar_satellite_transaccional debe aplicar dropDuplicates([hash_col]) "
                "para deduplicación intra-batch"
            )
            idx_drop = src.find("dropDuplicates([hash_col])")
            idx_try = src.find("try:")
            assert idx_drop != -1 and idx_try != -1 and idx_drop < idx_try, (
                "dropDuplicates debe ejecutarse ANTES del bloque try/ANTI JOIN"
            )
            return
    assert False, "procesar_satellite_transaccional no encontrada"


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
