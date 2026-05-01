"""Tests estáticos para los notebooks de exploración (GenerarParquets)."""
from pathlib import Path

EXPLORATIONS = (
    Path(__file__).resolve().parent.parent
    / "src" / "LSDP_Lab_DataVault_DWH" / "explorations" / "GenerarParquets"
)

NB_TRANSACCIONAL = EXPLORATIONS / "NbGenerarTransaccionalCliente.py"


def _leer_transaccional():
    return NB_TRANSACCIONAL.read_text(encoding="utf-8")


# ===== Existencia =====

def test_notebook_transaccional_existe():
    assert NB_TRANSACCIONAL.exists(), f"No existe: {NB_TRANSACCIONAL}"


# ===== Widget rutaRelativaParquetsExistentes =====

def test_widget_ruta_parquets_existentes_definido():
    """El widget rutaRelativaParquetsExistentes debe estar declarado en el notebook."""
    codigo = _leer_transaccional()
    assert "rutaRelativaParquetsExistentes" in codigo, (
        "Falta el widget 'rutaRelativaParquetsExistentes' para continuidad de TRXID"
    )


def test_widget_ruta_parquets_existentes_valor_por_defecto_vacio():
    """El widget debe tener valor por defecto vacío para indicar primer archivo."""
    codigo = _leer_transaccional()
    # El widget debe estar definido con string vacío como valor por defecto
    assert 'dbutils.widgets.text("rutaRelativaParquetsExistentes", ""' in codigo, (
        "El widget 'rutaRelativaParquetsExistentes' debe tener valor por defecto vacío"
    )


def test_captura_ruta_parquets_existentes():
    """El valor del widget debe ser capturado en una variable."""
    codigo = _leer_transaccional()
    assert 'dbutils.widgets.get("rutaRelativaParquetsExistentes")' in codigo, (
        "Falta capturar el valor del widget 'rutaRelativaParquetsExistentes'"
    )


# ===== Continuidad de TRXID — id_inicio =====

def test_usa_id_inicio_variable():
    """spark.range debe usar id_inicio como parámetro de inicio, no el literal 1."""
    codigo = _leer_transaccional()
    assert "spark.range(id_inicio," in codigo or "spark.range(id_inicio ," in codigo, (
        "spark.range debe iniciar en 'id_inicio' (no en el literal 1) para garantizar "
        "unicidad de TRXID entre ejecuciones"
    )


def test_no_usa_spark_range_literal_1():
    """spark.range no debe usar el literal 1 como inicio de secuencia."""
    codigo = _leer_transaccional()
    assert "spark.range(1," not in codigo and "spark.range(1 ," not in codigo, (
        "spark.range no debe iniciar en el literal 1; debe usar 'id_inicio' para "
        "garantizar TRXID irrepetibles entre ejecuciones"
    )


def test_variable_id_inicio_definida():
    """La variable id_inicio debe estar definida en el notebook."""
    codigo = _leer_transaccional()
    assert "id_inicio" in codigo, (
        "Falta la variable 'id_inicio' que determina el inicio de la secuencia TRXID"
    )


# ===== Lectura de TRXSQ máximo =====

def test_usa_trxsq_para_calcular_max():
    """El notebook debe leer el máximo de TRXSQ de los parquets existentes."""
    codigo = _leer_transaccional()
    assert "TRXSQ" in codigo, (
        "Falta referencia a 'TRXSQ' para calcular el máximo de la secuencia existente"
    )
    assert "F.max" in codigo, (
        "Falta F.max() para obtener el valor máximo de TRXSQ de los parquets existentes"
    )


def test_id_inicio_es_max_trxsq_mas_uno():
    """id_inicio debe ser max_trxsq + 1 para continuidad sin solapamiento."""
    codigo = _leer_transaccional()
    assert "max_trxsq" in codigo, (
        "Falta la variable 'max_trxsq' que almacena el TRXSQ máximo de parquets existentes"
    )
    assert "int(max_trxsq) + 1" in codigo or "max_trxsq + 1" in codigo, (
        "id_inicio debe calcularse como max_trxsq + 1 para garantizar continuidad"
    )


def test_maneja_ruta_vacia_con_id_inicio_1():
    """Cuando la ruta está vacía, id_inicio debe ser 1 (primer archivo)."""
    codigo = _leer_transaccional()
    assert "id_inicio = 1" in codigo, (
        "Cuando no hay parquets existentes, id_inicio debe ser 1"
    )


# ===== Construcción de ruta con TipoStorage =====

def test_ruta_existentes_usa_tipo_storage():
    """La ruta completa de parquets existentes debe construirse con la lógica de TipoStorage."""
    codigo = _leer_transaccional()
    assert "ruta_completa_existentes" in codigo, (
        "Falta la variable 'ruta_completa_existentes' para la ruta completa de parquets existentes"
    )


def test_ruta_existentes_volume_y_s3():
    """La ruta completa debe contemplar tanto Volume como AmazonS3."""
    codigo = _leer_transaccional()
    # La variable ruta_completa_existentes debe aparecer más de una vez
    # (una por cada rama de TipoStorage)
    count = codigo.count("ruta_completa_existentes")
    assert count >= 3, (
        f"'ruta_completa_existentes' debe aparecer al menos 3 veces (Volume, S3, uso). "
        f"Apariciones encontradas: {count}"
    )


# ===== Manejo de error al leer parquets existentes =====

def test_manejo_error_lectura_parquets_existentes():
    """Debe capturarse la excepción si los parquets existentes no pueden leerse."""
    codigo = _leer_transaccional()
    assert "ruta_completa_existentes" in codigo
    # Debe haber un bloque try/except que contemple fallo de lectura
    assert "except Exception" in codigo or "except" in codigo, (
        "Falta manejo de excepción al leer parquets existentes"
    )


# ===== Compatibilidad — sin UDFs =====

def test_no_usa_udf():
    codigo = _leer_transaccional()
    assert "udf(" not in codigo.lower(), "NO debe usar UDFs"


def test_no_usa_cache_persist():
    codigo = _leer_transaccional()
    assert ".cache()" not in codigo, "NO debe usar .cache()"
    assert ".persist()" not in codigo, "NO debe usar .persist()"
