"""Tests para LSDPConfiguracion.py — Módulo de configuración centralizada."""
import ast
import inspect
import sys
from unittest.mock import MagicMock


def _importar_configuracion():
    """Importa el módulo de configuración sin necesidad de PySpark real."""
    import importlib
    sys.path.insert(0, str(_ruta_utilities()))
    if "LSDPConfiguracion" in sys.modules:
        del sys.modules["LSDPConfiguracion"]
    return importlib.import_module("LSDPConfiguracion")


def _ruta_utilities():
    from pathlib import Path
    return Path(__file__).resolve().parent.parent / "src" / "LSDP_Lab_DataVault_DWH" / "utilities"


# ===== Tests de Constantes de Módulo =====

def test_constantes_atm():
    mod = _importar_configuracion()
    assert mod.TIPO_DATM == "DATM"
    assert mod.TIPO_CATM == "CATM"
    assert isinstance(mod.TIPOS_ATM, list)
    assert "DATM" in mod.TIPOS_ATM
    assert "CATM" in mod.TIPOS_ATM


def test_constantes_hash():
    mod = _importar_configuracion()
    assert mod.HASH_HUB_LINK_BITS == 256
    assert mod.HASH_SATELLITE_BITS == 512
    assert mod.HASH_SEPARATOR == "|"


def test_umbrales_existen_y_son_diccionarios():
    mod = _importar_configuracion()
    umbrales = [
        "UMBRAL_RANGO_ETARIO",
        "UMBRAL_CATEGORIA_INGRESOS",
        "UMBRAL_CATEGORIA_SALDO",
        "UMBRAL_UTILIZACION_CREDITO",
        "UMBRAL_SOBREGIRO",
        "UMBRAL_RANGO_MONTO",
        "UMBRAL_RIESGO_FRAUDE",
    ]
    for nombre in umbrales:
        val = getattr(mod, nombre)
        assert isinstance(val, dict), f"{nombre} debe ser dict"
        for etiqueta, rango in val.items():
            assert isinstance(etiqueta, str), f"{nombre}[{etiqueta}] etiqueta str"
            assert isinstance(rango, tuple) and len(rango) == 2, f"{nombre}[{etiqueta}] tupla (min, max)"


def test_umbral_rango_etario_valores():
    mod = _importar_configuracion()
    assert mod.UMBRAL_RANGO_ETARIO["JOVEN_ADULTO"] == (18, 25)
    assert mod.UMBRAL_RANGO_ETARIO["SENIOR"] == (56, 999)


def test_umbral_categoria_ingresos_valores():
    mod = _importar_configuracion()
    assert mod.UMBRAL_CATEGORIA_INGRESOS["BAJO"] == (0, 15000)
    assert mod.UMBRAL_CATEGORIA_INGRESOS["PREMIUM"] == (85001, 999999999)


def test_umbral_utilizacion_credito_valores():
    mod = _importar_configuracion()
    assert mod.UMBRAL_UTILIZACION_CREDITO["SIN_USO"] == (0, 0)
    assert mod.UMBRAL_UTILIZACION_CREDITO["SOBRE_UTILIZADO"] == (0.151, 1.0)


# ===== Tests de obtener_configuracion =====

def test_obtener_configuracion_retorna_dict_con_13_claves():
    mod = _importar_configuracion()
    mock_spark = MagicMock()
    mock_spark.conf.get.side_effect = lambda key: f"valor_{key.split('.')[-1]}"
    config = mod.obtener_configuracion(mock_spark)
    assert isinstance(config, dict)
    claves_esperadas = [
        "catalogo", "esquema", "volumen",
        "catalogo_plata", "esquema_plata",
        "catalogo_oro", "esquema_oro",
        "ruta_cmstfl", "ruta_trxpfl", "ruta_blncfl",
        "schema_location_cmstfl", "schema_location_trxpfl", "schema_location_blncfl",
    ]
    assert set(config.keys()) == set(claves_esperadas)


def test_obtener_configuracion_lee_spark_conf_get():
    mod = _importar_configuracion()
    mock_spark = MagicMock()
    valores = {
        "pipeline.catalogo": "mi_catalogo",
        "pipeline.esquema": "mi_esquema",
        "pipeline.volumen": "mi_volumen",
        "pipeline.catalogo_plata": "cat_plata",
        "pipeline.esquema_plata": "esq_plata",
        "pipeline.catalogo_oro": "cat_oro",
        "pipeline.esquema_oro": "esq_oro",
        "pipeline.ruta_cmstfl": "ruta/cmstfl",
        "pipeline.ruta_trxpfl": "ruta/trxpfl",
        "pipeline.ruta_blncfl": "ruta/blncfl",
        "pipeline.schema_location_cmstfl": "schema/cmstfl",
        "pipeline.schema_location_trxpfl": "schema/trxpfl",
        "pipeline.schema_location_blncfl": "schema/blncfl",
    }
    mock_spark.conf.get.side_effect = lambda k: valores[k]
    config = mod.obtener_configuracion(mock_spark)
    base = "/Volumes/mi_catalogo/mi_esquema/mi_volumen"
    assert config["catalogo"] == "mi_catalogo"
    # Las rutas se construyen como {base_volumen}/{ruta_relativa}
    assert config["ruta_cmstfl"] == f"{base}/ruta/cmstfl"
    assert config["schema_location_blncfl"] == f"{base}/schema/blncfl"


def test_obtener_configuracion_propaga_error_sin_default():
    mod = _importar_configuracion()
    mock_spark = MagicMock()
    mock_spark.conf.get.side_effect = Exception("parametro no configurado")
    try:
        mod.obtener_configuracion(mock_spark)
        assert False, "Debería propagar la excepción"
    except Exception as e:
        assert "parametro no configurado" in str(e)


def test_obtener_configuracion_recibe_spark_como_parametro():
    mod = _importar_configuracion()
    sig = inspect.signature(mod.obtener_configuracion)
    params = list(sig.parameters.keys())
    assert "spark" in params


# ===== Tests de Restricciones =====

def test_no_importa_lsdp():
    """El módulo NO debe contener 'from pyspark import pipelines' ni 'import databricks'."""
    ruta = _ruta_utilities() / "LSDPConfiguracion.py"
    codigo = ruta.read_text(encoding="utf-8")
    assert "from pyspark import pipelines" not in codigo
    assert "import databricks" not in codigo


def test_no_valores_default_en_spark_conf_get():
    """obtener_configuracion no debe pasar valores por defecto a spark.conf.get()."""
    ruta = _ruta_utilities() / "LSDPConfiguracion.py"
    tree = ast.parse(ruta.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr == "get":
                assert len(node.args) <= 1, \
                    "spark.conf.get() no debe tener segundo argumento (valor default)"
