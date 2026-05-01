"""Tests estáticos para los notebooks de Plata — Hubs, Links y Satellites."""
import ast
from pathlib import Path

TRANSFORMATIONS = (
    Path(__file__).resolve().parent.parent
    / "src"
    / "LSDP_Lab_DataVault_DWH"
    / "transformations"
)

HUBS = {
    "HubCliente": TRANSFORMATIONS / "LSDPPlataHubCliente.py",
    "HubOperacion": TRANSFORMATIONS / "LSDPPlataHubOperacion.py",
    "HubTransaccion": TRANSFORMATIONS / "LSDPPlataHubTransaccion.py",
}

LINKS = {
    "LinkClienteOperacion": TRANSFORMATIONS / "LSDPPlataLinkClienteOperacion.py",
    "LinkClienteTransaccion": TRANSFORMATIONS / "LSDPPlataLinkClienteTransaccion.py",
}

SATELLITES = {
    "SatCliente": TRANSFORMATIONS / "LSDPPlataSatCliente.py",
    "SatOperacion": TRANSFORMATIONS / "LSDPPlataSatOperacion.py",
    "SatTransaccion": TRANSFORMATIONS / "LSDPPlataSatTransaccion.py",
}

TODOS = {**HUBS, **LINKS, **SATELLITES}


def _codigo(nombre):
    return TODOS[nombre].read_text(encoding="utf-8")


def _tree(nombre):
    return ast.parse(_codigo(nombre))


# ─── Existencia de archivos ───────────────────────────────────────────────────


def test_hubs_existen():
    for nombre, ruta in HUBS.items():
        assert ruta.exists(), f"{nombre}: {ruta} no existe"


def test_links_existen():
    for nombre, ruta in LINKS.items():
        assert ruta.exists(), f"{nombre}: {ruta} no existe"


def test_satellites_existen():
    for nombre, ruta in SATELLITES.items():
        assert ruta.exists(), f"{nombre}: {ruta} no existe"


# ─── Imports obligatorios ─────────────────────────────────────────────────────


def test_hubs_import_lsdp_correcto():
    for nombre in HUBS:
        codigo = _codigo(nombre)
        assert "from pyspark import pipelines as dp" in codigo, f"{nombre}: import LSDP incorrecto"
        assert "import databricks.sdk" not in codigo, f"{nombre}: import SDK prohibido"


def test_links_import_lsdp_correcto():
    for nombre in LINKS:
        codigo = _codigo(nombre)
        assert "from pyspark import pipelines as dp" in codigo, f"{nombre}: import LSDP incorrecto"
        assert "import databricks.sdk" not in codigo


def test_satellites_import_lsdp_correcto():
    for nombre in SATELLITES:
        codigo = _codigo(nombre)
        assert "from pyspark import pipelines as dp" in codigo, f"{nombre}: import LSDP incorrecto"
        assert "import databricks.sdk" not in codigo


def test_todos_importan_obtener_configuracion():
    for nombre in TODOS:
        codigo = _codigo(nombre)
        assert "obtener_configuracion" in codigo, f"{nombre}: falta obtener_configuracion"
        assert "obtener_configuracion(spark)" in codigo, f"{nombre}: debe invocar con spark"


def test_todos_importan_utilidades():
    for nombre in TODOS:
        codigo = _codigo(nombre)
        assert "LSDPUtilidadPrincipal" in codigo, f"{nombre}: falta import de utilidades"


# ─── Hubs — decorador ST+append_flow y patrón ──────────────────────────────────────


def test_hubs_usan_create_streaming_table():
    for nombre in HUBS:
        codigo = _codigo(nombre)
        assert "dp.create_streaming_table(" in codigo, f"{nombre}: falta dp.create_streaming_table"


def test_hubs_usan_append_flow():
    # HubCliente y HubOperacion migrados a create_auto_cdc_flow; solo HubTransaccion conserva append_flow
    for nombre in ["HubTransaccion"]:
        codigo = _codigo(nombre)
        assert "@dp.append_flow(" in codigo, f"{nombre}: falta @dp.append_flow"


def test_hubs_nombre_3_partes():
    for nombre in HUBS:
        codigo = _codigo(nombre)
        assert "catalogo_plata" in codigo, f"{nombre}: debe usar catalogo_plata"
        assert "esquema_plata" in codigo, f"{nombre}: debe usar esquema_plata"


def test_hubs_usan_cluster_by():
    for nombre in HUBS:
        codigo = _codigo(nombre)
        assert "cluster_by" in codigo, f"{nombre}: falta cluster_by"
        assert "FechaRegistro" in codigo, f"{nombre}: cluster_by debe incluir FechaRegistro"


def test_hubs_usan_dp_read_stream():
    for nombre in HUBS:
        codigo = _codigo(nombre)
        assert "dp.read_stream(" in codigo, f"{nombre}: falta dp.read_stream"


def test_hubs_cliente_operacion_usan_auto_cdc_flow():
    # HubCliente y HubOperacion migrados a create_auto_cdc_flow SCD Type 1:
    # deduplicación cross-batch gestionada por el motor via MERGE, sin full scan.
    # FechaRegistro se actualiza en cada update (semántica "última vez vista", no DV2.0 estricto).
    for nombre in ["HubCliente", "HubOperacion"]:
        codigo = _codigo(nombre)
        assert "dp.create_auto_cdc_flow(" in codigo, f"{nombre}: falta dp.create_auto_cdc_flow"
        assert "stored_as_scd_type=1" in codigo, f"{nombre}: debe usar stored_as_scd_type=1"
        assert "except_column_list" not in codigo, f"{nombre}: no debe usar except_column_list (excluye columna del esquema target)"
        assert "@dp.view" in codigo, f"{nombre}: falta @dp.view para la fuente CDC"
        assert "procesar_hub(" not in codigo, f"{nombre}: no debe usar procesar_hub (migrado a AUTO CDC)"


def test_hubs_usan_fuente_datos():
    for nombre in HUBS:
        codigo = _codigo(nombre)
        assert "FuenteDatos" in codigo, f"{nombre}: falta FuenteDatos"
        assert "F.lit(" in codigo, f"{nombre}: FuenteDatos debe usar F.lit()"


def test_hubs_usan_reordenar_columnas():
    for nombre in HUBS:
        codigo = _codigo(nombre)
        assert "reordenar_columnas_lc" in codigo, f"{nombre}: falta reordenar_columnas_lc"


def test_hubs_tienen_expectations():
    for nombre in HUBS:
        codigo = _codigo(nombre)
        assert "expect" in codigo.lower(), f"{nombre}: falta expectations"


def test_hub_cliente_columnas_clave():
    codigo = _codigo("HubCliente")
    assert "Hash_Cliente" in codigo
    assert "IdentificadorCliente" in codigo
    assert "CUSTID" in codigo
    assert "CMSTFL" in codigo


def test_hub_operacion_columnas_clave():
    codigo = _codigo("HubOperacion")
    assert "Hash_Operacion" in codigo
    assert "IdentificadorCliente" in codigo
    assert "SecuenciaSaldo" in codigo
    assert "CUSTID" in codigo
    assert "BLSQ" in codigo
    assert "BLNCFL" in codigo


def test_hub_transaccion_columnas_clave():
    codigo = _codigo("HubTransaccion")
    assert "Hash_Transaccion" in codigo
    assert "IdentificadorTransaccion" in codigo
    assert "TRXID" in codigo
    assert "TRXPFL" in codigo


def test_hubs_no_propagan_columnas_bronce():
    # Verifica que no se accede a columnas exclusivas de Bronce como F.col("colname") o "colname"
    import re
    columnas_excluir = ["año", "mes", "dia", "FechaRegistroParquet", "_rescued_data"]
    for nombre in HUBS:
        codigo = _codigo(nombre)
        for col in columnas_excluir:
            # Busca accesos directos a la columna: F.col("col"), alias("col"), "col", 'col'
            patron = rf'["\'](?:F\.col\()?{re.escape(col)}["\'\)]'
            # Más simple: busca F.col("col") o .col("col") o "col" como alias
            assert f'F.col("{col}")' not in codigo, f"{nombre}: no debe acceder a columna de Bronce '{col}'"
            assert f"F.col('{col}')" not in codigo, f"{nombre}: no debe acceder a columna de Bronce '{col}'"


# ─── Links — decorador ST+append_flow y patrón ──────────────────────────────────────


def test_links_usan_create_streaming_table():
    for nombre in LINKS:
        codigo = _codigo(nombre)
        assert "dp.create_streaming_table(" in codigo, f"{nombre}: falta dp.create_streaming_table"


def test_links_usan_append_flow():
    # LinkClienteOperacion migrado a create_auto_cdc_flow; solo LinkClienteTransaccion conserva append_flow
    for nombre in ["LinkClienteTransaccion"]:
        codigo = _codigo(nombre)
        assert "@dp.append_flow(" in codigo, f"{nombre}: falta @dp.append_flow"


def test_links_nombre_3_partes():
    for nombre in LINKS:
        codigo = _codigo(nombre)
        assert "catalogo_plata" in codigo
        assert "esquema_plata" in codigo


def test_links_usan_cluster_by():
    for nombre in LINKS:
        codigo = _codigo(nombre)
        assert "cluster_by" in codigo


def test_links_usan_dp_read_stream():
    for nombre in LINKS:
        codigo = _codigo(nombre)
        assert "dp.read_stream(" in codigo, f"{nombre}: falta dp.read_stream"


def test_link_cliente_operacion_usa_auto_cdc_flow():
    # LinkClienteOperacion migrado a create_auto_cdc_flow SCD Type 1:
    # la combinación Hash_Cliente + Hash_Operacion queda única garantizada por el motor.
    # FechaRegistro se actualiza en cada update (semántica "última vez vista", no DV2.0 estricto).
    nombre = "LinkClienteOperacion"
    codigo = _codigo(nombre)
    assert "dp.create_auto_cdc_flow(" in codigo, f"{nombre}: falta dp.create_auto_cdc_flow"
    assert "stored_as_scd_type=1" in codigo, f"{nombre}: debe usar stored_as_scd_type=1"
    assert "except_column_list" not in codigo, f"{nombre}: no debe usar except_column_list (excluye columna del esquema target)"
    assert "@dp.view" in codigo, f"{nombre}: falta @dp.view para la fuente CDC"
    assert "procesar_link(" not in codigo, f"{nombre}: no debe usar procesar_link (migrado a AUTO CDC)"


def test_link_cliente_operacion_columnas():
    codigo = _codigo("LinkClienteOperacion")
    assert "Hash_Link_Cliente_Operacion" in codigo
    assert "Hash_Cliente" in codigo
    assert "Hash_Operacion" in codigo
    assert "BLNCFL" in codigo


def test_link_cliente_transaccion_columnas():
    codigo = _codigo("LinkClienteTransaccion")
    assert "Hash_Link_Cliente_Transaccion" in codigo
    assert "Hash_Cliente" in codigo
    assert "Hash_Transaccion" in codigo
    assert "TRXPFL" in codigo


def test_links_no_propagan_columnas_bronce():
    columnas_excluir = ["año", "mes", "dia", "FechaRegistroParquet", "_rescued_data"]
    for nombre in LINKS:
        codigo = _codigo(nombre)
        for col in columnas_excluir:
            assert f'F.col("{col}")' not in codigo, f"{nombre}: no debe acceder a columna de Bronce '{col}'"
            assert f"F.col('{col}')" not in codigo, f"{nombre}: no debe acceder a columna de Bronce '{col}'"


# ─── Satellites — patrón ST Acumulativa + append_flow ────────────────────────


def test_satellites_usan_create_streaming_table():
    for nombre in SATELLITES:
        codigo = _codigo(nombre)
        assert "dp.create_streaming_table(" in codigo, f"{nombre}: falta dp.create_streaming_table"


def test_satellites_usan_append_flow():
    for nombre in SATELLITES:
        codigo = _codigo(nombre)
        assert "@dp.append_flow(" in codigo, f"{nombre}: falta @dp.append_flow"


def test_satellites_definen_cluster_by():
    for nombre in SATELLITES:
        codigo = _codigo(nombre)
        assert "cluster_by" in codigo


def test_satellites_tienen_expectations_hash_diferenciador():
    for nombre in SATELLITES:
        codigo = _codigo(nombre)
        assert "hash_diferenciador_no_nulo" in codigo, f"{nombre}: falta expectation hash_diferenciador_no_nulo"


def test_satellites_usan_procesar_satellite():
    """SatCliente y SatOperacion usan procesar_satellite.

    OPT-001: SatTransaccion ya no usa procesar_satellite_transaccional; lee de
    la vista CDF compartida vista_trxpfl_cdf y aplica append puro porque TRXID
    es globalmente único entre ejecuciones.
    """
    for nombre in ["SatCliente", "SatOperacion"]:
        codigo = _codigo(nombre)
        assert "procesar_satellite(" in codigo, f"{nombre}: falta procesar_satellite"


def test_satellites_usan_hash_diferenciador():
    for nombre in SATELLITES:
        codigo = _codigo(nombre)
        assert "Hash_Diferenciador" in codigo
        assert "calcular_hash_diferenciador" in codigo


def test_satellites_usan_dp_read_stream():
    """Todos los Satellites deben leer Bronce con dp.read_stream()."""
    for nombre in SATELLITES:
        codigo = _codigo(nombre)
        assert "dp.read_stream(" in codigo, f"{nombre}: falta dp.read_stream"


def test_satellites_no_propagan_columnas_bronce():
    columnas_excluir = ["año", "mes", "dia", "FechaRegistroParquet", "_rescued_data"]
    for nombre in SATELLITES:
        codigo = _codigo(nombre)
        for col in columnas_excluir:
            assert f'F.col("{col}")' not in codigo, f"{nombre}: no debe acceder a columna de Bronce '{col}'"
            assert f"F.col('{col}')" not in codigo, f"{nombre}: no debe acceder a columna de Bronce '{col}'"


def test_sat_cliente_usa_clasificar_por_umbral():
    codigo = _codigo("SatCliente")
    assert "clasificar_por_umbral" in codigo
    assert "UMBRAL_RANGO_ETARIO" in codigo
    assert "UMBRAL_CATEGORIA_INGRESOS" in codigo


def test_sat_cliente_define_4_streaming_tables():
    codigo = _codigo("SatCliente")
    assert "Sat_Cliente_DatosEstables" in codigo
    assert "Sat_Cliente_Contacto" in codigo
    assert "Sat_Cliente_Clasificacion" in codigo
    assert "Sat_Cliente_Financiero" in codigo


def test_sat_operacion_usa_clasificar_por_umbral():
    codigo = _codigo("SatOperacion")
    assert "clasificar_por_umbral" in codigo
    assert "UMBRAL_CATEGORIA_SALDO" in codigo
    assert "UMBRAL_UTILIZACION_CREDITO" in codigo
    assert "UMBRAL_SOBREGIRO" in codigo


def test_sat_operacion_define_3_streaming_tables():
    codigo = _codigo("SatOperacion")
    assert "Sat_Operacion_DatosEstables" in codigo
    assert "Sat_Operacion_Montos" in codigo
    assert "Sat_Operacion_FechasEvento" in codigo


def test_sat_transaccion_define_2_streaming_tables():
    codigo = _codigo("SatTransaccion")
    assert "Sat_Transaccion_DatosEstables" in codigo
    assert "Sat_Transaccion_Montos" in codigo


def test_sat_transaccion_datos_estables_tiene_clasificacion_canal_atm():
    codigo = _codigo("SatTransaccion")
    assert "ClasificacionCanalATM" in codigo
    assert "RETIRO_ATM" in codigo
    assert "DEPOSITO_ATM" in codigo
    assert "NO_ATM" in codigo


def test_sat_transaccion_montos_usa_clasificar_por_umbral():
    codigo = _codigo("SatTransaccion")
    assert "clasificar_por_umbral" in codigo
    assert "UMBRAL_RANGO_MONTO" in codigo
    assert "UMBRAL_RIESGO_FRAUDE" in codigo


# ─── Restricciones Serverless — todos los notebooks ──────────────────────────


def test_todos_sin_cache_ni_persist():
    for nombre in TODOS:
        codigo = _codigo(nombre)
        assert ".cache()" not in codigo, f"{nombre}: NO .cache()"
        assert ".persist()" not in codigo, f"{nombre}: NO .persist()"


def test_todos_sin_spark_context():
    for nombre in TODOS:
        codigo = _codigo(nombre)
        assert "sparkContext" not in codigo, f"{nombre}: NO sparkContext"
        assert ".parallelize(" not in codigo, f"{nombre}: NO .parallelize"


def test_todos_sin_udfs():
    for nombre in TODOS:
        codigo = _codigo(nombre)
        assert "@udf" not in codigo.lower(), f"{nombre}: NO @udf"
        assert "udf(" not in codigo.lower(), f"{nombre}: NO udf("


def test_todos_sin_sdk_prohibido():
    for nombre in TODOS:
        codigo = _codigo(nombre)
        assert "import databricks.sdk.pipelines" not in codigo, f"{nombre}: SDK prohibido"


def test_todos_usan_sha2_no_hash_simple():
    for nombre in TODOS:
        codigo = _codigo(nombre)
        assert "sha2" in codigo or "calcular_hash_hub" in codigo or "calcular_hash_diferenciador" in codigo, \
            f"{nombre}: debe usar SHA2"


# ─── Change Data Feed obligatorio en todas las Streaming Tables de Plata ─────


def test_todos_habilitan_change_data_feed():
    """Hubs, Links y Satellites de Plata DEBEN declarar table_properties con CDF.

    Requisito para refresh incremental de las MV de Oro: Enzyme requiere CDF
    en todas las fuentes upstream de una MV para evitar COMPLETE_RECOMPUTE.
    """
    for nombre in TODOS:
        codigo = _codigo(nombre)
        assert "table_properties" in codigo, \
            f"{nombre}: debe declarar table_properties en create_streaming_table"
        assert '"delta.enableChangeDataFeed": "true"' in codigo, \
            f"{nombre}: debe habilitar delta.enableChangeDataFeed=true"


# ─── OPT-001 — Linaje transaccional sobre Change Data Feed ──────────────────


VISTA_TRXPFL_CDF_PATH = TRANSFORMATIONS / "LSDPPlataVistaTRXPFLCDF.py"

CONSUMIDORES_VISTA_CDF = {
    "HubTransaccion": HUBS["HubTransaccion"],
    "LinkClienteTransaccion": LINKS["LinkClienteTransaccion"],
    "SatTransaccion": SATELLITES["SatTransaccion"],
}


def test_vista_trxpfl_cdf_existe():
    assert VISTA_TRXPFL_CDF_PATH.exists(), \
        f"OPT-001: falta {VISTA_TRXPFL_CDF_PATH.name} (vista CDF compartida)"


def test_vista_trxpfl_cdf_usa_dp_view_y_change_feed():
    codigo = VISTA_TRXPFL_CDF_PATH.read_text(encoding="utf-8")
    assert "@dp.view(" in codigo, "vista_trxpfl_cdf debe declararse con @dp.view"
    assert "vista_trxpfl_cdf" in codigo, "vista_trxpfl_cdf debe nombrarse explícitamente"
    assert '"readChangeFeed"' in codigo and '"true"' in codigo, \
        "vista_trxpfl_cdf debe leer con readChangeFeed=true"
    assert "_change_type" in codigo, \
        "vista_trxpfl_cdf debe filtrar por _change_type"
    assert "VersionCarga" in codigo and "FechaCargaBronce" in codigo, \
        "vista_trxpfl_cdf debe promover _commit_version y _commit_timestamp"


def test_consumidores_transaccionales_leen_vista_cdf():
    for nombre in CONSUMIDORES_VISTA_CDF:
        codigo = _codigo(nombre)
        assert 'dp.read_stream("vista_trxpfl_cdf")' in codigo, \
            f"{nombre}: debe leer dp.read_stream(\"vista_trxpfl_cdf\")"
        assert "dp.read_stream(fuente)" not in codigo, \
            f"{nombre}: ya no debe consumir TRXPFL directamente (usar vista CDF)"
        assert "dp.read_stream(_fuente)" not in codigo, \
            f"{nombre}: ya no debe consumir TRXPFL directamente (usar vista CDF)"


def test_sat_transaccion_propaga_columnas_cdf():
    codigo = _codigo("SatTransaccion")
    assert "VersionCarga" in codigo, "SatTransaccion debe propagar VersionCarga"
    assert "FechaCargaBronce" in codigo, "SatTransaccion debe propagar FechaCargaBronce"


def test_consumidores_transaccionales_no_usan_helpers_dedup():
    """OPT-001: el linaje transaccional ya no usa procesar_hub/_link/_satellite_transaccional."""
    helpers_prohibidos = (
        "procesar_hub(",
        "procesar_link(",
        "procesar_satellite_transaccional(",
    )
    for nombre in CONSUMIDORES_VISTA_CDF:
        codigo = _codigo(nombre)
        for helper in helpers_prohibidos:
            assert helper not in codigo, \
                f"{nombre}: ya no debe invocar {helper.rstrip('(')} (append puro vía CDF)"
