"""Tests estáticos (AST) para los notebooks de la Medalla de Oro — Dimensiones y Hecho ATM."""
import ast
from pathlib import Path

TRANSFORMATIONS = (
    Path(__file__).resolve().parent.parent
    / "src"
    / "LSDP_Lab_DataVault_DWH"
    / "transformations"
)

ORO = {
    "DimTiempo": TRANSFORMATIONS / "LSDPOroDimTiempo.py",
    "DimCliente": TRANSFORMATIONS / "LSDPOroDimCliente.py",
    "DimOperacion": TRANSFORMATIONS / "LSDPOroDimOperacion.py",
    "MapClienteOperacionDominante": TRANSFORMATIONS / "LSDPOroMapClienteOperacionDominante.py",
    "TrxATMEnriquecida": TRANSFORMATIONS / "LSDPOroTrxATMEnriquecida.py",
    "HecTransaccionesATM": TRANSFORMATIONS / "LSDPOroHecTransaccionesATM.py",
}


def _codigo(nombre: str) -> str:
    return ORO[nombre].read_text(encoding="utf-8")


def _tree(nombre: str) -> ast.Module:
    return ast.parse(_codigo(nombre))


# ─── Existencia de archivos ───────────────────────────────────────────────────


def test_notebooks_oro_existen():
    for nombre, ruta in ORO.items():
        assert ruta.exists(), f"{nombre}: {ruta} no existe"


# ─── Imports obligatorios ─────────────────────────────────────────────────────


def test_todos_importan_lsdp_correcto():
    for nombre in ORO:
        codigo = _codigo(nombre)
        assert "from pyspark import pipelines as dp" in codigo, \
            f"{nombre}: import LSDP incorrecto — debe ser 'from pyspark import pipelines as dp'"
        assert "import databricks.sdk" not in codigo, \
            f"{nombre}: import SDK prohibido"


def test_todos_importan_obtener_configuracion():
    for nombre in ORO:
        codigo = _codigo(nombre)
        assert "obtener_configuracion" in codigo, \
            f"{nombre}: falta uso de obtener_configuracion"
        assert "obtener_configuracion(spark)" in codigo, \
            f"{nombre}: debe invocar obtener_configuracion(spark)"


def test_todos_usan_catalogo_oro_y_esquema_oro():
    for nombre in ORO:
        if nombre == "TrxATMEnriquecida":
            # MV temporary que solo lee de Plata; no construye nombre Oro de 3 partes.
            continue
        codigo = _codigo(nombre)
        assert "catalogo_oro" in codigo, f"{nombre}: falta referencia a catalogo_oro"
        assert "esquema_oro" in codigo, f"{nombre}: falta referencia a esquema_oro"


def test_todos_no_usan_catalogo_plata_separado():
    """Los nombres de tabla de Plata deben construirse con catalogo_plata/esquema_plata."""
    for nombre in ORO:
        codigo = _codigo(nombre)
        # No deben acceder a parámetros por fuera de la función obtener_configuracion
        # Verificar que NO hay hardcoded catalog/schema strings
        assert "catalog=" not in codigo and "schema=" not in codigo, \
            f"{nombre}: no debe usar catalog=/schema= como kwargs separados en el decorador"


# ─── Dim_Tiempo — Vista Materializada incremental ─────────────────────────────


def test_dim_tiempo_usa_materialized_view():
    codigo = _codigo("DimTiempo")
    assert "@dp.materialized_view" in codigo, \
        "Dim_Tiempo debe usar @dp.materialized_view, no Streaming Table"
    assert "dp.create_streaming_table" not in codigo, \
        "Dim_Tiempo NO debe usar dp.create_streaming_table"
    assert "@dp.append_flow" not in codigo, \
        "Dim_Tiempo NO debe usar @dp.append_flow"


def test_dim_tiempo_usa_nombre_tres_partes():
    codigo = _codigo("DimTiempo")
    assert "Dim_Tiempo" in codigo, "Dim_Tiempo debe tener nombre Dim_Tiempo"
    # El nombre de 3 partes debe incluir catalogo_oro y esquema_oro
    assert "catalogo_oro" in codigo and "esquema_oro" in codigo, \
        "Dim_Tiempo debe usar nombre de 3 partes con catalogo_oro.esquema_oro.Dim_Tiempo"


def test_dim_tiempo_no_usa_funciones_no_deterministas():
    codigo = _codigo("DimTiempo")
    assert "current_date" not in codigo, \
        "Dim_Tiempo no debe usar current_date() — función no determinística"
    assert "current_timestamp" not in codigo, \
        "Dim_Tiempo no debe usar current_timestamp() — función no determinística"
    assert "F.now()" not in codigo, \
        "Dim_Tiempo no debe usar F.now()"
    assert "spark.range" not in codigo, \
        "Dim_Tiempo no debe usar spark.range — genera rango sintético"


def test_dim_tiempo_lee_sat_transaccion_montos():
    codigo = _codigo("DimTiempo")
    assert "Sat_Transaccion_Montos" in codigo, \
        "Dim_Tiempo debe leer Sat_Transaccion_Montos"
    assert "fecha_transaccion" in codigo, \
        "Dim_Tiempo debe acceder a fecha_transaccion"


def test_dim_tiempo_usa_distinct():
    codigo = _codigo("DimTiempo")
    assert ".distinct()" in codigo, \
        "Dim_Tiempo debe usar .distinct() para obtener fechas únicas"


def test_dim_tiempo_renombra_fecha_transaccion_a_fecha_clave():
    codigo = _codigo("DimTiempo")
    assert "FechaClave" in codigo, \
        "Dim_Tiempo debe renombrar la columna a FechaClave"


def test_dim_tiempo_configura_liquid_clustering():
    codigo = _codigo("DimTiempo")
    assert 'cluster_by=["FechaClave"]' in codigo or "cluster_by=['FechaClave']" in codigo, \
        "Dim_Tiempo debe configurar cluster_by=[\"FechaClave\"]"


def test_dim_tiempo_deriva_atributos_calendario():
    codigo = _codigo("DimTiempo")
    atributos = ["Anio", "Mes", "Dia", "Trimestre", "Semestre", "DiaSemana",
                 "NombreDia", "NombreMes", "EsFinSemana", "DiaDelAnio", "SemanaDelAnio"]
    for attr in atributos:
        assert attr in codigo, f"Dim_Tiempo debe derivar el atributo {attr}"


def test_dim_tiempo_declara_expectations():
    codigo = _codigo("DimTiempo")
    assert "FechaClave IS NOT NULL" in codigo, \
        "Dim_Tiempo debe declarar expectation: FechaClave IS NOT NULL"
    assert "expect_all_or_fail" in codigo or "expect_or_fail" in codigo, \
        "Dim_Tiempo debe tener expectation de fallo para FechaClave"


def test_dim_tiempo_no_usa_joins():
    tree = _tree("DimTiempo")
    codigo = _codigo("DimTiempo")
    # .join( no debe aparecer en el notebook de Dim_Tiempo
    assert ".join(" not in codigo, \
        "Dim_Tiempo no debe usar joins — incompatible con incremental refresh"


def test_dim_tiempo_no_usa_window_functions():
    codigo = _codigo("DimTiempo")
    assert "Window" not in codigo, \
        "Dim_Tiempo no debe usar Window functions — incompatible con incremental refresh"


# ─── Dim_Cliente — Vista Materializada Tipo 1 ────────────────────────────────


def test_dim_cliente_usa_materialized_view():
    codigo = _codigo("DimCliente")
    assert "@dp.materialized_view" in codigo, \
        "Dim_Cliente debe usar @dp.materialized_view"


def test_dim_cliente_nombre_tres_partes():
    codigo = _codigo("DimCliente")
    assert "Dim_Cliente" in codigo, "Dim_Cliente debe incluir nombre Dim_Cliente"


def test_dim_cliente_lee_hub_y_sats():
    codigo = _codigo("DimCliente")
    assert "Hub_Cliente" in codigo, "Dim_Cliente debe leer Hub_Cliente"
    assert "Sat_Cliente_DatosEstables" in codigo, "Dim_Cliente debe leer Sat_Cliente_DatosEstables"
    assert "Sat_Cliente_Contacto" in codigo, "Dim_Cliente debe leer Sat_Cliente_Contacto"
    assert "Sat_Cliente_Clasificacion" in codigo, "Dim_Cliente debe leer Sat_Cliente_Clasificacion"
    assert "Sat_Cliente_Financiero" in codigo, "Dim_Cliente debe leer Sat_Cliente_Financiero"


def test_dim_cliente_usa_helper_ultimo_por_hash():
    codigo = _codigo("DimCliente")
    assert "obtener_ultimo_por_hash" in codigo, \
        "Dim_Cliente debe usar obtener_ultimo_por_hash para Satellites de estado"


def test_dim_cliente_usa_helper_dim_id_estable():
    codigo = _codigo("DimCliente")
    assert "asignar_dim_id_estable" in codigo, \
        "Dim_Cliente debe usar asignar_dim_id_estable para DimIdCliente"


def test_dim_cliente_columnas_cerradas():
    codigo = _codigo("DimCliente")
    columnas_requeridas = [
        "DimIdCliente", "Hash_Cliente", "IdentificadorCliente",
        "SexoCliente", "EdadCliente", "FechaNacimiento", "PaisResidencia",
        "RangoEtario", "CategoriaIngresos", "NombreCompletoCliente",
        "CorreoElectronico", "TelefonoPrincipal", "CiudadResidencia",
        "EstadoCivil", "OcupacionCliente", "TipoCliente", "SegmentoCliente",
        "RegionGeografica", "NivelRiesgo", "IndicadorVip", "EstadoKyc",
        "CalificacionCrediticia", "ScoreCliente", "IngresosCliente",
        "CantidadCuentas", "CantidadTransacciones", "FechaAperturaRelacion",
        "FechaUltimaActualizacion",
    ]
    for col in columnas_requeridas:
        assert col in codigo, f"Dim_Cliente: falta columna cerrada '{col}'"


def test_dim_cliente_no_propaga_columnas_bronce():
    codigo = _codigo("DimCliente")
    assert "FechaRegistroParquet" not in codigo, \
        "Dim_Cliente no debe propagar FechaRegistroParquet (columna exclusiva de Bronce)"
    assert "_rescued_data" not in codigo, \
        "Dim_Cliente no debe propagar _rescued_data"


def test_dim_cliente_no_propaga_metadata_dv():
    codigo = _codigo("DimCliente")
    assert "Hash_Diferenciador" not in codigo, \
        "Dim_Cliente no debe exponer Hash_Diferenciador en el esquema final"


def test_dim_cliente_configura_liquid_clustering():
    codigo = _codigo("DimCliente")
    assert 'cluster_by=["DimIdCliente"]' in codigo or "cluster_by=['DimIdCliente']" in codigo, \
        "Dim_Cliente debe tener cluster_by=[\"DimIdCliente\"]"


def test_dim_cliente_declara_expectations():
    codigo = _codigo("DimCliente")
    assert "DimIdCliente IS NOT NULL" in codigo, \
        "Dim_Cliente debe declarar expectation: DimIdCliente IS NOT NULL"
    assert "Hash_Cliente IS NOT NULL" in codigo, \
        "Dim_Cliente debe declarar expectation: Hash_Cliente IS NOT NULL"


# ─── Dim_Operacion — Vista Materializada Tipo 1 ──────────────────────────────


def test_dim_operacion_usa_materialized_view():
    codigo = _codigo("DimOperacion")
    assert "@dp.materialized_view" in codigo, \
        "Dim_Operacion debe usar @dp.materialized_view"


def test_dim_operacion_lee_hub_y_sats():
    codigo = _codigo("DimOperacion")
    assert "Hub_Operacion" in codigo, "Dim_Operacion debe leer Hub_Operacion"
    assert "Sat_Operacion_DatosEstables" in codigo, \
        "Dim_Operacion debe leer Sat_Operacion_DatosEstables"
    assert "Sat_Operacion_Montos" in codigo, "Dim_Operacion debe leer Sat_Operacion_Montos"
    assert "Sat_Operacion_FechasEvento" in codigo, \
        "Dim_Operacion debe leer Sat_Operacion_FechasEvento"


def test_dim_operacion_usa_helper_ultimo_por_hash():
    codigo = _codigo("DimOperacion")
    assert "obtener_ultimo_por_hash" in codigo, \
        "Dim_Operacion debe usar obtener_ultimo_por_hash para Satellites de estado"


def test_dim_operacion_usa_helper_dim_id_estable():
    codigo = _codigo("DimOperacion")
    assert "asignar_dim_id_estable" in codigo, \
        "Dim_Operacion debe usar asignar_dim_id_estable para DimIdOperacion"


def test_dim_operacion_columnas_cerradas():
    codigo = _codigo("DimOperacion")
    columnas_requeridas = [
        "DimIdOperacion", "Hash_Operacion", "IdentificadorCliente",
        "SecuenciaSaldo", "TipoCuenta", "MonedaCuenta", "EstadoCuenta",
        "ProductoCuenta", "SubproductoCuenta", "RiesgoCuenta", "RegionCuenta",
        "CategoriaSaldo", "EstadoUtilizacionCredito", "IndicadorSobregiro",
        "SaldoDisponible", "SaldoTotal", "LimiteCredito", "CreditoUtilizado",
        "RatioCuenta", "TasaInteres", "FechaAperturaCuenta",
        "FechaUltimoMovimiento", "FechaCierreCuenta", "FechaActualizacionCuenta",
    ]
    for col in columnas_requeridas:
        assert col in codigo, f"Dim_Operacion: falta columna cerrada '{col}'"


def test_dim_operacion_configura_liquid_clustering():
    codigo = _codigo("DimOperacion")
    assert 'cluster_by=["DimIdOperacion"]' in codigo or "cluster_by=['DimIdOperacion']" in codigo, \
        "Dim_Operacion debe tener cluster_by=[\"DimIdOperacion\"]"


def test_dim_operacion_declara_expectations():
    codigo = _codigo("DimOperacion")
    assert "DimIdOperacion IS NOT NULL" in codigo, \
        "Dim_Operacion debe declarar expectation: DimIdOperacion IS NOT NULL"
    assert "Hash_Operacion IS NOT NULL" in codigo, \
        "Dim_Operacion debe declarar expectation: Hash_Operacion IS NOT NULL"


# ─── Hec_Transacciones_ATM ────────────────────────────────────────────────────


def test_hec_atm_usa_materialized_view():
    codigo = _codigo("HecTransaccionesATM")
    assert "@dp.materialized_view" in codigo, \
        "Hec_Transacciones_ATM debe usar @dp.materialized_view (no Streaming Table)"


def test_hec_atm_filtra_tipos_atm():
    codigo = _codigo("HecTransaccionesATM")
    # El hecho referencia TIPO_DATM/TIPO_CATM solo para derivar EsRetiro/EsDeposito;
    # el filtro real DATM/CATM se aplica aguas arriba en Trx_ATM_Stream.
    assert "TIPO_DATM" in codigo, \
        "Hec_Transacciones_ATM debe referenciar TIPO_DATM (derivación EsRetiro)"
    assert "TIPO_CATM" in codigo, \
        "Hec_Transacciones_ATM debe referenciar TIPO_CATM (derivación EsDeposito)"


def test_hec_atm_no_lee_sats_transaccionales_directamente():
    """Hec consume Trx_ATM_Stream; los Sats transaccionales se leen aguas arriba."""
    codigo = _codigo("HecTransaccionesATM")
    assert "Sat_Transaccion_DatosEstables" not in codigo, \
        "Hec_Transacciones_ATM NO debe leer Sat_Transaccion_DatosEstables directamente (vive en Trx_ATM_Stream)"
    assert "Sat_Transaccion_Montos" not in codigo, \
        "Hec_Transacciones_ATM NO debe leer Sat_Transaccion_Montos directamente (vive en Trx_ATM_Stream)"
    assert "Trx_ATM_Stream" in codigo, \
        "Hec_Transacciones_ATM debe consumir Trx_ATM_Stream"

def test_hec_atm_usa_operacion_dominante():
    codigo = _codigo("HecTransaccionesATM")
    # Solución 1: las FKs DimIdCliente/DimIdOperacion se pre-resuelven en
    # Trx_ATM_Stream (que internamente joinea con Map_Cliente_Operacion_Dominante).
    # El Hec NO consume Map directamente: así evita propagar el changeset
    # masivo de Map (CHANGESET_SIZE_THRESHOLD_EXCEEDED).
    assert "Trx_ATM_Stream" in codigo, \
        "Hec_Transacciones_ATM debe consumir Trx_ATM_Stream (que ya trae las FKs resueltas)"
    assert '"Map_Cliente_Operacion_Dominante"' not in codigo, \
        "Hec_Transacciones_ATM NO debe consumir Map_Cliente_Operacion_Dominante directamente (Solución 1: FKs pre-resueltas en Trx_ATM_Stream)"
    assert "seleccionar_operacion_dominante" not in codigo, \
        "Hec_Transacciones_ATM NO debe invocar seleccionar_operacion_dominante directamente"
    assert "Window" not in codigo, \
        "Hec_Transacciones_ATM NO debe importar ni usar Window (incompatible con refresh incremental)"


def test_hec_atm_usa_lectura_batch_spark_read():
    codigo = _codigo("HecTransaccionesATM")
    assert "spark.read.table" in codigo, \
        "Hec_Transacciones_ATM debe leer todas las fuentes con spark.read.table() (MV batch)"
    assert "spark.readStream" not in codigo, \
        "Hec_Transacciones_ATM NO debe usar spark.readStream: es una Materialized View, no una Streaming Table"
    assert "_marca_duplicado" not in codigo, \
        "Hec_Transacciones_ATM NO debe contener _marca_duplicado: la unicidad la garantiza Plata"


def test_hec_atm_no_contiene_marca_duplicado():
    codigo = _codigo("HecTransaccionesATM")
    assert "_marca_duplicado" not in codigo, \
        "Hec_Transacciones_ATM no debe contener _marca_duplicado: fue eliminado completamente"


def test_hec_atm_clusters_correctos():
    codigo = _codigo("HecTransaccionesATM")
    # cluster_by=["FechaClave", "DimIdCliente"] o variante con comillas simples
    assert "FechaClave" in codigo and "DimIdCliente" in codigo, \
        "Hec_Transacciones_ATM debe incluir FechaClave y DimIdCliente"
    assert "cluster_by=" in codigo, \
        "Hec_Transacciones_ATM debe tener cluster_by"


def test_hec_atm_columnas_cerradas():
    codigo = _codigo("HecTransaccionesATM")
    columnas_requeridas = [
        "FechaClave", "DimIdCliente", "DimIdOperacion",
        "IdentificadorTransaccion", "Hash_Transaccion",
        "TipoTransaccion", "MonedaTransaccion", "EstadoTransaccion",
        "CanalTransaccion", "RangoMontoTransaccion", "ClasificacionCanalATM",
        "MontoPrincipal", "ComisionTransaccion", "TotalTransaccion",
        "EsRetiro", "EsDeposito",
    ]
    for col in columnas_requeridas:
        assert col in codigo, f"Hec_Transacciones_ATM: falta columna cerrada '{col}'"


def test_hec_atm_declara_expectations_fk():
    codigo = _codigo("HecTransaccionesATM")
    assert "DimIdCliente IS NOT NULL" in codigo, \
        "Hec_Transacciones_ATM debe declarar expectation: DimIdCliente IS NOT NULL"
    assert "FechaClave IS NOT NULL" in codigo, \
        "Hec_Transacciones_ATM debe declarar expectation: FechaClave IS NOT NULL"


# ─── Ausencia de APIs prohibidas (todos los notebooks Oro) ───────────────────


def test_oro_no_usa_apis_prohibidas():
    for nombre in ORO:
        codigo = _codigo(nombre)
        assert "sparkContext" not in codigo, \
            f"{nombre}: usa sparkContext (prohibido en Serverless)"
        assert ".cache()" not in codigo, \
            f"{nombre}: usa .cache() (prohibido en Serverless)"
        assert ".persist()" not in codigo, \
            f"{nombre}: usa .persist() (prohibido en Serverless)"
        assert ".rdd" not in codigo, \
            f"{nombre}: usa .rdd (prohibido en Serverless)"
        assert ".parallelize(" not in codigo, \
            f"{nombre}: usa .parallelize() (prohibido en Serverless)"
        assert "@udf" not in codigo, \
            f"{nombre}: usa @udf (prohibido en Serverless)"
        assert "threading" not in codigo, \
            f"{nombre}: usa threading (prohibido en Serverless)"
        assert "sc.broadcast" not in codigo, \
            f"{nombre}: usa sc.broadcast (prohibido — usar F.broadcast)"


def test_oro_nombre_tres_partes_en_materialized_view():
    for nombre in ORO:
        if nombre in ("MapClienteOperacionDominante", "TrxATMEnriquecida"):
            # MVs temporary: no se publican en Unity Catalog, no usan nombre de 3 partes.
            continue
        codigo = _codigo(nombre)
        # Verificar que el decorador usa nombre de 3 partes compuesto dinámicamente
        assert "catalogo_oro" in codigo and "esquema_oro" in codigo, \
            f"{nombre}: debe usar nombre de 3 partes con catalogo_oro y esquema_oro en el decorador"


# ─── Map_Cliente_Operacion_Dominante — MV auxiliar ───────────────────────────


def test_map_dom_usa_materialized_view():
    codigo = _codigo("MapClienteOperacionDominante")
    assert "@dp.materialized_view" in codigo, \
        "Map_Cliente_Operacion_Dominante debe usar @dp.materialized_view"
    assert "Map_Cliente_Operacion_Dominante" in codigo, \
        "Debe registrar el nombre Map_Cliente_Operacion_Dominante"


def test_map_dom_no_usa_window_ni_row_number():
    """Map debe usar groupBy().agg(max(struct(...))) en lugar de funciones de ventana.

    Causa raíz CHANGESET_SIZE_THRESHOLD_EXCEEDED: las funciones de ventana
    no son elegibles para mantenimiento incremental por Enzyme, lo que
    hacía que esta MV cayera en COMPLETE_RECOMPUTE en cada corrida y
    propagara un changeset = 100% de su contenido al hecho.
    """
    util = (Path(__file__).resolve().parent.parent
            / "src" / "LSDP_Lab_DataVault_DWH" / "utilities" / "LSDPUtilidadOro.py"
           ).read_text(encoding="utf-8")
    inicio = util.index("def seleccionar_operacion_dominante")
    siguiente_def = util.find("\ndef ", inicio + 1)
    cuerpo = util[inicio:siguiente_def] if siguiente_def != -1 else util[inicio:]
    # La aplicación real (no la docstring): verificamos que no haya
    # invocaciones a F.row_number() ni .over() en el cuerpo del helper.
    assert "F.row_number()" not in cuerpo, \
        "seleccionar_operacion_dominante NO debe invocar F.row_number() (operador no incrementalizable)"
    assert ".over(" not in cuerpo, \
        "seleccionar_operacion_dominante NO debe usar funciones de ventana (.over(...))"
    assert "groupBy(" in cuerpo and "F.max(" in cuerpo and "F.struct(" in cuerpo, \
        "seleccionar_operacion_dominante debe usar groupBy().agg(F.max(F.struct(...)))"


def test_map_dom_lee_hub_y_link_y_dim():
    codigo = _codigo("MapClienteOperacionDominante")
    assert "Hub_Operacion" in codigo
    assert "Link_Cliente_Operacion" in codigo
    assert "Dim_Operacion" in codigo
    assert "Dim_Cliente" in codigo, \
        "Map_Cliente_Operacion_Dominante debe leer Dim_Cliente para resolver DimIdCliente"


def test_map_dom_columnas_cerradas():
    codigo = _codigo("MapClienteOperacionDominante")
    for col in ("Hash_Cliente", "Hash_Operacion", "DimIdCliente", "DimIdOperacion"):
        assert col in codigo, f"Map_Cliente_Operacion_Dominante: falta columna '{col}'"


def test_map_dom_habilita_cdf():
    codigo = _codigo("MapClienteOperacionDominante")
    assert '"delta.enableChangeDataFeed": "true"' in codigo, \
        "Map_Cliente_Operacion_Dominante debe habilitar Change Data Feed"


def test_map_dom_es_temporary():
    """Map_Cliente_Operacion_Dominante debe ser un dataset temporary (no publicado a UC)."""
    codigo = _codigo("MapClienteOperacionDominante")
    assert "temporary=True" in codigo, \
        "Map_Cliente_Operacion_Dominante debe declarar temporary=True para no publicarse en Unity Catalog"


def test_hec_atm_no_consume_map_dom_directamente():
    """Solución 1: el Hec NO debe leer Map_Cliente_Operacion_Dominante.

    Al pre-resolver las FKs en Trx_ATM_Stream, el changeset masivo de Map
    (≈99% por COMPLETE_RECOMPUTE de la MV) deja de propagarse al hecho.
    Esto elimina el bloqueo CHANGESET_SIZE_THRESHOLD_EXCEEDED del cost model.
    """
    codigo = _codigo("HecTransaccionesATM")
    assert '"Map_Cliente_Operacion_Dominante"' not in codigo, \
        "Hec_Transacciones_ATM NO debe referenciar Map_Cliente_Operacion_Dominante (FKs resueltas aguas arriba)"


def test_hec_atm_no_lee_sat_operacion_ni_sat_cliente():
    """El hecho debe resolver FK desde MV auxiliares; no debe leer Sats ni Hubs/Links directamente."""
    codigo = _codigo("HecTransaccionesATM")
    assert "Sat_Operacion" not in codigo, \
        "Hec_Transacciones_ATM no debe leer Sat_Operacion_*"
    assert "Sat_Cliente" not in codigo, \
        "Hec_Transacciones_ATM no debe leer Sat_Cliente_*"
    assert "Hub_Transaccion" not in codigo, \
        "Hec_Transacciones_ATM no debe leer Hub_Transaccion directamente (vive en Trx_ATM_Stream)"
    assert "Link_Cliente_Transaccion" not in codigo, \
        "Hec_Transacciones_ATM no debe leer Link_Cliente_Transaccion directamente (vive en Trx_ATM_Stream)"
    assert "Dim_Cliente" not in codigo, \
        "Hec_Transacciones_ATM no debe leer Dim_Cliente directamente (DimIdCliente viene de Map_Cliente_Operacion_Dominante)"


def test_hec_atm_sin_joins():
    """Solución 1: el hecho debe tener CERO joins.

    Las FKs ya vienen resueltas desde Trx_ATM_Stream. El plan del hecho
    es: read + 2 withColumn + select — trivialmente elegible para
    mantenimiento incremental ROW_BASED.
    """
    codigo = _codigo("HecTransaccionesATM")
    cuenta = codigo.count(".join(")
    assert cuenta == 0, (
        f"Hec_Transacciones_ATM debe contener CERO joins (Solución 1: FKs "
        f"pre-resueltas en Trx_ATM_Stream); encontrados: {cuenta}"
    )


# ─── Trx_ATM_Stream — Streaming Table temporary de pre-enriquecimiento ──────


def test_trx_atm_enriquecida_es_streaming_table():
    """Trx_ATM_Stream es Streaming Table (no MV) para acotar el changeset al hecho.

    Implementación soportada por la API actual de `pyspark.pipelines`:
    `@dp.table(temporary=True, ...)` sobre una función que devuelve un
    DataFrame de streaming (originado por `spark.readStream.table(...)`).
    Lakeflow SDP la materializa como Streaming Table automáticamente.
    No usamos `dp.create_streaming_table(...)` porque esa API no acepta
    `temporary` en el runtime actual.

    El identificador del dataset es `Trx_ATM_Stream` (renombrado desde
    `Trx_ATM_Enriquecida`) para evitar `CANNOT_CHANGE_DATASET_TYPE` al
    redeplegar: SDP no permite cambiar tipo MV→ST bajo el mismo nombre.
    """
    codigo = _codigo("TrxATMEnriquecida")
    assert "@dp.table(" in codigo, \
        "Trx_ATM_Stream debe declararse con @dp.table(...)"
    assert "temporary=True" in codigo, \
        "Trx_ATM_Stream debe declarar temporary=True"
    assert "spark.readStream.table" in codigo, \
        "Trx_ATM_Stream debe usar spark.readStream.table para Sat_Transaccion_DatosEstables (Streaming Table)"
    assert "@dp.materialized_view" not in codigo, \
        "Trx_ATM_Stream ya NO debe ser una @dp.materialized_view (causaba COMPLETE_RECOMPUTE en cada corrida)"
    assert "dp.create_streaming_table(" not in codigo, \
        "No invocar dp.create_streaming_table(...) (no acepta temporary en el runtime actual)"
    assert 'name="Trx_ATM_Stream"' in codigo, \
        "Debe registrar el nombre Trx_ATM_Stream (no Trx_ATM_Enriquecida) para evitar CANNOT_CHANGE_DATASET_TYPE"


def test_trx_atm_enriquecida_es_temporary():
    codigo = _codigo("TrxATMEnriquecida")
    assert "temporary=True" in codigo, \
        "Trx_ATM_Stream debe declarar temporary=True (no publicada en Unity Catalog)"


def test_trx_atm_enriquecida_habilita_cdf():
    codigo = _codigo("TrxATMEnriquecida")
    assert '"delta.enableChangeDataFeed": "true"' in codigo, \
        "Trx_ATM_Stream debe habilitar Change Data Feed"


def test_trx_atm_enriquecida_filtra_datm_catm_aguas_arriba():
    codigo = _codigo("TrxATMEnriquecida")
    assert "TIPO_DATM" in codigo and "TIPO_CATM" in codigo, \
        "Trx_ATM_Stream debe aplicar el filtro DATM/CATM aguas arriba"
    assert ".isin(TIPO_DATM, TIPO_CATM)" in codigo or "TIPO_DATM, TIPO_CATM" in codigo, \
        "Trx_ATM_Stream debe filtrar tipo_transaccion contra DATM/CATM"


def test_trx_atm_enriquecida_lee_los_cinco_datasets_upstream():
    """Solución 1: Trx_ATM_Stream pre-resuelve FKs leyendo Map_Cliente_Operacion_Dominante.

    Esto absorbe el join Hash_Cliente que antes vivía en el Hec, dejándolo
    con plan = read + 2 withColumn + select (cero joins).
    """
    codigo = _codigo("TrxATMEnriquecida")
    for tabla in (
        "Sat_Transaccion_DatosEstables",
        "Sat_Transaccion_Montos",
        "Hub_Transaccion",
        "Link_Cliente_Transaccion",
        "Map_Cliente_Operacion_Dominante",
    ):
        assert tabla in codigo, f"Trx_ATM_Stream debe leer {tabla}"


def test_trx_atm_enriquecida_columnas_cerradas():
    """Solución 1: el esquema cerrado debe incluir DimIdCliente y DimIdOperacion."""
    codigo = _codigo("TrxATMEnriquecida")
    for col in (
        "Hash_Transaccion", "Hash_Cliente", "IdentificadorTransaccion",
        "FechaClave", "TipoTransaccion", "MonedaTransaccion",
        "EstadoTransaccion", "CanalTransaccion", "ClasificacionCanalATM",
        "MontoPrincipal", "ComisionTransaccion", "TotalTransaccion",
        "RangoMontoTransaccion",
        "DimIdCliente", "DimIdOperacion",
    ):
        assert col in codigo, f"Trx_ATM_Stream: falta columna cerrada '{col}'"


def test_trx_atm_enriquecida_target_file_size_reforzado():
    """Solución 3: archivos pequeños bajan el coste estimado de ROW_BASED."""
    codigo = _codigo("TrxATMEnriquecida")
    assert '"delta.targetFileSize": "16mb"' in codigo, \
        "Trx_ATM_Stream debe declarar delta.targetFileSize=16mb (Solución 3)"
    assert '"delta.tuneFileSizesForRewrites": "true"' in codigo, \
        "Trx_ATM_Stream debe declarar delta.tuneFileSizesForRewrites=true (Solución 3)"


def test_hec_atm_target_file_size_reforzado():
    """Solución 3: archivos pequeños permiten al cost model elegir ROW_BASED."""
    codigo = _codigo("HecTransaccionesATM")
    assert '"delta.targetFileSize": "16mb"' in codigo, \
        "Hec_Transacciones_ATM debe declarar delta.targetFileSize=16mb (Solución 3)"
    assert '"delta.tuneFileSizesForRewrites": "true"' in codigo, \
        "Hec_Transacciones_ATM debe declarar delta.tuneFileSizesForRewrites=true (Solución 3)"


def test_hec_atm_referencia_trx_enriquecida_por_nombre_no_calificado():
    codigo = _codigo("HecTransaccionesATM")
    assert '"Trx_ATM_Stream"' in codigo, \
        "Hec_Transacciones_ATM debe referenciar Trx_ATM_Stream por nombre no calificado (dataset temporary)"


# ─── B.2 — Deduplicación de fuentes estáticas en Trx_ATM_Stream ─────────────


def test_trx_atm_stream_deduplica_fuentes_estaticas_por_hash_transaccion():
    """B.2: las tres fuentes estáticas deben llevar dropDuplicates(["Hash_Transaccion"]).

    Defensa en profundidad: garantiza que residuos de duplicados upstream
    (anteriores a la corrección B.1 o por reingesta excepcional) no produzcan
    fan-out en el resultado de Trx_ATM_Stream y por ende en Hec_Transacciones_ATM.
    Las tres fuentes estáticas (sat_montos, hub_trx, link_cli_trx) deben
    deduplicarse por Hash_Transaccion antes de participar en el stream-static join.
    """
    codigo = _codigo("TrxATMEnriquecida")
    assert 'dropDuplicates(["Hash_Transaccion"])' in codigo or \
           "dropDuplicates(['Hash_Transaccion'])" in codigo, (
        "Trx_ATM_Stream: las fuentes estáticas deben usar "
        'dropDuplicates(["Hash_Transaccion"]) para prevenir fan-out (B.2)'
    )


def test_trx_atm_stream_sat_montos_deduplica():
    """B.2: Sat_Transaccion_Montos debe deduplicarse antes del join."""
    codigo = _codigo("TrxATMEnriquecida")
    # sat_montos select + dropDuplicates debe aparecer antes del join con sat_datos
    idx_montos = codigo.find("Sat_Transaccion_Montos")
    idx_dedup = codigo.find('dropDuplicates(["Hash_Transaccion"])', idx_montos)
    assert idx_montos != -1 and idx_dedup != -1, (
        "sat_montos debe aplicar dropDuplicates([\"Hash_Transaccion\"]) después del select"
    )


def test_trx_atm_stream_hub_trx_deduplica():
    """B.2: Hub_Transaccion debe deduplicarse antes del join."""
    codigo = _codigo("TrxATMEnriquecida")
    idx_hub = codigo.find("Hub_Transaccion")
    idx_dedup = codigo.find('dropDuplicates(["Hash_Transaccion"])', idx_hub)
    assert idx_hub != -1 and idx_dedup != -1, (
        "hub_trx debe aplicar dropDuplicates([\"Hash_Transaccion\"]) después del select"
    )


def test_trx_atm_stream_link_cli_trx_deduplica():
    """B.2: Link_Cliente_Transaccion debe deduplicarse antes del join."""
    codigo = _codigo("TrxATMEnriquecida")
    idx_link = codigo.find("Link_Cliente_Transaccion")
    idx_dedup = codigo.find('dropDuplicates(["Hash_Transaccion"])', idx_link)
    assert idx_link != -1 and idx_dedup != -1, (
        "link_cli_trx debe aplicar dropDuplicates([\"Hash_Transaccion\"]) después del select"
    )
