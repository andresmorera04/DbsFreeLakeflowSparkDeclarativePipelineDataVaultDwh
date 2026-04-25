# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataSatTransaccion — 2 Satellites de Transacción (Streaming Tables Acumulativas)
# ---------------------------------------------------------------------------
# Fuente: {catalogo}.{esquema}.TRXPFL (lectura única compartida)
# Sat_Transaccion_DatosEstables  · 34 cols
# Sat_Transaccion_Montos         · 36 cols
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import (
    obtener_configuracion,
    TIPO_DATM,
    TIPO_CATM,
    UMBRAL_RANGO_MONTO,
    UMBRAL_RIESGO_FRAUDE,
)
from utilities.LSDPUtilidadPrincipal import (
    calcular_hash_hub,
    calcular_hash_diferenciador,
    procesar_satellite_transaccional,
    clasificar_por_umbral,
)

config = obtener_configuracion(spark)
_catalogo_plata = config["catalogo_plata"]
_esquema_plata = config["esquema_plata"]
_fuente = f"{config['catalogo']}.{config['esquema']}.TRXPFL"

_PROP_TABLE = {
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days",
}

# ─── Definición de las 2 Streaming Tables ─────────────────────────────────

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Sat_Transaccion_DatosEstables",
    cluster_by=["FechaRegistro", "Hash_Transaccion"],
    expect_all_or_fail={"hash_diferenciador_no_nulo": "Hash_Diferenciador IS NOT NULL"},
    table_properties=_PROP_TABLE,
)

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Sat_Transaccion_Montos",
    cluster_by=["FechaRegistro", "Hash_Transaccion"],
    expect_all_or_drop={"monto_transaccion_positivo": "monto_principal > 0"},
    expect_all_or_fail={"hash_diferenciador_no_nulo": "Hash_Diferenciador IS NOT NULL"},
    table_properties=_PROP_TABLE,
)


# ─── Lectura única de Bronce (streaming para append_flow) ─────────────────
def _leer_trxpfl():
    return dp.read_stream(_fuente)


# ─── Sat_Transaccion_DatosEstables ───────────────────────────────────────

@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Sat_Transaccion_DatosEstables")
def sat_transaccion_datos_estables():
    df = _leer_trxpfl()
    hash_transaccion = calcular_hash_hub([F.col("TRXID")])

    # ClasificacionCanalATM: lógica condicional — NO usa clasificar_por_umbral()
    clasificacion_canal_atm = (
        F.when(F.col("TRXTYP") == TIPO_DATM, F.lit("RETIRO_ATM"))
        .when(F.col("TRXTYP") == TIPO_CATM, F.lit("DEPOSITO_ATM"))
        .when(F.col("TRXCH") == "ATM", F.lit("OTRA_OP_ATM"))
        .otherwise(F.lit("NO_ATM"))
    )

    datos = df.select(
        hash_transaccion.alias("Hash_Transaccion"),
        F.col("TRXDT").alias("fecha_transaccion"),
        F.col("TRXTYP").alias("tipo_transaccion"),
        F.col("TRXCUR").alias("moneda_transaccion"),
        F.col("TRXST").alias("estado_transaccion"),
        F.col("TRXCH").alias("canal_transaccion"),
        F.col("TRXDSC").alias("descripcion_transaccion"),
        F.col("TRXREF").alias("referencia_externa"),
        F.col("TRXSQ").alias("secuencia_transaccion"),
        F.col("TRXMX").alias("monto_maximo"),
        F.col("TRXMN").alias("monto_minimo"),
        F.col("TRXVD").alias("fecha_valor"),
        F.col("TRXPD").alias("fecha_procesamiento"),
        F.col("TRXSD").alias("fecha_liquidacion"),
        F.col("TRXCD").alias("fecha_compensacion"),
        F.col("TRXED").alias("fecha_efectiva"),
        F.col("TRXRD").alias("fecha_reverso"),
        F.col("TRXAD").alias("fecha_autorizacion"),
        F.col("TRXND").alias("fecha_notificacion_trx"),
        F.col("TRXXD").alias("fecha_expiracion_trx"),
        F.col("TRXFD").alias("fecha_fondeo_trx"),
        F.col("TRXGD").alias("fecha_gracia_trx"),
        F.col("TRXHD").alias("fecha_historica_trx"),
        F.col("TRXBD").alias("fecha_bloqueo_trx"),
        F.col("TRXMD").alias("fecha_maduracion_trx"),
        F.col("TRXLD").alias("fecha_limite_trx"),
        F.col("TRXUD").alias("fecha_actualizacion_trx"),
        F.col("TRXOD").alias("fecha_origen_trx"),
        F.col("TRXKD").alias("fecha_kyc_trx"),
        F.col("TRXTS").alias("timestamp_transaccion"),
        F.col("TRXUS").alias("timestamp_actualizacion"),
        clasificacion_canal_atm.alias("ClasificacionCanalATM"),
    )

    cols_negocio = [
        F.col("fecha_transaccion"), F.col("tipo_transaccion"), F.col("moneda_transaccion"), F.col("estado_transaccion"),
        F.col("canal_transaccion"), F.col("descripcion_transaccion"), F.col("referencia_externa"),
        F.col("secuencia_transaccion"), F.col("monto_maximo"), F.col("monto_minimo"),
        F.col("fecha_valor"), F.col("fecha_procesamiento"), F.col("fecha_liquidacion"),
        F.col("fecha_compensacion"), F.col("fecha_efectiva"), F.col("fecha_reverso"),
        F.col("fecha_autorizacion"), F.col("fecha_notificacion_trx"), F.col("fecha_expiracion_trx"),
        F.col("fecha_fondeo_trx"), F.col("fecha_gracia_trx"), F.col("fecha_historica_trx"),
        F.col("fecha_bloqueo_trx"), F.col("fecha_maduracion_trx"), F.col("fecha_limite_trx"),
        F.col("fecha_actualizacion_trx"), F.col("fecha_origen_trx"), F.col("fecha_kyc_trx"),
        F.col("timestamp_transaccion"), F.col("timestamp_actualizacion"),
        F.col("ClasificacionCanalATM"),
    ]

    datos = datos.withColumn(
        "Hash_Diferenciador",
        calcular_hash_diferenciador(F.col("Hash_Transaccion"), *cols_negocio),
    )
    datos = datos.withColumn("FechaRegistro", F.current_timestamp())
    datos = datos.withColumn("FuenteDatos", F.lit(_fuente))

    cambios = procesar_satellite_transaccional(
        spark, _catalogo_plata, _esquema_plata,
        "Sat_Transaccion_DatosEstables", "Hash_Transaccion", "fecha_transaccion", datos,
    )
    return cambios.select(
        "FechaRegistro", "Hash_Transaccion",
        "fecha_transaccion",
        "tipo_transaccion", "moneda_transaccion", "estado_transaccion",
        "canal_transaccion", "descripcion_transaccion", "referencia_externa",
        "secuencia_transaccion", "monto_maximo", "monto_minimo",
        "fecha_valor", "fecha_procesamiento", "fecha_liquidacion",
        "fecha_compensacion", "fecha_efectiva", "fecha_reverso",
        "fecha_autorizacion", "fecha_notificacion_trx", "fecha_expiracion_trx",
        "fecha_fondeo_trx", "fecha_gracia_trx", "fecha_historica_trx",
        "fecha_bloqueo_trx", "fecha_maduracion_trx", "fecha_limite_trx",
        "fecha_actualizacion_trx", "fecha_origen_trx", "fecha_kyc_trx",
        "timestamp_transaccion", "timestamp_actualizacion",
        "ClasificacionCanalATM",
        "Hash_Diferenciador", "FuenteDatos",
    )


# ─── Sat_Transaccion_Montos ───────────────────────────────────────────────

@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Sat_Transaccion_Montos")
def sat_transaccion_montos():
    df = _leer_trxpfl()
    hash_transaccion = calcular_hash_hub([F.col("TRXID")])

    datos = df.select(
        hash_transaccion.alias("Hash_Transaccion"),
        F.col("CUSTID").alias("identificador_cliente"),
        F.col("TRXDT").alias("fecha_transaccion"),
        F.col("TRXAMT").alias("monto_principal"),
        F.col("TRXCM").alias("comision_transaccion"),
        F.col("TRXBA").alias("saldo_posterior"),
        F.col("TRXBP").alias("saldo_anterior"),
        F.col("TRXTC").alias("cargo_fiscal"),
        F.col("TRXAL").alias("monto_local"),
        F.col("TRXPN").alias("monto_pago"),
        F.col("TRXBF").alias("beneficio_transaccion"),
        F.col("TRXRL").alias("perdida_tasa"),
        F.col("TRXAV").alias("monto_promedio"),
        F.col("TRXDV").alias("desviacion_monto"),
        F.col("TRXRK").alias("riesgo_transaccion"),
        F.col("TRXFR").alias("riesgo_fraude"),
        F.col("TRXLM").alias("limite_transaccion"),
        F.col("TRXLP").alias("porcentaje_limite"),
        F.col("TRXCP").alias("cargo_plataforma"),
        F.col("TRXCI").alias("cargo_institucion"),
        F.col("TRXCF").alias("cargo_extranjero"),
        F.col("TRXCV").alias("cargo_varianza"),
        F.col("TRXSB").alias("subtotal_transaccion"),
        F.col("TRXTL").alias("total_transaccion"),
        F.col("TRXRS").alias("residuo_transaccion"),
        F.col("TRXIM").alias("margen_interes"),
        F.col("TRXNT").alias("monto_neto"),
        F.col("TRXAO").alias("monto_original"),
        F.col("TRXIN").alias("monto_inversion"),
        F.col("TRXDS").alias("descuento_transaccion"),
        F.col("TRXPT").alias("monto_principal_prestamo"),
        clasificar_por_umbral(F.col("TRXAMT"), UMBRAL_RANGO_MONTO).alias("RangoMontoTransaccion"),
        clasificar_por_umbral(F.col("TRXFR"), UMBRAL_RIESGO_FRAUDE).alias("NivelRiesgoFraude"),
    )

    cols_negocio = [
        F.col("identificador_cliente"), F.col("fecha_transaccion"), F.col("monto_principal"),
        F.col("comision_transaccion"), F.col("saldo_posterior"), F.col("saldo_anterior"),
        F.col("cargo_fiscal"), F.col("monto_local"), F.col("monto_pago"),
        F.col("beneficio_transaccion"), F.col("perdida_tasa"), F.col("monto_promedio"),
        F.col("desviacion_monto"), F.col("riesgo_transaccion"), F.col("riesgo_fraude"),
        F.col("limite_transaccion"), F.col("porcentaje_limite"), F.col("cargo_plataforma"),
        F.col("cargo_institucion"), F.col("cargo_extranjero"), F.col("cargo_varianza"),
        F.col("subtotal_transaccion"), F.col("total_transaccion"), F.col("residuo_transaccion"),
        F.col("margen_interes"), F.col("monto_neto"), F.col("monto_original"),
        F.col("monto_inversion"), F.col("descuento_transaccion"), F.col("monto_principal_prestamo"),
        F.col("RangoMontoTransaccion"), F.col("NivelRiesgoFraude"),
    ]

    datos = datos.withColumn(
        "Hash_Diferenciador",
        calcular_hash_diferenciador(F.col("Hash_Transaccion"), *cols_negocio),
    )
    datos = datos.withColumn("FechaRegistro", F.current_timestamp())
    datos = datos.withColumn("FuenteDatos", F.lit(_fuente))

    cambios = procesar_satellite_transaccional(
        spark, _catalogo_plata, _esquema_plata,
        "Sat_Transaccion_Montos", "Hash_Transaccion", "fecha_transaccion", datos,
    )
    return cambios.select(
        "FechaRegistro", "Hash_Transaccion",
        "identificador_cliente", "fecha_transaccion", "monto_principal",
        "comision_transaccion", "saldo_posterior", "saldo_anterior",
        "cargo_fiscal", "monto_local", "monto_pago", "beneficio_transaccion",
        "perdida_tasa", "monto_promedio", "desviacion_monto", "riesgo_transaccion",
        "riesgo_fraude", "limite_transaccion", "porcentaje_limite",
        "cargo_plataforma", "cargo_institucion", "cargo_extranjero", "cargo_varianza",
        "subtotal_transaccion", "total_transaccion", "residuo_transaccion",
        "margen_interes", "monto_neto", "monto_original", "monto_inversion",
        "descuento_transaccion", "monto_principal_prestamo",
        "RangoMontoTransaccion", "NivelRiesgoFraude",
        "Hash_Diferenciador", "FuenteDatos",
    )
