# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataSatOperacion — 3 Satellites de Operación (Streaming Tables Acumulativas)
# ---------------------------------------------------------------------------
# Fuente: {catalogo}.{esquema}.BLNCFL (lectura única compartida)
# Sat_Operacion_DatosEstables  · 36 cols
# Sat_Operacion_Montos         · 38 cols
# Sat_Operacion_FechasEvento   · 23 cols
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import (
    obtener_configuracion,
    UMBRAL_CATEGORIA_SALDO,
    UMBRAL_UTILIZACION_CREDITO,
    UMBRAL_SOBREGIRO,
)
from utilities.LSDPUtilidadPrincipal import (
    calcular_hash_hub,
    calcular_hash_diferenciador,
    procesar_satellite,
    clasificar_por_umbral,
)

config = obtener_configuracion(spark)
_catalogo_plata = config["catalogo_plata"]
_esquema_plata = config["esquema_plata"]
_fuente = f"{config['catalogo']}.{config['esquema']}.BLNCFL"

_PROP_TABLE = {
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days",
}

# ─── Definición de las 3 Streaming Tables ─────────────────────────────────

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Sat_Operacion_DatosEstables",
    cluster_by=["FechaRegistro", "Hash_Operacion"],
    expect_all_or_fail={"hash_diferenciador_no_nulo": "Hash_Diferenciador IS NOT NULL"},
    table_properties=_PROP_TABLE,
)

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Sat_Operacion_Montos",
    cluster_by=["FechaRegistro", "Hash_Operacion"],
    expect_all_or_fail={"hash_diferenciador_no_nulo": "Hash_Diferenciador IS NOT NULL"},
    table_properties=_PROP_TABLE,
)

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Sat_Operacion_FechasEvento",
    cluster_by=["FechaRegistro", "Hash_Operacion"],
    expect_all_or_fail={"hash_diferenciador_no_nulo": "Hash_Diferenciador IS NOT NULL"},
    table_properties=_PROP_TABLE,
)


# ─── Lectura única de Bronce (streaming para append_flow) ─────────────────
def _leer_blncfl():
    return dp.read_stream("BLNCFL_temp")


# ─── Sat_Operacion_DatosEstables ─────────────────────────────────────────

@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Sat_Operacion_DatosEstables")
def sat_operacion_datos_estables():
    df = _leer_blncfl()
    hash_operacion = calcular_hash_hub([F.col("CUSTID"), F.col("BLSQ")])

    datos = df.select(
        hash_operacion.alias("Hash_Operacion"),
        F.col("BLACT").alias("tipo_cuenta"),
        F.col("BLACN").alias("numero_cuenta"),
        F.col("BLCUR").alias("moneda_cuenta"),
        F.col("BLST").alias("estado_cuenta"),
        F.col("BLBR").alias("sucursal_cuenta"),
        F.col("BLPR").alias("producto_cuenta"),
        F.col("BLSP").alias("subproducto_cuenta"),
        F.col("BLNM").alias("nombre_cuenta"),
        F.col("BLCL").alias("clase_cuenta"),
        F.col("BLRK").alias("riesgo_cuenta"),
        F.col("BLTP").alias("tipo_producto_cuenta"),
        F.col("BLMG").alias("gerente_cuenta"),
        F.col("BLRF").alias("referencia_cuenta"),
        F.col("BLCC").alias("centro_costos_cuenta"),
        F.col("BLAG").alias("grupo_afinidad_cuenta"),
        F.col("BLPL").alias("plan_cuenta"),
        F.col("BLRG").alias("region_cuenta"),
        F.col("BLSF").alias("sufijo_cuenta"),
        F.col("BLNT").alias("nota_cuenta"),
        F.col("BLLC").alias("ultimo_canal_cuenta"),
        F.col("BLPF").alias("perfil_cuenta"),
        F.col("BLAU").alias("autorizado_cuenta"),
        F.col("BLTX").alias("texto_cuenta"),
        F.col("BLGR").alias("grupo_cuenta"),
        F.col("BLEM").alias("email_cuenta"),
        F.col("BLFR").alias("frecuencia_cuenta"),
        F.col("BLKY").alias("clave_cuenta"),
        F.col("BLVP").alias("vip_cuenta"),
        F.col("BLFC").alias("factor_cuenta"),
        clasificar_por_umbral(F.col("BLAV"), UMBRAL_CATEGORIA_SALDO).alias("CategoriaSaldo"),
        clasificar_por_umbral(F.col("BLRT"), UMBRAL_UTILIZACION_CREDITO).alias("EstadoUtilizacionCredito"),
        clasificar_por_umbral(F.col("BLOV"), UMBRAL_SOBREGIRO).alias("IndicadorSobregiro"),
    )

    cols_negocio = [
        F.col("tipo_cuenta"), F.col("numero_cuenta"), F.col("moneda_cuenta"),
        F.col("estado_cuenta"), F.col("sucursal_cuenta"), F.col("producto_cuenta"),
        F.col("subproducto_cuenta"), F.col("nombre_cuenta"), F.col("clase_cuenta"),
        F.col("riesgo_cuenta"), F.col("tipo_producto_cuenta"), F.col("gerente_cuenta"),
        F.col("referencia_cuenta"), F.col("centro_costos_cuenta"), F.col("grupo_afinidad_cuenta"),
        F.col("plan_cuenta"), F.col("region_cuenta"), F.col("sufijo_cuenta"),
        F.col("nota_cuenta"), F.col("ultimo_canal_cuenta"), F.col("perfil_cuenta"),
        F.col("autorizado_cuenta"), F.col("texto_cuenta"), F.col("grupo_cuenta"),
        F.col("email_cuenta"), F.col("frecuencia_cuenta"), F.col("clave_cuenta"),
        F.col("vip_cuenta"), F.col("factor_cuenta"),
        F.col("CategoriaSaldo"), F.col("EstadoUtilizacionCredito"), F.col("IndicadorSobregiro"),
    ]

    datos = datos.withColumn(
        "Hash_Diferenciador",
        calcular_hash_diferenciador(F.col("Hash_Operacion"), *cols_negocio),
    )
    datos = datos.withColumn("FechaRegistro", F.current_timestamp())
    datos = datos.withColumn("FuenteDatos", F.lit(_fuente))

    cambios = procesar_satellite(
        spark, _catalogo_plata, _esquema_plata,
        "Sat_Operacion_DatosEstables", "Hash_Operacion", datos,
    )
    return cambios.select(
        "FechaRegistro", "Hash_Operacion",
        "tipo_cuenta", "numero_cuenta", "moneda_cuenta", "estado_cuenta",
        "sucursal_cuenta", "producto_cuenta", "subproducto_cuenta", "nombre_cuenta",
        "clase_cuenta", "riesgo_cuenta", "tipo_producto_cuenta", "gerente_cuenta",
        "referencia_cuenta", "centro_costos_cuenta", "grupo_afinidad_cuenta",
        "plan_cuenta", "region_cuenta", "sufijo_cuenta", "nota_cuenta",
        "ultimo_canal_cuenta", "perfil_cuenta", "autorizado_cuenta", "texto_cuenta",
        "grupo_cuenta", "email_cuenta", "frecuencia_cuenta", "clave_cuenta",
        "vip_cuenta", "factor_cuenta",
        "CategoriaSaldo", "EstadoUtilizacionCredito", "IndicadorSobregiro",
        "Hash_Diferenciador", "FuenteDatos",
    )


# ─── Sat_Operacion_Montos ─────────────────────────────────────────────────

@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Sat_Operacion_Montos")
def sat_operacion_montos():
    df = _leer_blncfl()
    hash_operacion = calcular_hash_hub([F.col("CUSTID"), F.col("BLSQ")])

    datos = df.select(
        hash_operacion.alias("Hash_Operacion"),
        F.col("BLAV").alias("saldo_disponible"),
        F.col("BLTB").alias("saldo_total"),
        F.col("BLRV").alias("saldo_reservado"),
        F.col("BLBK").alias("saldo_bloqueado"),
        F.col("BLCR").alias("limite_credito"),
        F.col("BLCU").alias("credito_utilizado"),
        F.col("BLCD").alias("credito_disponible"),
        F.col("BLOV").alias("valor_sobregiro"),
        F.col("BLOL").alias("limite_sobregiro"),
        F.col("BLPD").alias("depositos_pendientes"),
        F.col("BLPC").alias("cargos_pendientes"),
        F.col("BLPA").alias("ajustes_pendientes"),
        F.col("BLDI").alias("depositos_ingreso"),
        F.col("BLWI").alias("retenciones_cuenta"),
        F.col("BLTI").alias("transferencias_ingreso"),
        F.col("BLTC").alias("cargos_transferencia"),
        F.col("BLCA").alias("comisiones_anuales"),
        F.col("BLIM").alias("intereses_mensuales"),
        F.col("BLRF2").alias("reembolsos_cuenta"),
        F.col("BLPN").alias("penalidades_cuenta"),
        F.col("BLBN").alias("bonificaciones_cuenta"),
        F.col("BLAP").alias("ajustes_positivos"),
        F.col("BLAM").alias("ajustes_miscelaneos"),
        F.col("BLAY").alias("ajustes_anuales"),
        F.col("BLHI").alias("marca_alta_saldo"),
        F.col("BLLO").alias("marca_baja_saldo"),
        F.col("BLVR").alias("varianza_saldo"),
        F.col("BLRT").alias("ratio_cuenta"),
        F.col("BLCP").alias("porcentaje_aporte"),
        F.col("BLCI").alias("ingresos_aporte"),
        F.col("BLMN").alias("saldo_minimo"),
        F.col("BLMX").alias("saldo_maximo"),
        F.col("BLIR").alias("tasa_interes"),
        F.col("BLPM").alias("multiplicador_penalidad"),
    )

    cols_negocio = [
        F.col("saldo_disponible"), F.col("saldo_total"), F.col("saldo_reservado"),
        F.col("saldo_bloqueado"), F.col("limite_credito"), F.col("credito_utilizado"),
        F.col("credito_disponible"), F.col("valor_sobregiro"), F.col("limite_sobregiro"),
        F.col("depositos_pendientes"), F.col("cargos_pendientes"), F.col("ajustes_pendientes"),
        F.col("depositos_ingreso"), F.col("retenciones_cuenta"), F.col("transferencias_ingreso"),
        F.col("cargos_transferencia"), F.col("comisiones_anuales"), F.col("intereses_mensuales"),
        F.col("reembolsos_cuenta"), F.col("penalidades_cuenta"), F.col("bonificaciones_cuenta"),
        F.col("ajustes_positivos"), F.col("ajustes_miscelaneos"), F.col("ajustes_anuales"),
        F.col("marca_alta_saldo"), F.col("marca_baja_saldo"), F.col("varianza_saldo"),
        F.col("ratio_cuenta"), F.col("porcentaje_aporte"), F.col("ingresos_aporte"),
        F.col("saldo_minimo"), F.col("saldo_maximo"), F.col("tasa_interes"),
        F.col("multiplicador_penalidad"),
    ]

    datos = datos.withColumn(
        "Hash_Diferenciador",
        calcular_hash_diferenciador(F.col("Hash_Operacion"), *cols_negocio),
    )
    datos = datos.withColumn("FechaRegistro", F.current_timestamp())
    datos = datos.withColumn("FuenteDatos", F.lit(_fuente))

    cambios = procesar_satellite(
        spark, _catalogo_plata, _esquema_plata,
        "Sat_Operacion_Montos", "Hash_Operacion", datos,
    )
    return cambios.select(
        "FechaRegistro", "Hash_Operacion",
        "saldo_disponible", "saldo_total", "saldo_reservado", "saldo_bloqueado",
        "limite_credito", "credito_utilizado", "credito_disponible", "valor_sobregiro",
        "limite_sobregiro", "depositos_pendientes", "cargos_pendientes", "ajustes_pendientes",
        "depositos_ingreso", "retenciones_cuenta", "transferencias_ingreso",
        "cargos_transferencia", "comisiones_anuales", "intereses_mensuales",
        "reembolsos_cuenta", "penalidades_cuenta", "bonificaciones_cuenta",
        "ajustes_positivos", "ajustes_miscelaneos", "ajustes_anuales",
        "marca_alta_saldo", "marca_baja_saldo", "varianza_saldo",
        "ratio_cuenta", "porcentaje_aporte", "ingresos_aporte",
        "saldo_minimo", "saldo_maximo", "tasa_interes", "multiplicador_penalidad",
        "Hash_Diferenciador", "FuenteDatos",
    )


# ─── Sat_Operacion_FechasEvento ──────────────────────────────────────────

@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Sat_Operacion_FechasEvento")
def sat_operacion_fechas_evento():
    df = _leer_blncfl()
    hash_operacion = calcular_hash_hub([F.col("CUSTID"), F.col("BLSQ")])

    datos = df.select(
        hash_operacion.alias("Hash_Operacion"),
        F.col("BLOD").alias("fecha_apertura_cuenta"),
        F.col("BLXD").alias("fecha_expiracion_cuenta"),
        F.col("BLUD").alias("fecha_actualizacion_cuenta"),
        F.col("BLLD").alias("fecha_ultimo_movimiento"),
        F.col("BLSD").alias("fecha_estado_cuenta"),
        F.col("BLPD2").alias("fecha_penalidad"),
        F.col("BLRD").alias("fecha_renovacion_cuenta"),
        F.col("BLMD").alias("fecha_maduracion"),
        F.col("BLCD2").alias("fecha_cierre_cuenta"),
        F.col("BLBD").alias("fecha_bloqueo_cuenta"),
        F.col("BLFD").alias("fecha_fondeo"),
        F.col("BLGD").alias("fecha_gracia"),
        F.col("BLHD").alias("fecha_historica"),
        F.col("BLID").alias("fecha_interes"),
        F.col("BLJD").alias("fecha_ajuste"),
        F.col("BLKD").alias("fecha_kyc_cuenta"),
        F.col("BLND").alias("fecha_notificacion_cuenta"),
        F.col("BLTD").alias("fecha_transferencia"),
        F.col("BLVD").alias("fecha_verificacion_cuenta"),
    )

    cols_negocio = [
        F.col("fecha_apertura_cuenta"), F.col("fecha_expiracion_cuenta"),
        F.col("fecha_actualizacion_cuenta"), F.col("fecha_ultimo_movimiento"),
        F.col("fecha_estado_cuenta"), F.col("fecha_penalidad"), F.col("fecha_renovacion_cuenta"),
        F.col("fecha_maduracion"), F.col("fecha_cierre_cuenta"), F.col("fecha_bloqueo_cuenta"),
        F.col("fecha_fondeo"), F.col("fecha_gracia"), F.col("fecha_historica"),
        F.col("fecha_interes"), F.col("fecha_ajuste"), F.col("fecha_kyc_cuenta"),
        F.col("fecha_notificacion_cuenta"), F.col("fecha_transferencia"),
        F.col("fecha_verificacion_cuenta"),
    ]

    datos = datos.withColumn(
        "Hash_Diferenciador",
        calcular_hash_diferenciador(F.col("Hash_Operacion"), *cols_negocio),
    )
    datos = datos.withColumn("FechaRegistro", F.current_timestamp())
    datos = datos.withColumn("FuenteDatos", F.lit(_fuente))

    cambios = procesar_satellite(
        spark, _catalogo_plata, _esquema_plata,
        "Sat_Operacion_FechasEvento", "Hash_Operacion", datos,
    )
    return cambios.select(
        "FechaRegistro", "Hash_Operacion",
        "fecha_apertura_cuenta", "fecha_expiracion_cuenta", "fecha_actualizacion_cuenta",
        "fecha_ultimo_movimiento", "fecha_estado_cuenta", "fecha_penalidad",
        "fecha_renovacion_cuenta", "fecha_maduracion", "fecha_cierre_cuenta",
        "fecha_bloqueo_cuenta", "fecha_fondeo", "fecha_gracia", "fecha_historica",
        "fecha_interes", "fecha_ajuste", "fecha_kyc_cuenta",
        "fecha_notificacion_cuenta", "fecha_transferencia", "fecha_verificacion_cuenta",
        "Hash_Diferenciador", "FuenteDatos",
    )
