# Databricks notebook source
# ---------------------------------------------------------------------------
# LSDPPlataSatCliente — 4 Satellites de Cliente (Streaming Tables Acumulativas)
# ---------------------------------------------------------------------------
# Fuente: {catalogo}.{esquema}.CMSTFL (lectura única compartida)
# Sat_Cliente_DatosEstables   · 17 cols
# Sat_Cliente_Contacto        · 19 cols
# Sat_Cliente_Clasificacion   · 23 cols
# Sat_Cliente_Financiero      · 28 cols
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.LSDPConfiguracion import (
    obtener_configuracion,
    UMBRAL_RANGO_ETARIO,
    UMBRAL_CATEGORIA_INGRESOS,
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
_fuente = f"{config['catalogo']}.{config['esquema']}.CMSTFL"

_PROP_TABLE = {
    "delta.autoOptimize.autoCompact": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.deletedFileRetentionDuration": "interval 30 days",
    "delta.logRetentionDuration": "interval 60 days",
}

# ─── Definición de las 4 Streaming Tables ─────────────────────────────────

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Sat_Cliente_DatosEstables",
    cluster_by=["FechaRegistro", "Hash_Cliente"],
    expect_all_or_fail={"hash_diferenciador_no_nulo": "Hash_Diferenciador IS NOT NULL"},
    table_properties=_PROP_TABLE,
)

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Sat_Cliente_Contacto",
    cluster_by=["FechaRegistro", "Hash_Cliente"],
    expect_all_or_fail={"hash_diferenciador_no_nulo": "Hash_Diferenciador IS NOT NULL"},
    table_properties=_PROP_TABLE,
)

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Sat_Cliente_Clasificacion",
    cluster_by=["FechaRegistro", "Hash_Cliente"],
    expect_all_or_fail={"hash_diferenciador_no_nulo": "Hash_Diferenciador IS NOT NULL"},
    table_properties=_PROP_TABLE,
)

dp.create_streaming_table(
    name=f"{_catalogo_plata}.{_esquema_plata}.Sat_Cliente_Financiero",
    cluster_by=["FechaRegistro", "Hash_Cliente"],
    expect_all={"score_cliente_en_rango": "score_cliente BETWEEN 300 AND 1150"},
    expect_all_or_fail={"hash_diferenciador_no_nulo": "Hash_Diferenciador IS NOT NULL"},
    table_properties=_PROP_TABLE,
)


# ─── Lectura única de Bronce (streaming para append_flow) ─────────────────
def _leer_cmstfl():
    return dp.read_stream(_fuente)


# ─── Sat_Cliente_DatosEstables ────────────────────────────────────────────

@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Sat_Cliente_DatosEstables")
def sat_cliente_datos_estables():
    df = _leer_cmstfl()
    hash_cliente = calcular_hash_hub([F.col("CUSTID")])

    datos = df.select(
        hash_cliente.alias("Hash_Cliente"),
        F.col("CUSSX").alias("sexo_cliente"),
        F.col("CUSTT").alias("tratamiento_cliente"),
        F.col("CUSDB").alias("fecha_nacimiento"),
        F.col("CUSYR").alias("anio_nacimiento"),
        F.col("CUSAG2").alias("edad_cliente"),
        F.col("CUSCN").alias("pais_residencia"),
        F.col("CUSNA").alias("nacionalidad_cliente"),
        F.col("CUSDL").alias("numero_licencia_conducir"),
        F.col("CUSDP").alias("tipo_documento_pasaporte"),
        F.col("CUSDP2").alias("cantidad_pasaportes"),
        F.col("CUSLG").alias("idioma_preferido"),
        clasificar_por_umbral(F.col("CUSAG2"), UMBRAL_RANGO_ETARIO).alias("RangoEtario"),
        clasificar_por_umbral(F.col("CUSIN"), UMBRAL_CATEGORIA_INGRESOS).alias("CategoriaIngresos"),
    )

    cols_negocio = [
        F.col("sexo_cliente"), F.col("tratamiento_cliente"), F.col("fecha_nacimiento"),
        F.col("anio_nacimiento"), F.col("edad_cliente"), F.col("pais_residencia"),
        F.col("nacionalidad_cliente"), F.col("numero_licencia_conducir"),
        F.col("tipo_documento_pasaporte"), F.col("cantidad_pasaportes"),
        F.col("idioma_preferido"), F.col("RangoEtario"), F.col("CategoriaIngresos"),
    ]

    datos = datos.withColumn(
        "Hash_Diferenciador",
        calcular_hash_diferenciador(F.col("Hash_Cliente"), *cols_negocio),
    )

    datos = datos.withColumn("FechaRegistro", F.current_timestamp())
    datos = datos.withColumn("FuenteDatos", F.lit(_fuente))

    cambios = procesar_satellite(
        spark, _catalogo_plata, _esquema_plata,
        "Sat_Cliente_DatosEstables", "Hash_Cliente", datos,
    )
    return cambios.select(
        "FechaRegistro", "Hash_Cliente",
        "sexo_cliente", "tratamiento_cliente", "fecha_nacimiento",
        "anio_nacimiento", "edad_cliente", "pais_residencia",
        "nacionalidad_cliente", "numero_licencia_conducir",
        "tipo_documento_pasaporte", "cantidad_pasaportes", "idioma_preferido",
        "RangoEtario", "CategoriaIngresos",
        "Hash_Diferenciador", "FuenteDatos",
    )


# ─── Sat_Cliente_Contacto ────────────────────────────────────────────────

@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Sat_Cliente_Contacto")
def sat_cliente_contacto():
    df = _leer_cmstfl()
    hash_cliente = calcular_hash_hub([F.col("CUSTID")])

    datos = df.select(
        hash_cliente.alias("Hash_Cliente"),
        F.col("CUSNM").alias("nombre_cliente"),
        F.col("CUSLN").alias("apellido_cliente"),
        F.col("CUSMD").alias("nombre_medio_cliente"),
        F.col("CUSFN").alias("nombre_completo_cliente"),
        F.col("CUSAD").alias("direccion_calle"),
        F.col("CUSA2").alias("direccion_apartamento"),
        F.col("CUSCT").alias("ciudad_residencia"),
        F.col("CUSST").alias("estado_provincia"),
        F.col("CUSZP").alias("codigo_postal"),
        F.col("CUSPH").alias("telefono_principal"),
        F.col("CUSMB").alias("telefono_movil"),
        F.col("CUSEM").alias("correo_electronico"),
        F.col("CUSMS").alias("estado_civil"),
        F.col("CUSOC").alias("ocupacion_cliente"),
        F.col("CUSED").alias("nivel_educativo"),
    )

    cols_negocio = [
        F.col("nombre_cliente"), F.col("apellido_cliente"), F.col("nombre_medio_cliente"),
        F.col("nombre_completo_cliente"), F.col("direccion_calle"), F.col("direccion_apartamento"),
        F.col("ciudad_residencia"), F.col("estado_provincia"), F.col("codigo_postal"),
        F.col("telefono_principal"), F.col("telefono_movil"), F.col("correo_electronico"),
        F.col("estado_civil"), F.col("ocupacion_cliente"), F.col("nivel_educativo"),
    ]

    datos = datos.withColumn(
        "Hash_Diferenciador",
        calcular_hash_diferenciador(F.col("Hash_Cliente"), *cols_negocio),
    )
    datos = datos.withColumn("FechaRegistro", F.current_timestamp())
    datos = datos.withColumn("FuenteDatos", F.lit(_fuente))

    cambios = procesar_satellite(
        spark, _catalogo_plata, _esquema_plata,
        "Sat_Cliente_Contacto", "Hash_Cliente", datos,
    )
    return cambios.select(
        "FechaRegistro", "Hash_Cliente",
        "nombre_cliente", "apellido_cliente", "nombre_medio_cliente",
        "nombre_completo_cliente", "direccion_calle", "direccion_apartamento",
        "ciudad_residencia", "estado_provincia", "codigo_postal",
        "telefono_principal", "telefono_movil", "correo_electronico",
        "estado_civil", "ocupacion_cliente", "nivel_educativo",
        "Hash_Diferenciador", "FuenteDatos",
    )


# ─── Sat_Cliente_Clasificacion ───────────────────────────────────────────

@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Sat_Cliente_Clasificacion")
def sat_cliente_clasificacion():
    df = _leer_cmstfl()
    hash_cliente = calcular_hash_hub([F.col("CUSTID")])

    datos = df.select(
        hash_cliente.alias("Hash_Cliente"),
        F.col("CUSTP").alias("tipo_cliente"),
        F.col("CUSSG").alias("segmento_cliente"),
        F.col("CUSRG").alias("region_geografica"),
        F.col("CUSBR").alias("sucursal_principal"),
        F.col("CUSMG").alias("gerente_asignado"),
        F.col("CUSRF").alias("referencia_interna"),
        F.col("CUSRS").alias("fuente_referencia"),
        F.col("CUSAG").alias("grupo_afinidad"),
        F.col("CUSPC").alias("preferencia_comunicacion"),
        F.col("CUSRK").alias("nivel_riesgo"),
        F.col("CUSVP").alias("indicador_vip"),
        F.col("CUSPF").alias("estado_perfil"),
        F.col("CUSKT").alias("estado_kyc"),
        F.col("CUSFM").alias("indicador_flags"),
        F.col("CUSLC").alias("ultimo_canal"),
        F.col("CUSCR").alias("calificacion_crediticia"),
        F.col("CUSAC").alias("cuenta_activa"),
        F.col("CUSCL").alias("clasificacion_interna"),
        F.col("CUSNT").alias("nota_cliente"),
    )

    cols_negocio = [
        F.col("tipo_cliente"), F.col("segmento_cliente"), F.col("region_geografica"),
        F.col("sucursal_principal"), F.col("gerente_asignado"), F.col("referencia_interna"),
        F.col("fuente_referencia"), F.col("grupo_afinidad"), F.col("preferencia_comunicacion"),
        F.col("nivel_riesgo"), F.col("indicador_vip"), F.col("estado_perfil"),
        F.col("estado_kyc"), F.col("indicador_flags"), F.col("ultimo_canal"),
        F.col("calificacion_crediticia"), F.col("cuenta_activa"), F.col("clasificacion_interna"),
        F.col("nota_cliente"),
    ]

    datos = datos.withColumn(
        "Hash_Diferenciador",
        calcular_hash_diferenciador(F.col("Hash_Cliente"), *cols_negocio),
    )
    datos = datos.withColumn("FechaRegistro", F.current_timestamp())
    datos = datos.withColumn("FuenteDatos", F.lit(_fuente))

    cambios = procesar_satellite(
        spark, _catalogo_plata, _esquema_plata,
        "Sat_Cliente_Clasificacion", "Hash_Cliente", datos,
    )
    return cambios.select(
        "FechaRegistro", "Hash_Cliente",
        "tipo_cliente", "segmento_cliente", "region_geografica",
        "sucursal_principal", "gerente_asignado", "referencia_interna",
        "fuente_referencia", "grupo_afinidad", "preferencia_comunicacion",
        "nivel_riesgo", "indicador_vip", "estado_perfil",
        "estado_kyc", "indicador_flags", "ultimo_canal",
        "calificacion_crediticia", "cuenta_activa", "clasificacion_interna",
        "nota_cliente",
        "Hash_Diferenciador", "FuenteDatos",
    )


# ─── Sat_Cliente_Financiero ──────────────────────────────────────────────

@dp.append_flow(target=f"{_catalogo_plata}.{_esquema_plata}.Sat_Cliente_Financiero")
def sat_cliente_financiero():
    df = _leer_cmstfl()
    hash_cliente = calcular_hash_hub([F.col("CUSTID")])

    datos = df.select(
        hash_cliente.alias("Hash_Cliente"),
        F.col("CUSAC2").alias("cantidad_cuentas"),
        F.col("CUSTX").alias("cantidad_transacciones"),
        F.col("CUSSC").alias("score_cliente"),
        F.col("CUSLR").alias("ranking_prestamos"),
        F.col("CUSRC").alias("cantidad_registros"),
        F.col("CUSIN").alias("ingresos_cliente"),
        F.col("CUSBL").alias("saldo_disponible_maestro"),
        F.col("CUSOD").alias("fecha_apertura_relacion"),
        F.col("CUSCD").alias("fecha_cierre_relacion"),
        F.col("CUSLV").alias("fecha_ultima_visita"),
        F.col("CUSUD").alias("fecha_ultima_actualizacion"),
        F.col("CUSKD").alias("fecha_verificacion_kyc"),
        F.col("CUSRD").alias("fecha_renovacion"),
        F.col("CUSXD").alias("fecha_expiracion"),
        F.col("CUSFD").alias("fecha_primer_producto"),
        F.col("CUSLD").alias("fecha_ultimo_producto"),
        F.col("CUSMD2").alias("fecha_migracion"),
        F.col("CUSAD2").alias("fecha_activacion"),
        F.col("CUSBD").alias("fecha_bloqueo"),
        F.col("CUSVD").alias("fecha_verificacion"),
        F.col("CUSPD").alias("fecha_promocion"),
        F.col("CUSDD").alias("fecha_desactivacion"),
        F.col("CUSED2").alias("fecha_educacion_financiera"),
        F.col("CUSND").alias("fecha_notificacion"),
    )

    cols_negocio = [
        F.col("cantidad_cuentas"), F.col("cantidad_transacciones"), F.col("score_cliente"),
        F.col("ranking_prestamos"), F.col("cantidad_registros"), F.col("ingresos_cliente"),
        F.col("saldo_disponible_maestro"), F.col("fecha_apertura_relacion"),
        F.col("fecha_cierre_relacion"), F.col("fecha_ultima_visita"),
        F.col("fecha_ultima_actualizacion"), F.col("fecha_verificacion_kyc"),
        F.col("fecha_renovacion"), F.col("fecha_expiracion"), F.col("fecha_primer_producto"),
        F.col("fecha_ultimo_producto"), F.col("fecha_migracion"), F.col("fecha_activacion"),
        F.col("fecha_bloqueo"), F.col("fecha_verificacion"), F.col("fecha_promocion"),
        F.col("fecha_desactivacion"), F.col("fecha_educacion_financiera"),
        F.col("fecha_notificacion"),
    ]

    datos = datos.withColumn(
        "Hash_Diferenciador",
        calcular_hash_diferenciador(F.col("Hash_Cliente"), *cols_negocio),
    )
    datos = datos.withColumn("FechaRegistro", F.current_timestamp())
    datos = datos.withColumn("FuenteDatos", F.lit(_fuente))

    cambios = procesar_satellite(
        spark, _catalogo_plata, _esquema_plata,
        "Sat_Cliente_Financiero", "Hash_Cliente", datos,
    )
    return cambios.select(
        "FechaRegistro", "Hash_Cliente",
        "cantidad_cuentas", "cantidad_transacciones", "score_cliente",
        "ranking_prestamos", "cantidad_registros", "ingresos_cliente",
        "saldo_disponible_maestro", "fecha_apertura_relacion",
        "fecha_cierre_relacion", "fecha_ultima_visita",
        "fecha_ultima_actualizacion", "fecha_verificacion_kyc",
        "fecha_renovacion", "fecha_expiracion", "fecha_primer_producto",
        "fecha_ultimo_producto", "fecha_migracion", "fecha_activacion",
        "fecha_bloqueo", "fecha_verificacion", "fecha_promocion",
        "fecha_desactivacion", "fecha_educacion_financiera",
        "fecha_notificacion",
        "Hash_Diferenciador", "FuenteDatos",
    )
