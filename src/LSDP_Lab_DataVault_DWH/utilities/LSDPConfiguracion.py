# ---------------------------------------------------------------------------
# LSDPConfiguracion.py — Módulo de Configuración Centralizada del Pipeline
# ---------------------------------------------------------------------------
# Módulo Python puro (NO es source_code LSDP). Centraliza:
#   • Función obtener_configuracion(spark) → dict con 13 parámetros del pipeline
#   • Constantes de negocio inmutables (tipos ATM, hash, separador)
#   • Diccionarios de umbrales de campos calculados
# ---------------------------------------------------------------------------

# === Constantes de Negocio ===

TIPO_DATM = "DATM"
TIPO_CATM = "CATM"
TIPOS_ATM = [TIPO_DATM, TIPO_CATM]

HASH_HUB_LINK_BITS = 256
HASH_SATELLITE_BITS = 512
HASH_SEPARATOR = "|"

# === Umbrales de Campos Calculados ===

UMBRAL_RANGO_ETARIO = {
    "JOVEN_ADULTO": (18, 25),
    "ADULTO": (26, 35),
    "ADULTO_MEDIO": (36, 45),
    "ADULTO_MAYOR": (46, 55),
    "SENIOR": (56, 999),
}

UMBRAL_CATEGORIA_INGRESOS = {
    "BAJO": (0, 15000),
    "MEDIO": (15001, 35000),
    "ALTO": (35001, 65000),
    "MUY_ALTO": (65001, 85000),
    "PREMIUM": (85001, 999999999),
}

UMBRAL_CATEGORIA_SALDO = {
    "BAJO": (0, 10000),
    "MEDIO": (10001, 30000),
    "ALTO": (30001, 60000),
    "MUY_ALTO": (60001, 90000),
    "PREMIUM": (90001, 999999999),
}

UMBRAL_UTILIZACION_CREDITO = {
    "SIN_USO": (0, 0),
    "USO_BAJO": (0.001, 0.05),
    "USO_MODERADO": (0.051, 0.10),
    "USO_ALTO": (0.101, 0.15),
    "SOBRE_UTILIZADO": (0.151, 1.0),
}

UMBRAL_SOBREGIRO = {
    "SIN_SOBREGIRO": (0, 100),
    "SOBREGIRO_LEVE": (101, 1000),
    "SOBREGIRO_MODERADO": (1001, 3000),
    "SOBREGIRO_CRITICO": (3001, 999999999),
}

UMBRAL_RANGO_MONTO = {
    "MICRO": (0, 1000),
    "PEQUENA": (1001, 10000),
    "MEDIANA": (10001, 50000),
    "GRANDE": (50001, 90000),
    "MUY_GRANDE": (90001, 999999999),
}

UMBRAL_RIESGO_FRAUDE = {
    "SIN_RIESGO": (0, 20),
    "RIESGO_BAJO": (21, 40),
    "RIESGO_MODERADO": (41, 60),
    "RIESGO_ALTO": (61, 80),
    "RIESGO_CRITICO": (81, 100),
}


# === Función de Configuración del Pipeline ===

def obtener_configuracion(spark):
    catalogo = spark.conf.get("pipeline.catalogo")
    esquema = spark.conf.get("pipeline.esquema")
    volumen = spark.conf.get("pipeline.volumen")
    base_volumen = f"/Volumes/{catalogo}/{esquema}/{volumen}"

    return {
        "catalogo": catalogo,
        "esquema": esquema,
        "volumen": volumen,
        "catalogo_plata": spark.conf.get("pipeline.catalogo_plata"),
        "esquema_plata": spark.conf.get("pipeline.esquema_plata"),
        "catalogo_oro": spark.conf.get("pipeline.catalogo_oro"),
        "esquema_oro": spark.conf.get("pipeline.esquema_oro"),
        "ruta_cmstfl": f"{base_volumen}/{spark.conf.get('pipeline.ruta_cmstfl')}",
        "ruta_trxpfl": f"{base_volumen}/{spark.conf.get('pipeline.ruta_trxpfl')}",
        "ruta_blncfl": f"{base_volumen}/{spark.conf.get('pipeline.ruta_blncfl')}",
        "schema_location_cmstfl": f"{base_volumen}/{spark.conf.get('pipeline.schema_location_cmstfl')}",
        "schema_location_trxpfl": f"{base_volumen}/{spark.conf.get('pipeline.schema_location_trxpfl')}",
        "schema_location_blncfl": f"{base_volumen}/{spark.conf.get('pipeline.schema_location_blncfl')}",
    }
