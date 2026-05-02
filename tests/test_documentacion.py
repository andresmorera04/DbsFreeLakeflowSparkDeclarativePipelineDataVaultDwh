"""
tests/test_documentacion.py

Validaciones ligeras (sin Spark) para la documentación y el notebook de metadatos.
Verifica existencia de artefactos, ausencia de nombres obsoletos y coherencia
entre TABLAS_MODELO_DATOS y COMENTARIOS_COLUMNAS/COMENTARIOS_TABLAS.
"""
import re
import ast
import pathlib

REPO_ROOT = pathlib.Path(__file__).parent.parent

# ─────────────────────────────────────────────────────────────────────────────
# 1. Existencia de artefactos obligatorios
# ─────────────────────────────────────────────────────────────────────────────
ARTEFACTOS_REQUERIDOS = [
    "docs/Quickstart.md",
    "docs/ManualTecnico.md",
    "docs/ModeloDatos.md",
    "SYSTEM.md",
    "src/LSDP_Lab_DataVault_DWH/explorations/Metadata/NbComentariosTablas.py",
]


def test_artefactos_existen():
    for rel_path in ARTEFACTOS_REQUERIDOS:
        p = REPO_ROOT / rel_path
        assert p.exists(), f"Artefacto requerido no encontrado: {rel_path}"


# ─────────────────────────────────────────────────────────────────────────────
# 2. Ausencia de "Repos" en Quickstart (Req 5.2)
# ─────────────────────────────────────────────────────────────────────────────
def test_quickstart_sin_repos():
    texto = (REPO_ROOT / "docs/Quickstart.md").read_text(encoding="utf-8")
    assert '"Repos"' not in texto, (
        "docs/Quickstart.md no debe mencionar la sección 'Repos' (Req 5.2)."
    )


# ─────────────────────────────────────────────────────────────────────────────
# 3. Ausencia de "Fact_Transacciones_ATM" en contextos vigentes de SYSTEM.md
#    (se permite en la sección de historial de cambios)
# ─────────────────────────────────────────────────────────────────────────────
def test_system_md_sin_fact_transacciones_atm():
    texto = (REPO_ROOT / "SYSTEM.md").read_text(encoding="utf-8")

    # Separar el cuerpo del historial de cambios
    separador = "# Historial de Cambios"
    partes = texto.split(separador, 1)
    cuerpo_vigente = partes[0]

    ocurrencias = [
        i for i in range(len(cuerpo_vigente))
        if cuerpo_vigente[i:i + len("Fact_Transacciones_ATM")] == "Fact_Transacciones_ATM"
    ]
    assert len(ocurrencias) == 0, (
        f"SYSTEM.md contiene {len(ocurrencias)} ocurrencia(s) de 'Fact_Transacciones_ATM' "
        f"fuera del historial de cambios. Usar 'Hec_Transacciones_ATM'."
    )


# ─────────────────────────────────────────────────────────────────────────────
# 4. NbComentariosTablas: exactamente 21 tablas en TABLAS_MODELO_DATOS
# ─────────────────────────────────────────────────────────────────────────────
def _extraer_set_literal(fuente: str, nombre_variable: str) -> set:
    """
    Extrae el conjunto de strings del primer set-literal asignado a nombre_variable.
    Solo funciona con conjuntos de literales de string simples.
    """
    patron = re.compile(
        rf"^{re.escape(nombre_variable)}\s*=\s*\{{([^}}]+)\}}",
        re.MULTILINE | re.DOTALL,
    )
    match = patron.search(fuente)
    if not match:
        return set()
    contenido = match.group(1)
    # Extraer solo los literales de string, ignorando líneas de comentario
    return {m[0] or m[1] for m in re.findall(r'"([^"]+)"|\x27([^\x27]+)\x27', contenido)}


TABLAS_ESPERADAS = {
    "CMSTFL", "TRXPFL", "BLNCFL",
    "Hub_Cliente", "Hub_Transaccion", "Hub_Operacion",
    "Link_Cliente_Operacion", "Link_Cliente_Transaccion",
    "Sat_Cliente_DatosEstables", "Sat_Cliente_Contacto",
    "Sat_Cliente_Clasificacion", "Sat_Cliente_Financiero",
    "Sat_Operacion_DatosEstables", "Sat_Operacion_Montos",
    "Sat_Operacion_FechasEvento",
    "Sat_Transaccion_DatosEstables", "Sat_Transaccion_Montos",
    "Dim_Cliente", "Dim_Operacion", "Dim_Tiempo",
    "Hec_Transacciones_ATM",
}


def test_nb_comentarios_21_tablas():
    fuente = (
        REPO_ROOT
        / "src/LSDP_Lab_DataVault_DWH/explorations/Metadata/NbComentariosTablas.py"
    ).read_text(encoding="utf-8")

    tablas = _extraer_set_literal(fuente, "TABLAS_MODELO_DATOS")
    assert len(tablas) == 21, (
        f"TABLAS_MODELO_DATOS debe tener 21 entradas, encontradas: {len(tablas)}.\n"
        f"  Tablas: {sorted(tablas)}"
    )
    assert tablas == TABLAS_ESPERADAS, (
        f"TABLAS_MODELO_DATOS no coincide con el conjunto esperado.\n"
        f"  Faltantes: {sorted(TABLAS_ESPERADAS - tablas)}\n"
        f"  Sobrantes: {sorted(tablas - TABLAS_ESPERADAS)}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 5. NbComentariosTablas: assert de paridad precede a la aplicación
#    (celda "## 6." debe ser assert, celda "## 7." debe ser aplicación)
# ─────────────────────────────────────────────────────────────────────────────
def test_nb_comentarios_orden_assert_antes_aplicacion():
    fuente = (
        REPO_ROOT
        / "src/LSDP_Lab_DataVault_DWH/explorations/Metadata/NbComentariosTablas.py"
    ).read_text(encoding="utf-8")

    pos_assert = fuente.find("## 6. Assert de paridad")
    pos_aplic  = fuente.find("## 7. Aplicación de comentarios por medalla")

    assert pos_assert != -1, "No se encontró la sección '## 6. Assert de paridad'."
    assert pos_aplic  != -1, "No se encontró la sección '## 7. Aplicación de comentarios por medalla'."
    assert pos_assert < pos_aplic, (
        "El assert de paridad (celda 6) debe aparecer ANTES de la aplicación (celda 7)."
    )


# ─────────────────────────────────────────────────────────────────────────────
# 6. NbComentariosTablas: SQL de tabla usa COMMENT ON TABLE … IS
# ─────────────────────────────────────────────────────────────────────────────
def test_nb_comentarios_sql_correcto():
    fuente = (
        REPO_ROOT
        / "src/LSDP_Lab_DataVault_DWH/explorations/Metadata/NbComentariosTablas.py"
    ).read_text(encoding="utf-8")

    assert "COMMENT ON TABLE" in fuente, (
        "NbComentariosTablas debe usar 'COMMENT ON TABLE … IS …' para comentar tablas."
    )
    assert "SET TBLPROPERTIES" not in fuente, (
        "NbComentariosTablas no debe usar 'SET TBLPROPERTIES' para comentar tablas."
    )


# ─────────────────────────────────────────────────────────────────────────────
# 7. Encabezados de documentos incluyen campo Autor
# ─────────────────────────────────────────────────────────────────────────────
DOCS_CON_AUTOR = [
    "docs/Quickstart.md",
    "docs/ManualTecnico.md",
    "docs/ModeloDatos.md",
]


def test_docs_tienen_autor():
    for rel_path in DOCS_CON_AUTOR:
        texto = (REPO_ROOT / rel_path).read_text(encoding="utf-8")
        assert "**Autor**" in texto, (
            f"{rel_path} no contiene el campo '**Autor**' en el encabezado."
        )


# ─────────────────────────────────────────────────────────────────────────────
# 8. Quickstart incluye campo Spec activo
# ─────────────────────────────────────────────────────────────────────────────
def test_quickstart_tiene_spec_activo():
    texto = (REPO_ROOT / "docs/Quickstart.md").read_text(encoding="utf-8")
    assert "**Spec activo**" in texto, (
        "docs/Quickstart.md no contiene el campo '**Spec activo**' en el encabezado."
    )
