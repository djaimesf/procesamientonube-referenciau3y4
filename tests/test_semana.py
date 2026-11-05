"""Pruebas de la semana del DAG de la U7. Sin Spark ni GCP: corren en .venv-airflow."""
import pytest

from churn.semana import filas_campana, lotes_hasta, tasa_cuarentena, umbral_alerta


# ---------------------------------------------------------------- lotes_hasta
def test_lunes_14_procesa_7_lotes():
    lotes = lotes_hasta("2026-09-14")
    assert len(lotes) == 7
    assert lotes[0] == "2026-08-03"
    assert lotes[-1] == "2026-09-14"


def test_lunes_28_procesa_9_lotes():
    assert lotes_hasta("2026-09-28")[-2:] == ["2026-09-21", "2026-09-28"]
    assert len(lotes_hasta("2026-09-28")) == 9


def test_primer_lunes_procesa_1_lote():
    assert lotes_hasta("2026-08-03") == ["2026-08-03"]


def test_fecha_que_no_es_lunes_falla():
    with pytest.raises(ValueError):
        lotes_hasta("2026-09-15")


def test_fecha_antes_del_primer_lote_falla():
    with pytest.raises(ValueError):
        lotes_hasta("2026-07-27")


# ---------------------------------------------------------------- tasa_cuarentena
def test_tasa_lote_del_14():
    # Numeros reales de la revision del lote del 14 de septiembre
    assert tasa_cuarentena({"filas_leidas": 953, "buenas": 939}) == pytest.approx(0.0147, abs=1e-4)


def test_tasa_lote_vacio_es_cero():
    assert tasa_cuarentena({"filas_leidas": 0, "buenas": 0}) == 0.0


# ---------------------------------------------------------------- umbral_alerta
# Tasas reales de los 6 lotes que ya estaban en el gold (U3 y U4)
HISTORIA = [0.0, 0.005061, 0.013825, 0.0, 0.006135, 0.010417]


def test_umbral_con_la_historia_de_6_lotes():
    # Q1 = 0,13 %, Q3 = 0,93 % -> Q3 + 1,5 x IQR = 2,15 %
    assert umbral_alerta(HISTORIA) == pytest.approx(0.0215, abs=1e-4)


def test_el_14_no_alerta():
    assert tasa_cuarentena({"filas_leidas": 953, "buenas": 939}) < umbral_alerta(HISTORIA)


def test_el_21_alerta():
    historia = HISTORIA + [0.014690]  # se suma el lote del 14
    assert tasa_cuarentena({"filas_leidas": 987, "buenas": 943}) > umbral_alerta(historia)


def test_sin_historia_suficiente_no_hay_umbral():
    assert umbral_alerta([0.0, 0.01, 0.02]) is None


# ---------------------------------------------------------------- filas_campana
RESPUESTA = {
    "fecha_lote": "2026-09-14", "clientes": 2, "a_llamar": 1, "umbral": 0.3016,
    "modelo": "xgb_uso", "predicho_en": "2026-11-05T11:00:00Z",
    "ranking": [
        {"customerID": "0001-AAAAA", "probabilidad": 0.91, "llamar": True},
        {"customerID": "0002-BBBBB", "probabilidad": 0.12, "llamar": False},
    ],
}


def test_una_fila_por_cliente_en_orden():
    filas = filas_campana(RESPUESTA, "u7-class-sb-x", "1")
    assert [f["posicion"] for f in filas] == [1, 2]
    assert filas[0]["customerID"] == "0001-AAAAA"


def test_cada_fila_lleva_el_rastro():
    fila = filas_campana(RESPUESTA, "u7-class-sb-x", "1")[1]
    assert fila["fecha_lote"] == "2026-09-14"
    assert fila["modelo"] == "xgb_uso"
    assert fila["version_registro"] == "1"
    assert fila["id_ejecucion"] == "u7-class-sb-x"
    assert fila["llamar"] is False


def test_hora_en_formato_de_bigquery():
    fila = filas_campana(RESPUESTA, "u7-class-sb-x", "1")[0]
    assert fila["predicho_en"] == "2026-11-05 11:00:00+00:00"
