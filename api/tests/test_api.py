"""Pruebas de la API sin GCP: modelo de juguete con la MISMA preparación de la U5
y un gold falso en memoria. No descargan nada ni consultan BigQuery."""
import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBClassifier

from app.gold import COLUMNAS_USO
from app.main import app

CATEGORIAS = {
    "MultipleLines": ["No", "No phone service", "Yes"],
    "InternetService": ["DSL", "Fiber optic", "No"],
    **{c: ["No", "No internet service", "Yes"] for c in
       ["OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies"]},
    "Contract": ["Month-to-month", "One year", "Two year"],
    "PaymentMethod": ["Bank transfer (automatic)", "Credit card (automatic)", "Electronic check", "Mailed check"],
}
BINARIAS = ["Partner", "Dependents", "PhoneService", "PaperlessBilling"]
NUM_CRM = ["tenure", "MonthlyCharges", "TotalCharges"]
ENTRADA = ["SeniorCitizen", "gender"] + BINARIAS + NUM_CRM + list(CATEGORIAS) + COLUMNAS_USO

CLIENTE = {
    "customerID": "5685-IIXLY", "gender": "Female", "SeniorCitizen": "0", "Partner": "No",
    "Dependents": "No", "tenure": 5, "PhoneService": "Yes", "MultipleLines": "No",
    "InternetService": "Fiber optic", "OnlineSecurity": "No", "OnlineBackup": "No",
    "DeviceProtection": "No", "TechSupport": "No", "StreamingTV": "No", "StreamingMovies": "No",
    "Contract": "Month-to-month", "PaperlessBilling": "Yes", "PaymentMethod": "Electronic check",
    "MonthlyCharges": 84.0, "TotalCharges": 420.0,
}


def ohe(listas):
    return OneHotEncoder(categories=listas, drop="first", handle_unknown="error", sparse_output=False)


def datos_falsos(n, semilla, fecha):
    r = np.random.default_rng(semilla)
    df = pd.DataFrame({
        "customerID": [f"{i:04d}-FAKE" for i in range(n)],
        "SeniorCitizen": r.choice(["0", "1"], n), "gender": r.choice(["Female", "Male"], n),
        **{c: r.choice(["No", "Yes"], n) for c in BINARIAS},
        "tenure": r.integers(0, 73, n), "MonthlyCharges": r.uniform(18, 119, n),
        "TotalCharges": r.uniform(18, 8000, n),
        **{c: r.choice(v, n) for c, v in CATEGORIAS.items()},
        "n_llamadas": r.integers(0, 900, n), "min_llamadas": r.uniform(0, 3000, n),
        "mb_total": r.uniform(0, 2e5, n), "n_tickets": r.integers(0, 8, n),
        "n_llamadas_caidas": r.integers(0, 40, n), "tasa_caida": r.uniform(0, 0.2, n),
        "fecha_lote": fecha,
    })
    df["tenure"] = df["tenure"].astype("Int64")        # como llega de BigQuery
    df["n_tickets"] = df["n_tickets"].astype("Int64")
    return df


class GoldFalso:
    def __init__(self):
        self.df = pd.concat([datos_falsos(40, 1, "2026-08-31"), datos_falsos(30, 2, "2026-09-07")])
        # 5685-IIXLY está en 2 lotes: la API debe tomar el más reciente
        for f, tickets in [("2026-08-31", 0), ("2026-09-07", 6)]:
            fila = {**CLIENTE, "n_llamadas": 300, "min_llamadas": 500.0, "mb_total": 7e4,
                    "n_tickets": tickets, "n_llamadas_caidas": 10, "tasa_caida": 0.03, "fecha_lote": f}
            self.df = pd.concat([self.df, pd.DataFrame([fila])], ignore_index=True)

    def uso_vigente(self, customer_id):
        f = self.df[self.df.customerID == customer_id].sort_values("fecha_lote", ascending=False)
        return None if f.empty else f.iloc[0][["fecha_lote"] + COLUMNAS_USO].to_dict()

    def lote(self, fecha):
        return self.df[self.df.fecha_lote == fecha].copy()


@pytest.fixture(scope="module")
def cliente():
    entreno = datos_falsos(300, 7, "2026-08-03")
    y = (entreno["n_tickets"].astype(int) >= 4).astype(int)
    prep = ColumnTransformer([
        ("senior", ohe([["0", "1"]]), ["SeniorCitizen"]),
        ("genero", ohe([["Female", "Male"]]), ["gender"]),
        ("binarias", ohe([["No", "Yes"]] * 4), BINARIAS),
        ("numericas", "passthrough", NUM_CRM),
        ("categorias", ohe(list(CATEGORIAS.values())), list(CATEGORIAS)),
        ("uso", "passthrough", COLUMNAS_USO)], remainder="drop")
    X = entreno[ENTRADA].copy()
    X[NUM_CRM + COLUMNAS_USO] = X[NUM_CRM + COLUMNAS_USO].astype(float)
    modelo = Pipeline([("preparar", prep), ("modelo", XGBClassifier(n_estimators=20, max_depth=3))]).fit(X, y)
    app.state.modelo = modelo
    app.state.meta = {"modelo": "xgb_uso", "umbral": 0.3016, "entrada": ENTRADA}
    app.state.gold = GoldFalso()
    return TestClient(app)          # sin "with": no corre el arranque, no toca GCS


def test_health(cliente):
    r = cliente.get("/health")
    assert r.status_code == 200 and r.json()["modelo"] == "xgb_uso"


def test_predecir_usa_el_lote_mas_reciente(cliente):
    r = cliente.post("/predecir", json=CLIENTE)
    assert r.status_code == 200
    out = r.json()
    assert out["fecha_lote"] == "2026-09-07"
    assert 0 <= out["probabilidad"] <= 1
    assert out["llamar"] == (out["probabilidad"] >= out["umbral"])


def test_cliente_sin_uso_404(cliente):
    r = cliente.post("/predecir", json={**CLIENTE, "customerID": "9999-NADIE"})
    assert r.status_code == 404


@pytest.mark.parametrize("cambio", [
    {"Contract": "1 year"},                 # categoría no permitida (U4)
    {"PaymentMethod": "PSE"},
    {"SeniorCitizen": 0},                   # número, no texto
    {"tenure": -3},
    {"BancoPago": "Bancolombia"},           # columna que no existe
])
def test_entrada_invalida_422(cliente, cambio):
    assert cliente.post("/predecir", json={**CLIENTE, **cambio}).status_code == 422


def test_falta_columna_422(cliente):
    sin_contrato = {k: v for k, v in CLIENTE.items() if k != "Contract"}
    assert cliente.post("/predecir", json=sin_contrato).status_code == 422


def test_lote_ordenado_y_completo(cliente):
    r = cliente.post("/predecir_lote", json={"fecha_lote": "2026-09-07"})
    assert r.status_code == 200
    out = r.json()
    assert out["clientes"] == 31
    p = [c["probabilidad"] for c in out["ranking"]]
    assert p == sorted(p, reverse=True)
    assert out["a_llamar"] == sum(c["llamar"] for c in out["ranking"])


def test_misma_probabilidad_individual_y_lote(cliente):
    individual = cliente.post("/predecir", json=CLIENTE).json()["probabilidad"]
    lote = cliente.post("/predecir_lote", json={"fecha_lote": "2026-09-07"}).json()
    en_lote = next(c for c in lote["ranking"] if c["customerID"] == "5685-IIXLY")["probabilidad"]
    assert individual == en_lote


def test_lote_inexistente_404(cliente):
    assert cliente.post("/predecir_lote", json={"fecha_lote": "2026-09-14"}).status_code == 404


def test_fecha_mal_escrita_422(cliente):
    assert cliente.post("/predecir_lote", json={"fecha_lote": "07/09/2026"}).status_code == 422
