"""API de churn (U6): el riesgo le llega al asesor (/predecir) y a la campaña (/predecir_lote).

Configuración por variables de entorno (se fijan en `gcloud run deploy`):
  MODELO_GCS  carpeta con model.joblib y metadata.json (salida de la U5)
  TABLA_GOLD  proyecto.dataset.perfil_lotes
"""
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from google.cloud import storage

from app.gold import COLUMNAS_USO, Gold
from app.schemas import (ClienteInput, ClienteLote, LoteInput, LoteOutput,
                         PrediccionOutput)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("churn-api")

MODELO_GCS = os.environ.get("MODELO_GCS", "gs://u3-class-lake-20261001/artefactos/u5")
TABLA_GOLD = os.environ.get("TABLA_GOLD", "nube-20262-main.class_gold_20261001.perfil_lotes")
NUMERICAS = ["tenure", "MonthlyCharges", "TotalCharges"] + COLUMNAS_USO


def descargar(carpeta_gcs: str, destino: str = "/tmp") -> tuple[str, str]:
    bucket, prefijo = carpeta_gcs.removeprefix("gs://").split("/", 1)
    b = storage.Client().bucket(bucket)
    rutas = []
    for nombre in ("model.joblib", "metadata.json"):
        local = os.path.join(destino, nombre)
        b.blob(f"{prefijo.rstrip('/')}/{nombre}").download_to_filename(local)
        rutas.append(local)
    return rutas[0], rutas[1]


@asynccontextmanager
async def ciclo_de_vida(app: FastAPI):
    """Carga el modelo UNA vez al arrancar, no en cada petición."""
    log.info("Descargando modelo de %s", MODELO_GCS)
    ruta_modelo, ruta_meta = descargar(MODELO_GCS)
    app.state.modelo = joblib.load(ruta_modelo)
    app.state.meta = json.load(open(ruta_meta))
    app.state.gold = Gold(TABLA_GOLD)
    log.info("Modelo %s listo | umbral %s", app.state.meta["modelo"], app.state.meta["umbral"])
    yield


app = FastAPI(title="Churn API", lifespan=ciclo_de_vida)


def puntuar(df: pd.DataFrame):
    """Mismas 25 columnas y mismos tipos que en el entrenamiento."""
    meta = app.state.meta
    X = df[meta["entrada"]].copy()
    X[NUMERICAS] = X[NUMERICAS].astype(float)     # Int64 de BigQuery -> float, igual que en U5
    return app.state.modelo.predict_proba(X)[:, 1]


@app.get("/health")
def health():
    if getattr(app.state, "modelo", None) is None:
        raise HTTPException(503, "Modelo no cargado")
    return {"estado": "ok", "modelo": app.state.meta["modelo"], "umbral": app.state.meta["umbral"]}


@app.post("/predecir", response_model=PrediccionOutput)
def predecir(cliente: ClienteInput):
    uso = app.state.gold.uso_vigente(cliente.customerID)
    if uso is None:
        raise HTTPException(404, f"{cliente.customerID} no tiene uso en el gold")
    fila = pd.DataFrame([{**cliente.model_dump(), **uso}])
    p = float(puntuar(fila)[0])
    umbral = app.state.meta["umbral"]
    log.info("predecir id=%s lote=%s p=%.4f", cliente.customerID, uso["fecha_lote"], p)
    return PrediccionOutput(customerID=cliente.customerID, probabilidad=round(p, 4),
                            llamar=p >= umbral, fecha_lote=uso["fecha_lote"], umbral=umbral,
                            modelo=app.state.meta["modelo"],
                            predicho_en=datetime.now(timezone.utc))


@app.post("/predecir_lote", response_model=LoteOutput)
def predecir_lote(pedido: LoteInput):
    df = app.state.gold.lote(pedido.fecha_lote)
    if df.empty:
        raise HTTPException(404, f"No hay clientes del lote {pedido.fecha_lote} en el gold")
    df["probabilidad"] = puntuar(df)
    umbral = app.state.meta["umbral"]
    df = df.sort_values("probabilidad", ascending=False)
    ranking = [ClienteLote(customerID=r.customerID, probabilidad=round(float(r.probabilidad), 4),
                           llamar=bool(r.probabilidad >= umbral))
               for r in df.itertuples()]
    a_llamar = sum(c.llamar for c in ranking)
    log.info("predecir_lote lote=%s clientes=%d a_llamar=%d", pedido.fecha_lote, len(df), a_llamar)
    return LoteOutput(fecha_lote=pedido.fecha_lote, clientes=len(df), a_llamar=a_llamar,
                      umbral=umbral, modelo=app.state.meta["modelo"],
                      predicho_en=datetime.now(timezone.utc), ranking=ranking)
