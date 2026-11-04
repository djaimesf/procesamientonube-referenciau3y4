"""Contrato de la API de churn (U6).

Entrada: las 19 columnas del CRM con los MISMOS nombres y listas del gold
(churn/revision.py de la U4). El Pipeline de la U5 hace la preparación por
dentro, así que aquí no se traduce nada: solo se valida.
"""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

SiNo = Literal["No", "Yes"]
SinTelefono = Literal["No", "No phone service", "Yes"]
SinInternet = Literal["No", "No internet service", "Yes"]

# Formato de fecha_lote (en BigQuery es STRING): AAAA-MM-DD
PATRON_FECHA = r"^\d{4}-\d{2}-\d{2}$"


class ClienteInput(BaseModel):
    """Lo que manda el CRM del asesor: 1 cliente, sin el uso."""
    model_config = ConfigDict(extra="forbid")   # columna que no existe -> 422

    customerID: str = Field(..., min_length=1)
    gender: Literal["Female", "Male"]
    SeniorCitizen: Literal["0", "1"]            # en el gold es texto
    Partner: SiNo
    Dependents: SiNo
    tenure: int = Field(..., ge=0)
    PhoneService: SiNo
    MultipleLines: SinTelefono
    InternetService: Literal["DSL", "Fiber optic", "No"]
    OnlineSecurity: SinInternet
    OnlineBackup: SinInternet
    DeviceProtection: SinInternet
    TechSupport: SinInternet
    StreamingTV: SinInternet
    StreamingMovies: SinInternet
    Contract: Literal["Month-to-month", "One year", "Two year"]
    PaperlessBilling: SiNo
    PaymentMethod: Literal["Bank transfer (automatic)", "Credit card (automatic)",
                           "Electronic check", "Mailed check"]
    MonthlyCharges: float = Field(..., gt=0)
    TotalCharges: float = Field(..., ge=0)


class PrediccionOutput(BaseModel):
    customerID: str
    probabilidad: float = Field(..., description="Probabilidad de irse, 0 a 1")
    llamar: bool = Field(..., description="probabilidad >= umbral")
    fecha_lote: str = Field(..., description="Lote del que salió el uso (recencia)")
    umbral: float
    modelo: str
    predicho_en: datetime


class LoteInput(BaseModel):
    fecha_lote: str = Field(..., pattern=PATRON_FECHA, examples=["2026-09-07"])


class ClienteLote(BaseModel):
    customerID: str
    probabilidad: float
    llamar: bool


class LoteOutput(BaseModel):
    fecha_lote: str
    clientes: int
    a_llamar: int
    umbral: float
    modelo: str
    predicho_en: datetime
    ranking: list[ClienteLote]   # de mayor a menor riesgo
