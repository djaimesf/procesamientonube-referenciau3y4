"""Revision de un lote semanal del CRM ANTES de Spark (se ejecuta en Airflow).

STUB de la fase roja de TDD: cada funcion lanza NotImplementedError.
Solo se escribe el codigo necesario para poner los tests en verde.
"""
import csv
import json
import re
import math
import datetime
import pathlib

import pydantic

from churn.columnas import COLUMNAS_CRM

UNIDAD_ETAPA = "u4_revision"

CATEGORIAS_PERMITIDAS = {
    "gender": ["Female", "Male"],
    "SeniorCitizen": ["0", "1"],
    "Partner": ["No", "Yes"],
    "Dependents": ["No", "Yes"],
    "PhoneService": ["No", "Yes"],
    "PaperlessBilling": ["No", "Yes"],
    "MultipleLines": ["No", "No phone service", "Yes"],
    "InternetService": ["DSL", "Fiber optic", "No"],
    "OnlineSecurity": ["No", "No internet service", "Yes"],
    "OnlineBackup": ["No", "No internet service", "Yes"],
    "DeviceProtection": ["No", "No internet service", "Yes"],
    "TechSupport": ["No", "No internet service", "Yes"],
    "StreamingTV": ["No", "No internet service", "Yes"],
    "StreamingMovies": ["No", "No internet service", "Yes"],
    "Contract": ["Month-to-month", "One year", "Two year"],
    "PaymentMethod": [
        "Bank transfer (automatic)",
        "Credit card (automatic)",
        "Electronic check",
        "Mailed check",
    ],
}

RANGOS = {
    "tenure": (0, 72),
    "MonthlyCharges": (18.25, 118.75),
    "TotalCharges": (18.8, 8684.8),
}


class LoteBloqueado(Exception):
    """El lote no se puede procesar (faltan columnas obligatorias)."""


class ClienteLote(pydantic.BaseModel):
    """Modelo de una fila del lote. Por ahora sin campos ni validadores."""


def revisar_encabezados(columnas):
    """Compara encabezados (strip+lower) con COLUMNAS_CRM.

    Devuelve lista de {"regla","severidad","columna"}:
    columna_faltante (bloquea, nombre canonico) y
    columna_nueva (advierte, texto original del encabezado).
    """
    raise NotImplementedError("Paso: TDD verde")


def reglas_de_fila(fila):
    """Valida una fila (valores texto) con ClienteLote.

    Devuelve la lista ordenada de reglas incumplidas; [] si es valida.
    Reporta todas las columnas que fallan, no solo la primera.
    """
    raise NotImplementedError("Paso: TDD verde")


def revisar_filas(filas, fecha_archivo, archivo_origen, id_ejecucion):
    """Separa filas en (buenas, cuarentena).

    Quita duplicados identicos (deja el primero), marca ids repetidos con
    datos distintos, fecha_lote distinta a la del archivo y reglas de fila.
    Una fila con N reglas genera N registros de cuarentena (8 claves).
    """
    raise NotImplementedError("Paso: TDD verde")


def revisar_lote(ruta_csv, carpeta_salida, id_ejecucion):
    """Revisa un CSV clientes_AAAA-MM-DD.csv y escribe sus salidas.

    Escribe <salida>/cuarentena/u4_revision/clientes_<fecha>.jsonl (siempre)
    y <salida>/revisado/crm/clientes_<fecha>.csv (solo si no faltan columnas).
    Lanza LoteBloqueado si falta alguna columna; ValueError si el nombre
    del archivo no es valido. Devuelve {"fecha","filas_leidas","buenas",
    "cuarentena","avisos"}.
    """
    raise NotImplementedError("Paso: TDD verde")
