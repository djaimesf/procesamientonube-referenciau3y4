"""Revision de un lote semanal del CRM ANTES de Spark (se ejecuta en Airflow).

Corre en .venv-airflow: NO usa pyspark. Las reglas de fila viven en el modelo
pydantic ClienteLote. Cada cliente termina en UN solo lugar: revisado/ (pasa
a Spark) o la cuarentena (con el motivo en la columna "regla").
"""
import csv
import json
import re
import math
import datetime
import pathlib

import pydantic
from pydantic_core import PydanticCustomError

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


# Listas y rangos calculados UNA vez desde base/telco_entrenamiento.csv
# (info churn, 7.043 clientes). Si el historico cambia, se actualizan aqui.

def _texto(v):
    return None if v is None else str(v)


def _a_float(v):
    t = _texto(v)
    if t is None or t.strip() == "":
        return None
    try:
        x = float(t.strip())
    except ValueError:
        return None
    return x if math.isfinite(x) else None


class ClienteLote(pydantic.BaseModel):
    """Una fila del CRM; cada validador lanza el nombre de la regla."""

    model_config = pydantic.ConfigDict(extra="ignore", validate_default=True)

    customerID: object = None
    gender: object = None
    SeniorCitizen: object = None
    Partner: object = None
    Dependents: object = None
    PhoneService: object = None
    PaperlessBilling: object = None
    MultipleLines: object = None
    InternetService: object = None
    OnlineSecurity: object = None
    OnlineBackup: object = None
    DeviceProtection: object = None
    TechSupport: object = None
    StreamingTV: object = None
    StreamingMovies: object = None
    Contract: object = None
    PaymentMethod: object = None
    tenure: object = None
    MonthlyCharges: object = None
    TotalCharges: object = None

    @pydantic.field_validator("customerID", mode="before")
    @classmethod
    def _id(cls, v):
        t = _texto(v)
        if t is None or t.strip() == "":
            raise PydanticCustomError("cliente_sin_id", "sin id")
        return t

    @pydantic.field_validator(*CATEGORIAS_PERMITIDAS, mode="before")
    @classmethod
    def _cat(cls, v, info):
        col = info.field_name
        if v not in CATEGORIAS_PERMITIDAS[col]:
            raise PydanticCustomError(
                "categoria_no_permitida_" + col.lower(), "categoria invalida")
        return v

    @pydantic.field_validator("tenure", mode="before")
    @classmethod
    def _tenure(cls, v):
        x = _a_float(v)
        if x is None:
            raise PydanticCustomError("no_numerico_tenure", "no numerico")
        t = _texto(v).strip()
        if not re.fullmatch(r"\d+", t):
            raise PydanticCustomError("valor_imposible_tenure", "imposible")
        lo, hi = RANGOS["tenure"]
        if not lo <= int(t) <= hi:
            raise PydanticCustomError("fuera_de_rango_tenure", "fuera")
        return t

    @pydantic.field_validator("MonthlyCharges", "TotalCharges", mode="before")
    @classmethod
    def _dinero(cls, v, info):
        col = info.field_name
        low = col.lower()
        x = _a_float(v)
        if x is None:
            raise PydanticCustomError("no_numerico_" + low, "no numerico")
        if x < 0:
            raise PydanticCustomError("valor_imposible_" + low, "imposible")
        lo, hi = RANGOS[col]
        if not lo <= x <= hi:
            raise PydanticCustomError("fuera_de_rango_" + low, "fuera")
        return v


def revisar_encabezados(columnas):
    """Compara encabezados (strip+lower) con COLUMNAS_CRM.

    Devuelve lista de {"regla","severidad","columna"}:
    columna_faltante (bloquea, nombre canonico) y
    columna_nueva (advierte, texto original del encabezado).
    """
    canon = {c.lower(): c for c in COLUMNAS_CRM}
    vistos = {c.strip().lower() for c in columnas}
    res = []
    for c in COLUMNAS_CRM:
        if c.lower() not in vistos:
            res.append({"regla": "columna_faltante", "severidad": "bloquea",
                        "columna": c})
    for c in columnas:
        if c.strip().lower() not in canon:
            res.append({"regla": "columna_nueva", "severidad": "advierte",
                        "columna": c})
    return res


def reglas_de_fila(fila):
    """Valida una fila (valores texto) con ClienteLote.

    Devuelve la lista ordenada de reglas incumplidas; [] si es valida.
    Pydantic reporta TODAS las columnas que fallan, no solo la primera.
    """
    try:
        ClienteLote.model_validate(fila)
    except pydantic.ValidationError as e:
        return sorted({err["type"] for err in e.errors()})
    return []


def revisar_filas(filas, fecha_archivo, archivo_origen, id_ejecucion):
    """Separa filas en (buenas, cuarentena).

    Quita duplicados identicos (deja el primero), marca ids repetidos con
    datos distintos, fecha_lote distinta a la del archivo y reglas de fila.
    Una fila con N reglas genera N registros de cuarentena (8 claves).
    """
    # Una sola hora para todos los registros de esta revision.
    ahora = datetime.datetime.now(datetime.timezone.utc).isoformat(sep=" ")
    # 1) Duplicados identicos: se deja la primera aparicion.
    vistas = set()
    unicas = []
    for f in filas:
        k = json.dumps(f, sort_keys=True, ensure_ascii=False)
        if k not in vistas:
            vistas.add(k)
            unicas.append(f)

    def idc(f):
        t = f.get("customerID")
        t = None if t is None else str(t).strip()
        return t or None

    # 2) Mismo customerID con datos distintos: todas esas filas a cuarentena.
    cuenta = {}
    for f in unicas:
        i = idc(f)
        if i is not None:
            cuenta[i] = cuenta.get(i, 0) + 1

    buenas, cuarentena = [], []
    for f in unicas:
        reglas = []
        i = idc(f)
        if i is not None and cuenta[i] > 1:
            reglas.append("id_duplicado_conflictivo")
        if (f.get("fecha_lote") or "").strip() != fecha_archivo:
            reglas.append("fecha_lote_no_coincide")
        reglas += reglas_de_fila(f)
        if not reglas:
            buenas.append(f)
            continue
        for r in reglas:
            cuarentena.append({
                "fecha_error": ahora,
                "unidad_etapa": UNIDAD_ETAPA,
                "regla": r,
                "severidad": "bloquea",
                "archivo_origen": archivo_origen,
                "customerID": i,
                "id_ejecucion": id_ejecucion,
                "registro_original": json.dumps(f, ensure_ascii=False),
            })
    return buenas, cuarentena


def revisar_lote(ruta_csv, carpeta_salida, id_ejecucion):
    """Revisa un CSV clientes_AAAA-MM-DD.csv y escribe sus salidas.

    Escribe <salida>/cuarentena/u4_revision/clientes_<fecha>.jsonl (siempre)
    y <salida>/revisado/crm/clientes_<fecha>.csv (solo si no faltan columnas).
    Lanza LoteBloqueado si falta alguna columna; ValueError si el nombre
    del archivo no es valido. Devuelve {"fecha","filas_leidas","buenas",
    "cuarentena","avisos"}.
    """
    ruta = pathlib.Path(ruta_csv)
    m = re.fullmatch(r"clientes_(\d{4}-\d{2}-\d{2})\.csv", ruta.name)
    if not m:
        raise ValueError("nombre de archivo invalido: " + ruta.name)
    fecha = m.group(1)
    with open(ruta, encoding="utf-8", newline="") as fh:
        lector = csv.reader(fh)
        original = next(lector, [])
        canon = {c.lower(): c for c in COLUMNAS_CRM}
        # Columnas conocidas -> nombre canonico (como leer_csv de la U3);
        # columnas nuevas (BancoPago) conservan su nombre.
        enc = [canon.get(c.strip().lower(), c) for c in original]
        filas = [dict(zip(enc, r)) for r in lector if r]
    ahora = datetime.datetime.now(datetime.timezone.utc).isoformat(sep=" ")
    avisos = revisar_encabezados(original)
    reg_archivo = [{
        "fecha_error": ahora, "unidad_etapa": UNIDAD_ETAPA,
        "regla": a["regla"], "severidad": a["severidad"],
        "archivo_origen": str(ruta_csv), "customerID": None,
        "id_ejecucion": id_ejecucion,
        "registro_original": json.dumps({"encabezados": original}),
    } for a in avisos]
    faltantes = [a["columna"] for a in avisos if a["regla"] == "columna_faltante"]
    base = pathlib.Path(carpeta_salida)
    dir_q = base / "cuarentena" / UNIDAD_ETAPA
    dir_q.mkdir(parents=True, exist_ok=True)
    ruta_q = dir_q / ("clientes_%s.jsonl" % fecha)
    dir_r = base / "revisado" / "crm"
    ruta_r = dir_r / ("clientes_%s.csv" % fecha)
    if faltantes:
        # Lote bloqueado: se borra un revisado viejo de este mismo lote para
        # que Spark nunca lea datos de una corrida anterior.
        ruta_r.unlink(missing_ok=True)
        with open(ruta_q, "w", encoding="utf-8") as fh:
            for r in reg_archivo:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        raise LoteBloqueado("faltan columnas: " + ", ".join(faltantes))
    buenas, cuarentena = revisar_filas(filas, fecha, str(ruta_csv), id_ejecucion)
    with open(ruta_q, "w", encoding="utf-8") as fh:
        for r in reg_archivo + cuarentena:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    dir_r.mkdir(parents=True, exist_ok=True)
    with open(ruta_r, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=enc)
        w.writeheader()
        w.writerows(buenas)
    return {"fecha": fecha, "filas_leidas": len(filas), "buenas": len(buenas),
            "cuarentena": len(cuarentena),
            "avisos": sum(1 for a in avisos if a["regla"] == "columna_nueva")}
