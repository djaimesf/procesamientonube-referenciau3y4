"""La semana del DAG de la U7, sin GCP: funciones puras que se prueban con pytest.

- lotes_hasta: que lotes del CRM procesa la corrida de un lunes.
- tasa_cuarentena: que fraccion de un lote no paso la revision.
- umbral_alerta: desde que tasa la cuarentena es anormal, segun la historia.
- filas_campana: la respuesta de /predecir_lote convertida en filas de BigQuery.
"""
import datetime
import statistics

PRIMER_LOTE = "2026-08-03"  # lunes del primer lote del CRM (U3)


def lotes_hasta(fecha, primero=PRIMER_LOTE):
    """Lunes desde el primer lote hasta `fecha` (incluida), en texto AAAA-MM-DD.

    El DAG reprocesa la foto completa, como en la U4: cada lunes revisa y
    procesa todos los lotes que ya existen, nunca uno del futuro.
    """
    ini = datetime.date.fromisoformat(primero)
    fin = datetime.date.fromisoformat(fecha)
    if fin.weekday() != 0:
        raise ValueError("la fecha del lote debe ser lunes: " + fecha)
    if fin < ini:
        raise ValueError("no hay lotes antes de " + primero)
    semanas = (fin - ini).days // 7
    return [(ini + datetime.timedelta(days=7 * k)).isoformat() for k in range(semanas + 1)]


def tasa_cuarentena(resumen):
    """Fraccion de filas leidas que no llego a Spark (cuarentena o duplicado exacto)."""
    if resumen["filas_leidas"] == 0:
        return 0.0
    return 1 - resumen["buenas"] / resumen["filas_leidas"]


def umbral_alerta(tasas_previas):
    """Q3 + 1,5 x IQR de las tasas de los lotes anteriores (regla de los valores atipicos).

    Se calibra con la propia historia, no con un numero a dedo. Con menos de
    4 lotes no hay historia suficiente: devuelve None y la alerta no decide.
    """
    if len(tasas_previas) < 4:
        return None
    q1, _, q3 = statistics.quantiles(tasas_previas, n=4, method="inclusive")
    return q3 + 1.5 * (q3 - q1)


def _timestamp_bq(texto):
    """'2026-11-05T11:00:00.123456Z' (JSON de la API) -> '2026-11-05 11:00:00.123456+00:00'."""
    t = datetime.datetime.fromisoformat(texto.replace("Z", "+00:00"))
    return t.astimezone(datetime.timezone.utc).isoformat(sep=" ")


def filas_campana(respuesta, id_ejecucion, version_registro):
    """Una fila por cliente del ranking, con el rastro de quien y con que se calculo."""
    predicho_en = _timestamp_bq(respuesta["predicho_en"])
    return [
        {
            "fecha_lote": respuesta["fecha_lote"],
            "posicion": i,
            "customerID": c["customerID"],
            "probabilidad": c["probabilidad"],
            "llamar": c["llamar"],
            "umbral": respuesta["umbral"],
            "modelo": respuesta["modelo"],
            "version_registro": version_registro,
            "id_ejecucion": id_ejecucion,
            "predicho_en": predicho_en,
        }
        for i, c in enumerate(respuesta["ranking"], start=1)
    ]
