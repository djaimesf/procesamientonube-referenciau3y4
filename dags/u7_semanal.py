"""DAG de la Unidad 7: cada lunes, sin nadie.

Es el DAG de la U4 (revisar -> Spark -> BigQuery) con 2 cambios y 3 tareas nuevas.

Cambios:
  - Horario: cada lunes a las 6 a. m. (hora de Colombia). En nuestra historia
    los lunes de la U7 son el 14, el 21 y el 28 de septiembre: start_date y
    end_date. catchup=True: al encender el DAG, Airflow corre esos 3 lunes,
    en orden y de a 1 (max_active_runs=1).
  - Los lotes salen de la fecha de la corrida (lotes_hasta), no de una lista
    escrita en el codigo. Los valores del grupo salen de Variables de
    Airflow, no del codigo: el mismo archivo sirve para cualquier grupo.

Tareas nuevas:
  4. cuarentena_bq (Bash): la cuarentena de la revision sube a BigQuery
     (unidad_etapa = 'u4_revision').
  5. campana (Python): version campeona del Model Registry -> /predecir_lote
     de la API de la U6 -> tabla campana en BigQuery.
  6. alerta_cuarentena (Python): compara la tasa del lote de hoy con la
     historia (Q3 + 1,5 x IQR). Si la supera, la tarea falla: la campana ya
     salio, pero el DAG queda en rojo para que alguien mire.

Variables de Airflow: lake, sa, grupo, dataset, url_api, modelo_registro.
Las tareas heredan las credenciales de gcloud de la pestana donde corre
`airflow standalone`, igual que en la U4.
"""

from __future__ import annotations

import datetime
import json
import pathlib
import subprocess
import sys
import tempfile
import urllib.request

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Variable, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.timetables.trigger import CronTriggerTimetable

# La carpeta dags/ esta dentro del repo; churn/ vive en la raiz del repo.
REPO = pathlib.Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

BRONZE = "data-telco-bronze-20260926"
REGION = "us-central1"
ZONA = "America/Bogota"

LOTES = "{{ ti.xcom_pull(task_ids='revisar_lotes')['lotes'] | join(',') }}"
ID_CORRIDA = "{{ ti.xcom_pull(task_ids='revisar_lotes')['id_ejecucion'] }}"


def _correr(*comando: str, capturar: bool = False) -> str:
    """Corre un comando (gcloud o bq); si falla, la tarea falla."""
    print("$ " + " ".join(comando))
    r = subprocess.run(comando, check=True, text=True,
                       stdout=subprocess.PIPE if capturar else None)
    return r.stdout if capturar else ""


with DAG(
    dag_id="u7_class_semanal",
    schedule=CronTriggerTimetable("0 6 * * 1", timezone=ZONA),
    start_date=pendulum.datetime(2026, 9, 14, tz=ZONA),
    end_date=pendulum.datetime(2026, 9, 28, 23, 59, tz=ZONA),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["u7", "class"],
) as dag:

    @task
    def revisar_lotes(**context) -> dict:
        from churn.revision import LoteBloqueado, revisar_lote
        from churn.semana import lotes_hasta

        lake = Variable.get("lake")
        lotes = lotes_hasta(context["ds"])
        ahora = datetime.datetime.now(datetime.timezone.utc)
        id_ejecucion = "u7-%s-sb-%s" % (Variable.get("grupo"), ahora.strftime("%Y%m%d-%H%M%S"))
        print("lunes:", context["ds"], "| lotes:", len(lotes), "| id de la corrida:", id_ejecucion)

        resumenes = []
        with tempfile.TemporaryDirectory() as tmp:
            entrada = pathlib.Path(tmp) / "entrada"
            salida = pathlib.Path(tmp) / "salida"
            entrada.mkdir()
            for fecha in lotes:
                nombre = "clientes_%s.csv" % fecha
                _correr("gcloud", "storage", "cp", "gs://%s/crm/%s" % (BRONZE, nombre), str(entrada) + "/")
                jsonl = salida / "cuarentena" / "u4_revision" / ("clientes_%s.jsonl" % fecha)
                try:
                    resumen = revisar_lote(str(entrada / nombre), str(salida), id_ejecucion)
                except LoteBloqueado:
                    _correr("gcloud", "storage", "cp", str(jsonl), "gs://%s/cuarentena/u4_revision/" % lake)
                    subprocess.run(["gcloud", "storage", "rm", "gs://%s/revisado/crm/%s" % (lake, nombre)],
                                   check=False)
                    raise
                print("REVISION |", resumen)
                resumenes.append(resumen)
                _correr("gcloud", "storage", "cp", str(salida / "revisado" / "crm" / nombre),
                        "gs://%s/revisado/crm/" % lake)
                _correr("gcloud", "storage", "cp", str(jsonl), "gs://%s/cuarentena/u4_revision/" % lake)
        return {"id_ejecucion": id_ejecucion, "lotes": lotes, "resumenes": resumenes}

    revision = revisar_lotes()

    lanzar_batch = BashOperator(
        task_id="lanzar_batch",
        cwd=str(REPO),
        bash_command=(
            f"bash jobs/lanzar_batch.sh {ID_CORRIDA} "
            "{{ var.value.lake }} {{ var.value.sa }} {{ var.value.grupo }} "
            f"{LOTES} revisado"
        ),
    )

    cargar_bq = BashOperator(
        task_id="cargar_bq",
        cwd=str(REPO),
        bash_command="bash jobs/cargar_bq.sh {{ var.value.lake }} {{ var.value.dataset }}",
    )

    cuarentena_bq = BashOperator(
        task_id="cuarentena_bq",
        cwd=str(REPO),
        bash_command="bash jobs/cargar_cuarentena.sh {{ var.value.lake }} {{ var.value.dataset }} " + LOTES,
    )

    @task
    def campana(**context) -> dict:
        from churn.semana import filas_campana

        ds = context["ds"]
        rev = context["ti"].xcom_pull(task_ids="revisar_lotes")
        dataset = Variable.get("dataset")
        modelo = Variable.get("modelo_registro")

        # 1. Que version del modelo es la campeona, segun el Model Registry
        versiones = json.loads(_correr("gcloud", "ai", "models", "list-version", modelo,
                                       "--region=" + REGION, "--format=json", capturar=True))
        campeon = [v for v in versiones if "campeon" in v.get("versionAliases", [])]
        if len(campeon) != 1:
            raise AirflowFailException("El modelo %s no tiene una version con alias campeon" % modelo)
        version = str(campeon[0]["versionId"])
        print("MODELO |", modelo, "| version", version, "|", campeon[0].get("artifactUri"))

        # 2. La lista del lunes: la misma llamada que en la U6 se hizo a mano
        token = _correr("gcloud", "auth", "print-identity-token", capturar=True).strip()
        pedido = urllib.request.Request(
            Variable.get("url_api").rstrip("/") + "/predecir_lote",
            data=json.dumps({"fecha_lote": ds}).encode(),
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(pedido, timeout=300) as r:
            respuesta = json.load(r)
        print("CAMPANA |", ds, "| clientes", respuesta["clientes"], "| a llamar", respuesta["a_llamar"])

        # 3. A BigQuery: se reemplaza la lista de ese lunes (idempotente)
        filas = filas_campana(respuesta, rev["id_ejecucion"], version)
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
            for f in filas:
                fh.write(json.dumps(f) + "\n")
        q = ["bq", "query", "--location=" + REGION, "--use_legacy_sql=false"]
        _correr(*q, f"""CREATE TABLE IF NOT EXISTS {dataset}.campana (
  fecha_lote STRING NOT NULL, posicion INT64 NOT NULL, customerID STRING NOT NULL,
  probabilidad FLOAT64, llamar BOOL, umbral FLOAT64, modelo STRING,
  version_registro STRING, id_ejecucion STRING, predicho_en TIMESTAMP)
OPTIONS (description = 'Lista de la campana de Retencion: 1 fila por cliente y lunes (U7)')""")
        _correr("bq", "load", "--location=" + REGION, "--source_format=NEWLINE_DELIMITED_JSON",
                "--replace", f"{dataset}.stg_campana", fh.name,
                "fecha_lote:STRING,posicion:INTEGER,customerID:STRING,probabilidad:FLOAT,"
                "llamar:BOOLEAN,umbral:FLOAT,modelo:STRING,version_registro:STRING,"
                "id_ejecucion:STRING,predicho_en:TIMESTAMP")
        _correr(*q, f"""BEGIN TRANSACTION;
DELETE FROM {dataset}.campana WHERE fecha_lote = '{ds}';
INSERT INTO {dataset}.campana SELECT * FROM {dataset}.stg_campana;
COMMIT TRANSACTION;""")
        _correr("bq", "rm", "-f", "-t", f"{dataset}.stg_campana")
        pathlib.Path(fh.name).unlink()
        return {"clientes": respuesta["clientes"], "a_llamar": respuesta["a_llamar"],
                "version_registro": version}

    @task
    def alerta_cuarentena(**context) -> dict:
        from churn.semana import tasa_cuarentena, umbral_alerta

        rev = context["ti"].xcom_pull(task_ids="revisar_lotes")
        tasas = [tasa_cuarentena(r) for r in rev["resumenes"]]
        for r, t in zip(rev["resumenes"], tasas):
            print("TASA | %s | %4d leidas | %4d buenas | %5.1f %%" % (
                r["fecha"], r["filas_leidas"], r["buenas"], 100 * t))
        hoy, umbral = tasas[-1], umbral_alerta(tasas[:-1])
        if umbral is None:
            print("Sin historia suficiente para un umbral: no hay alerta.")
        else:
            print("HOY %.1f %% | UMBRAL (Q3 + 1,5 x IQR de los lotes anteriores) %.2f %%" % (
                100 * hoy, 100 * umbral))
            if hoy > umbral:
                raise AirflowFailException(
                    "ALERTA: la cuarentena del lote %s (%.1f %%) supera el umbral (%.2f %%). "
                    "La campana se genero, pero sin esos clientes. Revisar la fuente."
                    % (context["ds"], 100 * hoy, 100 * umbral))
        return {"tasa": hoy, "umbral": umbral}

    revision >> lanzar_batch >> cargar_bq >> [cuarentena_bq, campana()] >> alerta_cuarentena()
