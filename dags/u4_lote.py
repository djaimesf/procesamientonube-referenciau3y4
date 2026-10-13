"""DAG de la Unidad 4 (MVP): revisar los lotes crm -> Spark -> BigQuery.

Disparo manual (sin horario). Una corrida procesa la foto completa de los
lotes 1..6 (hasta el lote del 2026-09-07):

  1. revisar_lotes (Python): baja cada lote del bronze, lo revisa con
     churn.revision.revisar_lote y sube revisado/ y cuarentena/u4_revision/
     al lake. Si un lote viene sin una columna esperada, la tarea falla y
     las siguientes NO corren. Devuelve el id de la corrida (XCom).
  2. lanzar_batch (Bash): jobs/lanzar_batch.sh con los 6 lotes y el dato
     "revisado" -> Spark lee el crm desde revisado/.
  3. cargar_bq (Bash): jobs/cargar_bq.sh -> tablas gold y cuarentena u3_silver.

Las tareas NO reciben variables de entorno propias: heredan las de la
pestana donde corre `airflow standalone` (credenciales de gcloud).
"""

from __future__ import annotations

import datetime
import pathlib
import subprocess
import sys
import tempfile

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

# La carpeta dags/ esta dentro del repo; churn/ vive en la raiz del repo.
REPO = pathlib.Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

BRONZE = "data-telco-bronze-20260926"
LAKE = "u3-class-lake-20261001"
SA = "churn-profe@nube-20262-main.iam.gserviceaccount.com"
GRUPO = "class"
DATASET = "class_gold_20261001"

# Foto completa hasta el lote del 2026-09-07 (lote 6: aparece BancoPago)
LOTES = [
    "2026-08-03",
    "2026-08-10",
    "2026-08-17",
    "2026-08-24",
    "2026-08-31",
    "2026-09-07",
]


def _gcloud(*args: str) -> None:
    """Corre un comando gcloud; si falla, la tarea falla."""
    comando = ["gcloud", *args]
    print("$ " + " ".join(comando))
    subprocess.run(comando, check=True)


with DAG(
    dag_id="u4_class_lote",
    schedule=None,
    start_date=datetime.datetime(2026, 10, 1),
    catchup=False,
    default_args={"retries": 0},
    tags=["u4", "class"],
) as dag:

    @task
    def revisar_lotes() -> str:
        from churn.revision import LoteBloqueado, revisar_lote

        ahora = datetime.datetime.now(datetime.timezone.utc)
        id_ejecucion = "u4-class-sb-" + ahora.strftime("%Y%m%d-%H%M%S")
        print("id de la corrida:", id_ejecucion)

        with tempfile.TemporaryDirectory() as tmp:
            entrada = pathlib.Path(tmp) / "entrada"
            salida = pathlib.Path(tmp) / "salida"
            entrada.mkdir()
            for fecha in LOTES:
                nombre = "clientes_%s.csv" % fecha
                _gcloud("storage", "cp", "gs://%s/crm/%s" % (BRONZE, nombre), str(entrada) + "/")
                jsonl = salida / "cuarentena" / "u4_revision" / ("clientes_%s.jsonl" % fecha)
                try:
                    resumen = revisar_lote(str(entrada / nombre), str(salida), id_ejecucion)
                except LoteBloqueado:
                    # Se guarda el motivo y se borra un revisado viejo de este
                    # lote en el lake, para que Spark nunca lea datos viejos.
                    _gcloud("storage", "cp", str(jsonl), "gs://%s/cuarentena/u4_revision/" % LAKE)
                    subprocess.run(
                        ["gcloud", "storage", "rm", "gs://%s/revisado/crm/%s" % (LAKE, nombre)],
                        check=False,
                    )
                    raise
                print("REVISION |", resumen)
                _gcloud("storage", "cp", str(salida / "revisado" / "crm" / nombre),
                        "gs://%s/revisado/crm/" % LAKE)
                _gcloud("storage", "cp", str(jsonl), "gs://%s/cuarentena/u4_revision/" % LAKE)
        return id_ejecucion

    id_corrida = revisar_lotes()

    lanzar_batch = BashOperator(
        task_id="lanzar_batch",
        cwd=str(REPO),
        bash_command=(
            "bash jobs/lanzar_batch.sh "
            "{{ ti.xcom_pull(task_ids='revisar_lotes') }} "
            f"{LAKE} {SA} {GRUPO} {','.join(LOTES)} revisado"
        ),
    )

    cargar_bq = BashOperator(
        task_id="cargar_bq",
        cwd=str(REPO),
        bash_command=f"bash jobs/cargar_bq.sh {LAKE} {DATASET}",
    )

    id_corrida >> lanzar_batch >> cargar_bq
