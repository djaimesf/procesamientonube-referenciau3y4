#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# cargar_cuarentena.sh  (U7)
#
# Que hace:
#   Sube a BigQuery la cuarentena de la REVISION del lote (U4), que hasta
#   ahora solo quedaba en el lake: gs://LAKE/cuarentena/u4_revision/*.jsonl.
#   Va a la misma tabla cuarentena de la U3, con unidad_etapa = 'u4_revision'.
#
# Uso:
#   bash jobs/cargar_cuarentena.sh LAKE DATASET LOTES
#
#   LAKE     nombre del bucket lake del grupo, SIN gs://
#   DATASET  dataset de BigQuery del grupo
#   LOTES    fechas AAAA-MM-DD separadas por coma, sin espacios
#
# Idempotencia:
#   archivo_origen se guarda como la ruta del lote en el bronze (no la carpeta
#   temporal donde lo reviso Airflow). Antes de insertar se borran las filas
#   'u4_revision' de esos mismos archivos: correrlo 2 veces da lo mismo que 1,
#   y cada lote queda con su ultima revision.
#
# Permisos: identidad del usuario de Cloud Shell, igual que cargar_bq.sh.
# ---------------------------------------------------------------------------

LAKE="${1:?Falta el bucket lake (sin gs://)}"
DATASET="${2:?Falta el dataset de BigQuery}"
LOTES="${3:?Falta la lista de lotes AAAA-MM-DD separados por coma}"

LOCATION="us-central1"
BRONZE="data-telco-bronze-20260926"

if [[ "${LAKE}" == gs://* ]]; then
  echo "Error: LAKE no debe incluir el prefijo gs://." >&2
  exit 1
fi
if ! [[ "${DATASET}" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "Error: DATASET invalido '${DATASET}'." >&2
  exit 1
fi
if ! [[ "${LOTES}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}(,[0-9]{4}-[0-9]{2}-[0-9]{2})*$ ]]; then
  echo "Error: LOTES debe ser AAAA-MM-DD separados por coma, sin espacios." >&2
  exit 1
fi

URIS=""
for f in ${LOTES//,/ }; do
  URIS+="gs://${LAKE}/cuarentena/u4_revision/clientes_${f}.jsonl,"
done
URIS="${URIS%,}"

STAGING_CREADA=0
limpiar_staging() {
  if [[ "${STAGING_CREADA}" -eq 1 ]]; then
    bq rm -f -t "${DATASET}.stg_cuarentena_revision" || true
  fi
}
trap limpiar_staging ERR

echo "[1/4] Cargando staging ${DATASET}.stg_cuarentena_revision ..."
bq load --location="${LOCATION}" --source_format=NEWLINE_DELIMITED_JSON --replace \
  "${DATASET}.stg_cuarentena_revision" "${URIS}" \
  fecha_error:STRING,unidad_etapa:STRING,regla:STRING,severidad:STRING,archivo_origen:STRING,customerID:STRING,id_ejecucion:STRING,registro_original:STRING
STAGING_CREADA=1

echo "[2/4] Mezclando en ${DATASET}.cuarentena (delete + insert, transaccional) ..."
bq query --location="${LOCATION}" --use_legacy_sql=false \
"BEGIN TRANSACTION;
DELETE FROM ${DATASET}.cuarentena
WHERE unidad_etapa = 'u4_revision'
  AND archivo_origen IN (
    SELECT DISTINCT CONCAT('gs://${BRONZE}/crm/', REGEXP_EXTRACT(archivo_origen, r'clientes_[0-9]{4}-[0-9]{2}-[0-9]{2}[.]csv'))
    FROM ${DATASET}.stg_cuarentena_revision);
INSERT INTO ${DATASET}.cuarentena
SELECT TIMESTAMP(fecha_error), unidad_etapa, regla, severidad,
       CONCAT('gs://${BRONZE}/crm/', REGEXP_EXTRACT(archivo_origen, r'clientes_[0-9]{4}-[0-9]{2}-[0-9]{2}[.]csv')),
       customerID, id_ejecucion, PARSE_JSON(registro_original)
FROM ${DATASET}.stg_cuarentena_revision;
COMMIT TRANSACTION;"

echo "[3/4] Eliminando staging ..."
bq rm -f -t "${DATASET}.stg_cuarentena_revision"
STAGING_CREADA=0

echo "[4/4] Cuarentena por etapa:"
bq query --location="${LOCATION}" --use_legacy_sql=false \
"SELECT unidad_etapa, COUNT(*) AS filas FROM ${DATASET}.cuarentena GROUP BY unidad_etapa ORDER BY unidad_etapa"

echo "Listo."
