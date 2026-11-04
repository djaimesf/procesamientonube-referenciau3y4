"""Lectura del gold en BigQuery (perfil_lotes).

Consultas con parámetros (@id, @fecha): el valor nunca se pega dentro del SQL.
"""
import pandas as pd
from google.cloud import bigquery

COLUMNAS_USO = ["n_llamadas", "min_llamadas", "mb_total",
                "n_tickets", "n_llamadas_caidas", "tasa_caida"]


class Gold:
    def __init__(self, tabla: str, cliente: bigquery.Client | None = None):
        self.tabla = tabla                       # proyecto.dataset.perfil_lotes
        self.bq = cliente or bigquery.Client()

    def uso_vigente(self, customer_id: str) -> dict | None:
        """Uso del lote MÁS RECIENTE del cliente. None si no está en el gold."""
        sql = f"""
            SELECT fecha_lote, {", ".join(COLUMNAS_USO)}
            FROM `{self.tabla}`
            WHERE customerID = @id
            QUALIFY ROW_NUMBER() OVER (PARTITION BY customerID ORDER BY fecha_lote DESC) = 1
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", customer_id)])
        filas = list(self.bq.query(sql, job_config=cfg).result())
        return dict(filas[0]) if filas else None

    def lote(self, fecha_lote: str) -> pd.DataFrame:
        """Todos los clientes de UN lote, con CRM y uso, en 1 sola consulta."""
        sql = f"""
            SELECT * EXCEPT (_archivo_origen, _fecha_ingesta, _id_ejecucion, fecha_foto)
            FROM `{self.tabla}`
            WHERE fecha_lote = @fecha
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("fecha", "STRING", fecha_lote)])
        return self.bq.query(sql, job_config=cfg).to_dataframe()
