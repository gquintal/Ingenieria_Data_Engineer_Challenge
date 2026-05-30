"""
DAG: etl_engineer_challenge
Orquesta el ETL de transacciones:
  1. Lee CSV desde Minio (bck-landing)
  2. Limpia y transforma con Polars
  3. Guarda Parquet en Minio (bck-bronze)
  4. Registra tabla en Trino

Configuración:
  - Credenciales de Minio → Airflow Connection: minio_conn
  - Credenciales de Trino → Airflow Connection: trino_conn
  - Parámetros de negocio → variables de entorno (.env)
"""

import os
from datetime import datetime

from airflow.sdk import dag, task

from hooks.minio_hook import MinioHook
from hooks.trino_hook import TrinoHook
from transactions import build_transactions_aggregation


PIPELINE_CONFIG = {
    "source_key": "data/data_prueba_tecnica.csv",
    "dest_key": "master/data_prueba_tecnica.parquet",
    "schema_bronze": "prueba",
    "table_bronze": "tbl_data",
}


@dag(
    dag_id="etl_engineer_challenge",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "challenge"],
)
def etl_engineer_challenge():

    @task()
    def read_data() -> str:
        """Valida la ubicación del CSV de entrada y retorna su key."""
        hook = MinioHook()
        hook.check_for_key(
            bucket=os.environ["BUCKET_LANDING"],
            key=PIPELINE_CONFIG["source_key"],
        )
        return PIPELINE_CONFIG["source_key"]

    @task()
    def transform(source_key: str) -> str:
        """Limpia, agrega y guarda el resultado en parquet. Retorna la key destino."""
        hook = MinioHook()
        raw_bytes = hook.read_bytes(
            bucket=os.environ["BUCKET_LANDING"],
            key=source_key,
        )
        parquet_bytes = build_transactions_aggregation(raw_bytes)
        hook.write_bytes(
            bucket=os.environ["BUCKET_BRONZE"],
            key=PIPELINE_CONFIG["dest_key"],
            data=parquet_bytes,
        )
        return PIPELINE_CONFIG["dest_key"]

    @task()
    def load_data(dest_key: str) -> None:
        """Crea schema y tabla externa en Trino apuntando al Parquet."""
        hook = TrinoHook(catalog=os.environ["TRINO_CATALOG"])
        hook.register_table(
            schema=PIPELINE_CONFIG["schema_bronze"],
            table=PIPELINE_CONFIG["table_bronze"],
            bucket=os.environ["BUCKET_BRONZE"],
            path=dest_key.rsplit("/", 1)[0] + "/",
        )

    # Flujo del ETL
    source_key = read_data()
    dest_key = transform(source_key)
    load_data(dest_key)


etl_engineer_challenge()
