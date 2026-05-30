"""
TrinoHook
Encapsula la conexión y operaciones contra Trino usando la Connection de Airflow.
"""

import logging

from trino.dbapi import connect as trino_connect

from airflow.sdk.bases.hook import BaseHook

logger = logging.getLogger(__name__)


class TrinoHook(BaseHook):

    def __init__(self, catalog: str, conn_id: str = "trino_conn"):
        super().__init__()
        self.conn_id = conn_id
        self._catalog = catalog
        conn = self.get_connection(self.conn_id)
        self._conn = trino_connect(
            host=conn.host,
            port=int(conn.port),
            user=conn.login,
            catalog=self._catalog,
        )

    def execute(self, sql: str) -> None:
        """Ejecuta un statement SQL en Trino."""
        cur = self._conn.cursor()
        cur.execute(sql)
        cur.close()
        logger.info("[TrinoHook] Ejecutado: %s...", sql.strip()[:80])

    def close(self) -> None:
        self._conn.close()

    def register_table(self, schema: str, table: str, bucket: str, path: str) -> None:
        """
        Crea el schema y la tabla externa en Trino apuntando
        al Parquet almacenado en Minio.
        """
        self.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {self._catalog}.{schema}
            WITH (location = 's3a://{bucket}/')
            """
        )
        logger.info("[TrinoHook] Schema '%s' listo.", schema)

        self.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._catalog}.{schema}.{table} (
                name                   VARCHAR,
                created_at             DATE,
                total_transacciones    BIGINT,
                monto_total            DECIMAL(18, 2),
                monto_promedio         DECIMAL(18, 2),
                monto_maximo           DECIMAL(18, 2),
                monto_minimo           DECIMAL(18, 2),
                transacciones_pagadas  BIGINT,
                monto_pagado           DECIMAL(18, 2),
                monto_promedio_pagado  DECIMAL(18, 2)
            )
            WITH (
                external_location = 's3a://{bucket}/{path}',
                format            = 'PARQUET'
            )
        """)
        logger.info("[TrinoHook] Tabla '%s.%s.%s' lista.", self._catalog, schema, table)
        self.close()
