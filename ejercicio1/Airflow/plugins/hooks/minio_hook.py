"""
MinioHook
Encapsula la conexión y operaciones contra Minio usando la Connection de Airflow.
"""

import logging

import boto3
from botocore.client import Config

from airflow.sdk.bases.hook import BaseHook

logger = logging.getLogger(__name__)


class MinioHook(BaseHook):

    def __init__(self, conn_id: str = "minio_conn"):
        super().__init__()
        self.conn_id = conn_id
        conn = self.get_connection(self.conn_id)
        self._client = boto3.client(
            "s3",
            endpoint_url=conn.host,
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            config=Config(signature_version="s3v4"),
        )

    def read_bytes(self, bucket: str, key: str) -> bytes:
        """Lee un objeto de Minio y retorna su contenido como bytes."""
        response = self._client.get_object(Bucket=bucket, Key=key)
        data = response["Body"].read()
        logger.info("[MinioHook] Leídos %s bytes desde %s/%s", len(data), bucket, key)
        return data

    def check_for_key(self, bucket: str, key: str) -> None:
        """Valida que exista un objeto en Minio."""
        self._client.head_object(Bucket=bucket, Key=key)
        logger.info("[MinioHook] Objeto disponible en %s/%s", bucket, key)

    def write_bytes(self, bucket: str, key: str, data: bytes) -> None:
        """Sube bytes como objeto a Minio."""
        self._client.put_object(Bucket=bucket, Key=key, Body=data)
        logger.info("[MinioHook] Guardado en %s/%s", bucket, key)
