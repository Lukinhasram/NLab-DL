"""
config.py — Configuracoes centrais do pipeline NLab-DL
======================================================
Le variaveis de ambiente (com suporte opcional a .env na raiz do repo)
e fornece conexoes padronizadas para:
  - MariaDB OLTP (nlab_db)
  - Silo S3 (boto3 + limpeza idempotente de prefixos)
  - DuckDB (com httpfs, mysql opcional, limite de memoria e spill-to-disk)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from urllib.parse import urlparse

import boto3
import duckdb
import pymysql
from botocore.config import Config

try:
    from dotenv import load_dotenv

    _REPO_ROOT = Path(__file__).resolve().parents[2]
    load_dotenv(_REPO_ROOT / ".env", override=False)
except ImportError:
    pass

log = logging.getLogger(__name__)

# ── MariaDB fonte (nLab OLTP) ────────────────────────────────────────────────
MARIADB_HOST = os.environ.get("MARIADB_HOST", "127.0.0.1")
MARIADB_PORT = int(os.environ.get("MARIADB_PORT", "3307"))
MARIADB_DATABASE = os.environ.get("MARIADB_DATABASE", "nlab_db")
MARIADB_USER = os.environ.get("MARIADB_USER", "root")
MARIADB_PASSWORD = os.environ.get("MARIADB_PASSWORD", "senhadoida")

# ── Object Storage S3 (Silo) ─────────────────────────────────────────────────
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "siloadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "siloadmin123")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")

BUCKET_BRONZE = "bronze"
BUCKET_PRATA = "prata"
BUCKET_OURO = "ouro"

# ── Hive Metastore & Trino ───────────────────────────────────────────────────
HMS_HOST = os.environ.get("HMS_HOST", "127.0.0.1")
HMS_PORT = int(os.environ.get("HMS_PORT", "9083"))

TRINO_HOST = os.environ.get("TRINO_HOST", "127.0.0.1")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

# ── DuckDB (calibrado para host 16 GB / WSL2 8 GB) ───────────────────────────
DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "1000MB")
DUCKDB_THREADS = int(os.environ.get("DUCKDB_THREADS", "2"))
DUCKDB_TEMP_DIR = os.environ.get("DUCKDB_TEMP_DIR", "/tmp/duckdb_tmp")

# ── Tabelas fonte do nLab e tamanhos de lote por faixa de ID ─────────────────
TABELAS_FONTE = [
    "webs",
    "users",
    "categories",
    "wiki_files",
    "pages",
    "revisions",
    "wiki_references",
]

# None = tabela pequena (extracao em um unico arquivo Parquet)
CHUNK_SIZES: dict[str, int | None] = {
    "webs": None,
    "users": None,
    "categories": None,
    "wiki_files": None,
    "pages": 100_000,
    "revisions": 5_000,         # Uniforme em 5k IDs para manter RAM baixa no LONGTEXT
    "wiki_references": 250_000,
}


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def get_s3():
    """Retorna cliente boto3 configurado para o Silo (S3 path-style)."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
        region_name=S3_REGION,
    )


def garantir_buckets_s3(s3, buckets: tuple[str, ...] = (BUCKET_BRONZE, BUCKET_PRATA, BUCKET_OURO, "warehouse")) -> None:
    """Garante via boto3 que os buckets do Data Lake existem no Silo."""
    existentes = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    for bucket in buckets:
        if bucket not in existentes:
            s3.create_bucket(Bucket=bucket)
            log.info(f"  Bucket criado automaticamente no Silo: {bucket}")


def limpar_prefixo_s3(s3, bucket: str, prefix: str) -> int:
    """
    Remove todos os objetos sob um prefixo no Silo antes de regravar.
    Garante idempotencia sem deixar arquivos orfaos de execucoes anteriores.
    """
    removidos = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        if not contents:
            continue
        keys = [{"Key": obj["Key"]} for obj in contents]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
        removidos += len(keys)
    return removidos


def get_mariadb_conn():
    """Retorna conexao pymysql somente-leitura para consultas leves de controle."""
    return pymysql.connect(
        host=MARIADB_HOST,
        port=MARIADB_PORT,
        user=MARIADB_USER,
        password=MARIADB_PASSWORD,
        database=MARIADB_DATABASE,
        charset="utf8mb4",
        autocommit=True,
    )


def _load_extension(con: duckdb.DuckDBPyConnection, ext_name: str) -> None:
    try:
        con.execute(f"LOAD {ext_name};")
    except duckdb.Error:
        con.execute(f"INSTALL {ext_name}; LOAD {ext_name};")


def get_duckdb_conn(attach_mariadb: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Cria conexao DuckDB em memoria com spill-to-disk, limite de RAM explicito,
    secret S3 (Silo) via httpfs e opcionalmente MariaDB anexado como `nlab_src`.
    """
    os.makedirs(DUCKDB_TEMP_DIR, exist_ok=True)
    con = duckdb.connect(":memory:")

    con.execute(f"SET memory_limit = '{_sql_escape(DUCKDB_MEMORY_LIMIT)}';")
    con.execute(f"SET threads = {DUCKDB_THREADS};")
    con.execute(f"SET temp_directory = '{_sql_escape(DUCKDB_TEMP_DIR)}';")
    con.execute("SET preserve_insertion_order = false;")
    con.execute("SET TimeZone = 'UTC';")

    _load_extension(con, "httpfs")

    parsed = urlparse(S3_ENDPOINT)
    endpoint_host = parsed.netloc or parsed.path
    use_ssl = "true" if parsed.scheme == "https" else "false"

    con.execute(
        f"""
        CREATE OR REPLACE SECRET silo_s3 (
            TYPE s3,
            PROVIDER config,
            KEY_ID '{_sql_escape(S3_ACCESS_KEY)}',
            SECRET '{_sql_escape(S3_SECRET_KEY)}',
            ENDPOINT '{_sql_escape(endpoint_host)}',
            URL_STYLE 'path',
            USE_SSL {use_ssl},
            REGION '{_sql_escape(S3_REGION)}'
        );
        """
    )

    if attach_mariadb:
        _load_extension(con, "mysql")
        dsn = (
            f"host={MARIADB_HOST} "
            f"port={MARIADB_PORT} "
            f"user={MARIADB_USER} "
            f"password={MARIADB_PASSWORD} "
            f"database={MARIADB_DATABASE}"
        )
        con.execute(
            f"ATTACH '{_sql_escape(dsn)}' AS nlab_src (TYPE mysql, READ_ONLY);"
        )

    return con
