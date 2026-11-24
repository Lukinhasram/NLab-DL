"""
registrar_tabelas.py — Catalogo Hive Metastore (NLab-DL)
=========================================================
Registra os schemas `bronze`, `prata` e `ouro` no Hive Metastore via Thrift
apontando para os caminhos `s3a://` no Silo.

Melhorias sobre o template original:
  - Tabelas da camada Bronze sao registradas com `partitionKeys=[dt_ingestao]`
    e a particao `dt_ingestao=YYYY-MM-DD` e registrada explicitamente no HMS,
    evitando duplicacao de registros entre execucoes de dias diferentes.
  - Suporte a `--recreate` para atualizar metadados caso algum schema evolua.
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date

import hmsclient
from hmsclient.genthrift.hive_metastore.ttypes import (
    Database,
    FieldSchema,
    Partition,
    SerDeInfo,
    StorageDescriptor,
    Table,
)

from config import HMS_HOST, HMS_PORT, TRINO_HOST, TRINO_PORT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PIN = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
POUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PSDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"


def col(name: str, hive_type: str) -> FieldSchema:
    return FieldSchema(name=name, type=hive_type, comment="")


def _make_sd(cols: list[FieldSchema], location: str) -> StorageDescriptor:
    return StorageDescriptor(
        cols=cols,
        location=location,
        inputFormat=PIN,
        outputFormat=POUT,
        compressed=False,
        serdeInfo=SerDeInfo(
            serializationLib=PSDE,
            parameters={"serialization.format": "1"},
        ),
    )


def _hms(fn):
    with hmsclient.HMSClient(host=HMS_HOST, port=HMS_PORT) as client:
        return fn(client)


def criar_db(name: str) -> None:
    try:
        _hms(lambda c: c.create_database(Database(name=name, parameters={})))
        log.info(f"  OK db: {name}")
    except Exception as exc:
        if "AlreadyExists" in type(exc).__name__:
            log.info(f"  -> ja existe: {name}")
        else:
            log.error(f"  ERRO db {name}: {exc}")
            raise


def criar_tabela(
    db: str,
    nome: str,
    cols: list[FieldSchema],
    location: str,
    partition_keys: list[FieldSchema] | None = None,
    recreate: bool = False,
) -> None:
    label = f"{db}.{nome}"
    if recreate:
        try:
            _hms(lambda c: c.drop_table(db, nome, deleteData=False))
            log.info(f"  -> drop para recriacao: {label}")
        except Exception as exc:
            if "NoSuchObject" not in type(exc).__name__:
                raise

    try:
        _hms(
            lambda c: c.create_table(
                Table(
                    tableName=nome,
                    dbName=db,
                    owner="airflow",
                    tableType="EXTERNAL_TABLE",
                    sd=_make_sd(cols, location),
                    partitionKeys=partition_keys or [],
                    parameters={"EXTERNAL": "TRUE"},
                )
            )
        )
        log.info(f"  OK tabela: {label}")
    except Exception as exc:
        if "AlreadyExists" in type(exc).__name__:
            log.info(f"  -> ja existe: {label}")
        else:
            log.error(f"  ERRO tabela {label}: {exc}")
            raise


def registrar_particao_bronze(nome: str, cols: list[FieldSchema], dt_ingestao: str) -> None:
    part_location = f"s3a://bronze/{nome}/dt_ingestao={dt_ingestao}/"
    now_ts = int(time.time())
    part = Partition(
        values=[dt_ingestao],
        dbName="bronze",
        tableName=nome,
        createTime=now_ts,
        lastAccessTime=0,
        sd=_make_sd(cols, part_location),
        parameters={},
    )
    try:
        _hms(lambda c: c.add_partition(part))
        log.info(f"    OK particao bronze.{nome} [dt_ingestao={dt_ingestao}]")
    except Exception as exc:
        if "AlreadyExists" in type(exc).__name__:
            log.info(f"    -> particao ja registrada: bronze.{nome} [dt_ingestao={dt_ingestao}]")
        else:
            log.error(f"    ERRO particao bronze.{nome}: {exc}")
            raise


def _sync_trino_partitions(tabelas_bronze: list[str]) -> None:
    """Sincronizacao opcional via Trino caso existam particoes anteriores no Silo."""
    try:
        import trino
    except ImportError:
        return

    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user="airflow",
            catalog="hive",
            schema="bronze",
        )
        cur = conn.cursor()
        for tabela in tabelas_bronze:
            cur.execute(
                f"CALL hive.system.sync_partition_metadata('bronze', '{tabela}', 'ADD')"
            )
            cur.fetchall()
        conn.close()
        log.info("  OK sincronizacao de particoes via Trino concluida")
    except Exception as exc:
        log.warning(f"  Aviso: sync opcional via Trino indisponivel ({exc}) — particoes ja registradas direto no HMS.")


# ── Definicoes de Schema (1:1 com os Parquets gerados pelo DuckDB) ───────────

BRONZE_SCHEMAS: dict[str, list[FieldSchema]] = {
    "webs": [
        col("id", "bigint"), col("created_at", "timestamp"), col("updated_at", "timestamp"),
        col("name", "string"), col("address", "string"), col("password", "string"),
        col("additional_style", "string"), col("allow_uploads", "bigint"), col("published", "bigint"),
        col("count_pages", "bigint"), col("markup", "string"), col("color", "string"),
        col("max_upload_size", "bigint"), col("safe_mode", "bigint"), col("brackets_only", "bigint"),
        col("_dt_ingestao", "timestamp"), col("_fonte", "string"),
    ],
    "users": [
        col("id", "bigint"), col("name", "string"), col("total_revisions", "bigint"),
        col("pages_created", "bigint"), col("first_edit_at", "timestamp"), col("last_edit_at", "timestamp"),
        col("_dt_ingestao", "timestamp"), col("_fonte", "string"),
    ],
    "categories": [
        col("id", "bigint"), col("name", "string"), col("page_count", "bigint"),
        col("_dt_ingestao", "timestamp"), col("_fonte", "string"),
    ],
    "wiki_files": [
        col("id", "bigint"), col("created_at", "timestamp"), col("updated_at", "timestamp"),
        col("web_id", "bigint"), col("file_name", "string"), col("file_type", "string"),
        col("description", "string"),
        col("_dt_ingestao", "timestamp"), col("_fonte", "string"),
    ],
    "pages": [
        col("id", "bigint"), col("created_at", "timestamp"), col("updated_at", "timestamp"),
        col("web_id", "bigint"), col("locked_by", "string"), col("name", "string"),
        col("locked_at", "timestamp"), col("revision_count", "bigint"),
        col("first_revised_at", "timestamp"), col("last_revised_at", "timestamp"),
        col("_dt_ingestao", "timestamp"), col("_fonte", "string"),
    ],
    "revisions": [
        col("id", "bigint"), col("created_at", "timestamp"), col("updated_at", "timestamp"),
        col("revised_at", "timestamp"), col("page_id", "bigint"), col("content", "string"),
        col("author_id", "bigint"), col("ip", "string"), col("web_id", "bigint"),
        col("_dt_ingestao", "timestamp"), col("_fonte", "string"),
    ],
    "wiki_references": [
        col("id", "bigint"), col("created_at", "timestamp"), col("updated_at", "timestamp"),
        col("page_id", "bigint"), col("referenced_name", "string"), col("link_type", "string"),
        col("target_page_id", "bigint"), col("category_id", "bigint"), col("file_id", "bigint"),
        col("_dt_ingestao", "timestamp"), col("_fonte", "string"),
    ],
}

PRATA_SCHEMAS: dict[str, list[FieldSchema]] = {
    "webs": [
        col("id", "bigint"), col("name", "string"), col("address", "string"),
        col("allow_uploads", "bigint"), col("published", "bigint"), col("count_pages", "bigint"),
        col("markup", "string"), col("color", "string"), col("max_upload_size", "bigint"),
        col("safe_mode", "bigint"), col("brackets_only", "bigint"),
        col("created_at", "timestamp"), col("updated_at", "timestamp"),
    ],
    "users": [
        col("id", "bigint"), col("name", "string"), col("total_revisions", "bigint"),
        col("pages_created", "bigint"), col("avg_revisions_per_page", "double"),
        col("first_edit_at", "timestamp"), col("last_edit_at", "timestamp"),
        col("tier_atividade", "string"),
    ],
    "categories": [
        col("id", "bigint"), col("name", "string"), col("page_count", "bigint"),
    ],
    "wiki_files": [
        col("id", "bigint"), col("web_id", "bigint"), col("file_name", "string"),
        col("file_type", "string"), col("description", "string"),
        col("created_at", "timestamp"), col("updated_at", "timestamp"),
    ],
    "pages": [
        col("id", "bigint"), col("web_id", "bigint"), col("name", "string"),
        col("revision_count", "bigint"), col("created_at", "timestamp"),
        col("updated_at", "timestamp"), col("first_revised_at", "timestamp"),
        col("last_revised_at", "timestamp"), col("dt_criacao", "date"),
        col("dt_ultima_revisao", "date"), col("fl_bloqueada", "boolean"),
        col("fl_multi_revisao", "boolean"),
    ],
    "revisions": [
        col("id", "bigint"), col("page_id", "bigint"), col("author_id", "bigint"),
        col("web_id", "bigint"), col("created_at", "timestamp"), col("updated_at", "timestamp"),
        col("revised_at", "timestamp"), col("dt_revisao", "date"), col("hr_revisao", "bigint"),
        col("nr_sequencia_revisao", "bigint"), col("fl_criacao_pagina", "boolean"),
        col("content_length", "bigint"), col("word_count", "bigint"),
        col("internal_link_count", "bigint"), col("fl_possui_latex", "boolean"),
        col("fl_conteudo_vazio", "boolean"),
    ],
    "wiki_references": [
        col("id", "bigint"), col("page_id", "bigint"), col("referenced_name", "string"),
        col("link_type", "string"), col("ds_tipo_referencia", "string"), col("ds_grupo", "string"),
        col("target_page_id", "bigint"), col("category_id", "bigint"), col("file_id", "bigint"),
        col("fl_resolvido", "boolean"), col("created_at", "timestamp"), col("updated_at", "timestamp"),
    ],
}

OURO_SCHEMAS: dict[str, list[FieldSchema]] = {
    "dim_tempo": [
        col("sk_data", "bigint"), col("dt_data", "date"), col("nr_ano", "bigint"),
        col("nr_mes", "bigint"), col("nr_dia", "bigint"), col("nr_trimestre", "bigint"),
        col("ds_trimestre", "string"), col("nm_mes", "string"), col("ds_ano_mes", "string"),
        col("nr_dia_semana", "bigint"), col("ds_dia_semana", "string"), col("fl_fim_semana", "boolean"),
    ],
    "dim_usuario": [
        col("sk_usuario", "bigint"), col("nm_usuario", "string"),
        col("qtd_total_revisoes", "bigint"), col("qtd_paginas_criadas", "bigint"),
        col("vl_media_revisoes_pagina", "double"), col("dt_primeira_edicao", "timestamp"),
        col("dt_ultima_edicao", "timestamp"), col("ds_tier_atividade", "string"),
    ],
    "dim_categoria": [
        col("sk_categoria", "bigint"), col("nm_categoria", "string"), col("qtd_paginas", "bigint"),
    ],
    "dim_tipo_referencia": [
        col("sk_tipo_referencia", "bigint"), col("cd_link_type", "string"),
        col("ds_tipo_referencia", "string"), col("ds_grupo", "string"),
    ],
    "dim_pagina": [
        col("sk_pagina", "bigint"), col("nm_pagina", "string"),
        col("nm_categoria_principal", "string"), col("qtd_revisoes", "bigint"),
        col("dt_criacao", "date"), col("dt_ultima_revisao", "date"),
        col("qtd_links_entrada", "bigint"), col("qtd_links_saida", "bigint"),
        col("fl_multi_revisao", "boolean"), col("fl_bloqueada", "boolean"),
    ],
    "fato_revisoes": [
        col("cd_revisao", "bigint"), col("sk_data", "bigint"), col("sk_pagina", "bigint"),
        col("sk_usuario", "bigint"), col("hr_revisao", "bigint"),
        col("nr_sequencia_revisao", "bigint"), col("fl_criacao_pagina", "boolean"),
        col("vl_bytes_conteudo", "bigint"), col("vl_qtd_palavras", "bigint"),
        col("vl_qtd_links_internos", "bigint"), col("fl_possui_latex", "boolean"),
        col("fl_conteudo_vazio", "boolean"),
    ],
    "fato_referencias": [
        col("cd_referencia", "bigint"), col("sk_pagina_origem", "bigint"),
        col("sk_pagina_destino", "bigint"), col("sk_categoria", "bigint"),
        col("sk_tipo_referencia", "bigint"), col("cd_arquivo", "bigint"),
        col("nm_referenciado", "string"), col("fl_resolvido", "boolean"),
    ],
}


def main(dt_ingestao: str | None = None, recreate: bool = False) -> None:
    if dt_ingestao is None:
        dt_ingestao = date.today().isoformat()

    log.info(f"Conectando ao Hive Metastore: {HMS_HOST}:{HMS_PORT}")

    log.info("-- Databases")
    for db in ("bronze", "prata", "ouro"):
        criar_db(db)

    log.info("-- BRONZE (particionada por dt_ingestao)")
    part_keys = [col("dt_ingestao", "string")]
    for tabela, cols in BRONZE_SCHEMAS.items():
        criar_tabela(
            "bronze",
            tabela,
            cols,
            f"s3a://bronze/{tabela}/",
            partition_keys=part_keys,
            recreate=recreate,
        )
        registrar_particao_bronze(tabela, cols, dt_ingestao)

    log.info("-- PRATA")
    for tabela, cols in PRATA_SCHEMAS.items():
        criar_tabela("prata", tabela, cols, f"s3a://prata/{tabela}/", recreate=recreate)

    log.info("-- OURO (Star Schema)")
    for tabela, cols in OURO_SCHEMAS.items():
        criar_tabela("ouro", tabela, cols, f"s3a://ouro/{tabela}/", recreate=recreate)

    _sync_trino_partitions(list(BRONZE_SCHEMAS.keys()))
    log.info("Catalogo Hive Metastore atualizado com sucesso.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Registra tabelas do NLab-DL no Hive Metastore")
    parser.add_argument("--data", default=date.today().isoformat(), help="Particao dt_ingestao YYYY-MM-DD")
    parser.add_argument("--recreate", action="store_true", help="Recria metadados das tabelas existentes")
    args = parser.parse_args()
    main(dt_ingestao=args.data, recreate=args.recreate)
