"""
bronze.py — Camada Bronze (NLab-DL)
====================================
Extrai as 7 tabelas do MariaDB (nlab_db) via DuckDB (extensao mysql) e grava
em formato Parquet (Snappy) no Silo (S3), particionadas por `dt_ingestao`.

Caracteristicas de escala (host 16 GB):
  - Tabelas grandes (`pages`, `revisions`, `wiki_references`) sao lidas por
    faixas contiguas de PRIMARY KEY (`id`), gravando multiplos arquivos
    `part_00001.parquet`, `part_00002.parquet`, ...
  - `revisions.content` (LONGTEXT) e preservado integralmente na Bronze em
    lotes uniformes de 5k IDs para manter o consumo de RAM sob controle
    inclusive em hosts Windows/WSL2 com 8 GB alocados.
  - Casts explicitos (BIGINT, TIMESTAMP, VARCHAR) garantem compatibilidade
    estrita com Hive Metastore e Trino 380 (sem float64 em FKs nulas).
"""

from __future__ import annotations

import logging
from datetime import date

from config import (
    BUCKET_BRONZE,
    CHUNK_SIZES,
    TABELAS_FONTE,
    garantir_buckets_s3,
    get_duckdb_conn,
    get_mariadb_conn,
    get_s3,
    limpar_prefixo_s3,
)

log = logging.getLogger(__name__)

# Projecoes explicitas alinhadas 1:1 com registrar_tabelas.py (Hive Metastore)
BRONZE_SELECT_SQL: dict[str, str] = {
    "webs": """
        CAST(id AS BIGINT) AS id,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        CAST(name AS VARCHAR) AS name,
        CAST(address AS VARCHAR) AS address,
        CAST(password AS VARCHAR) AS password,
        CAST(additional_style AS VARCHAR) AS additional_style,
        CAST(allow_uploads AS BIGINT) AS allow_uploads,
        CAST(published AS BIGINT) AS published,
        CAST(count_pages AS BIGINT) AS count_pages,
        CAST(markup AS VARCHAR) AS markup,
        CAST(color AS VARCHAR) AS color,
        CAST(max_upload_size AS BIGINT) AS max_upload_size,
        CAST(safe_mode AS BIGINT) AS safe_mode,
        CAST(brackets_only AS BIGINT) AS brackets_only
    """,
    "users": """
        CAST(id AS BIGINT) AS id,
        CAST(name AS VARCHAR) AS name,
        CAST(total_revisions AS BIGINT) AS total_revisions,
        CAST(pages_created AS BIGINT) AS pages_created,
        CAST(first_edit_at AS TIMESTAMP) AS first_edit_at,
        CAST(last_edit_at AS TIMESTAMP) AS last_edit_at
    """,
    "categories": """
        CAST(id AS BIGINT) AS id,
        CAST(name AS VARCHAR) AS name,
        CAST(page_count AS BIGINT) AS page_count
    """,
    "wiki_files": """
        CAST(id AS BIGINT) AS id,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        CAST(web_id AS BIGINT) AS web_id,
        CAST(file_name AS VARCHAR) AS file_name,
        CAST(file_type AS VARCHAR) AS file_type,
        CAST(description AS VARCHAR) AS description
    """,
    "pages": """
        CAST(id AS BIGINT) AS id,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        CAST(web_id AS BIGINT) AS web_id,
        CAST(locked_by AS VARCHAR) AS locked_by,
        CAST(name AS VARCHAR) AS name,
        CAST(locked_at AS TIMESTAMP) AS locked_at,
        CAST(revision_count AS BIGINT) AS revision_count,
        CAST(first_revised_at AS TIMESTAMP) AS first_revised_at,
        CAST(last_revised_at AS TIMESTAMP) AS last_revised_at
    """,
    "revisions": """
        CAST(id AS BIGINT) AS id,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        CAST(revised_at AS TIMESTAMP) AS revised_at,
        CAST(page_id AS BIGINT) AS page_id,
        CAST(content AS VARCHAR) AS content,
        CAST(author_id AS BIGINT) AS author_id,
        CAST(ip AS VARCHAR) AS ip,
        CAST(web_id AS BIGINT) AS web_id
    """,
    "wiki_references": """
        CAST(id AS BIGINT) AS id,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        CAST(page_id AS BIGINT) AS page_id,
        CAST(referenced_name AS VARCHAR) AS referenced_name,
        CAST(link_type AS VARCHAR) AS link_type,
        CAST(target_page_id AS BIGINT) AS target_page_id,
        CAST(category_id AS BIGINT) AS category_id,
        CAST(file_id AS BIGINT) AS file_id
    """,
}


def _obter_limites_tabela(mdb_conn, tabela: str) -> tuple[int, int]:
    """Consulta O(1) de MIN(id) e MAX(id) direto na B-Tree da PK (evita full scan)."""
    with mdb_conn.cursor() as cur:
        cur.execute(f"SELECT MIN(id), MAX(id) FROM `{tabela}`")
        min_id, max_id = cur.fetchone()
    if min_id is None or max_id is None:
        return 0, 0
    return int(min_id), int(max_id)


def _contar_faixa(mdb_conn, tabela: str, start_id: int, end_id: int) -> int:
    with mdb_conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM `{tabela}` WHERE id >= %s AND id <= %s",
            (start_id, end_id),
        )
        return int(cur.fetchone()[0])


def ingerir_tabela(con, mdb_conn, s3, tabela: str, dt_ingestao: str) -> dict:
    """Ingere uma tabela do MariaDB para s3://bronze/{tabela}/dt_ingestao={dt}/."""
    if tabela not in BRONZE_SELECT_SQL:
        raise ValueError(f"Tabela nao suportada na Bronze: {tabela}")

    log.info(f"  > Lendo {tabela}...")
    min_id, max_id = _obter_limites_tabela(mdb_conn, tabela)

    if max_id == 0:
        log.warning(f"    Tabela {tabela} esta vazia — pulando.")
        return {"tabela": tabela, "linhas": 0, "arquivos": 0, "status": "vazio"}

    prefix = f"{tabela}/dt_ingestao={dt_ingestao}/"
    removidos = limpar_prefixo_s3(s3, BUCKET_BRONZE, prefix)
    if removidos:
        log.info(f"    Limpeza idempotente: {removidos} arquivo(s) antigo(s) removido(s) em s3://{BUCKET_BRONZE}/{prefix}")

    select_cols = BRONZE_SELECT_SQL[tabela].strip()
    chunk_size = CHUNK_SIZES.get(tabela)

    # Tabela pequena: exportacao unica
    if chunk_size is None or (max_id - min_id + 1) <= chunk_size:
        s3_uri = f"s3://{BUCKET_BRONZE}/{prefix}part_00001.parquet"
        con.execute(
            f"""
            COPY (
                SELECT
                    {select_cols},
                    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS _dt_ingestao,
                    'mariadb/{tabela}' AS _fonte
                FROM nlab_src.{tabela}
            ) TO '{s3_uri}'
            (FORMAT PARQUET, COMPRESSION SNAPPY);
            """
        )
        total_linhas = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{s3_uri}')").fetchone()[0])
        log.info(f"    OK {total_linhas:,} linhas -> {s3_uri}")
        return {"tabela": tabela, "linhas": total_linhas, "arquivos": 1, "status": "ok"}

    # Tabela grande: paginacao deterministica por faixa de PRIMARY KEY (id)
    part_idx = 0
    linhas_gravadas = 0
    cur_start = min_id

    while cur_start <= max_id:
        cur_end = min(cur_start + chunk_size - 1, max_id)
        qtd_faixa = _contar_faixa(mdb_conn, tabela, cur_start, cur_end)
        if qtd_faixa > 0:
            part_idx += 1
            s3_uri = f"s3://{BUCKET_BRONZE}/{prefix}part_{part_idx:05d}.parquet"
            con.execute(
                f"""
                COPY (
                    SELECT
                        {select_cols},
                        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS _dt_ingestao,
                        'mariadb/{tabela}' AS _fonte
                    FROM nlab_src.{tabela}
                    WHERE id >= {cur_start} AND id <= {cur_end}
                ) TO '{s3_uri}'
                (FORMAT PARQUET, COMPRESSION SNAPPY);
                """
            )
            linhas_gravadas += qtd_faixa
            if part_idx == 1 or part_idx % 10 == 0 or cur_end >= max_id:
                log.info(
                    f"    part_{part_idx:05d}.parquet: +{qtd_faixa:,} linhas "
                    f"(acumulado {linhas_gravadas:,}, ids {cur_start:,}..{cur_end:,} / max {max_id:,})"
                )
        cur_start = cur_end + 1

    log.info(f"    OK {linhas_gravadas:,} linhas em {part_idx} arquivo(s) -> s3://{BUCKET_BRONZE}/{prefix}")
    return {
        "tabela": tabela,
        "linhas": linhas_gravadas,
        "arquivos": part_idx,
        "status": "ok",
    }


def executar_bronze(dt_ingestao: str | None = None, tabelas: list[str] | None = None) -> list[dict]:
    """Ponto de entrada da camada Bronze."""
    if dt_ingestao is None:
        dt_ingestao = date.today().isoformat()

    alvos = tabelas if tabelas else list(TABELAS_FONTE)

    log.info("=" * 60)
    log.info(f" BRONZE — Ingestao MariaDB -> Silo ({len(alvos)} tabelas)")
    log.info(f" Particao: dt_ingestao={dt_ingestao}")
    log.info("=" * 60)

    s3 = get_s3()
    garantir_buckets_s3(s3)
    mdb_conn = get_mariadb_conn()
    con = get_duckdb_conn(attach_mariadb=True)
    metricas: list[dict] = []

    try:
        for tabela in alvos:
            try:
                resultado = ingerir_tabela(con, mdb_conn, s3, tabela, dt_ingestao)
                metricas.append(resultado)
            except Exception as exc:
                log.error(f"  ERRO em {tabela}: {exc}", exc_info=True)
                metricas.append({"tabela": tabela, "linhas": 0, "arquivos": 0, "status": "erro", "erro": str(exc)})
    finally:
        con.close()
        mdb_conn.close()

    total = sum(m["linhas"] for m in metricas)
    ok = sum(1 for m in metricas if m["status"] == "ok")
    log.info(f"Resumo Bronze: {ok}/{len(alvos)} tabelas OK · {total:,} linhas totais")
    return metricas
