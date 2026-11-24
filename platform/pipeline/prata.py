"""
prata.py — Camada Prata (NLab-DL)
==================================
Le os Parquets da particao do dia na camada Bronze via DuckDB, aplica
limpeza, tipagem estrita e enriquecimento, e grava snapshots limpos na
camada Prata (`s3://prata/{tabela}/{tabela}.parquet`).

Destaques de performance e modelagem:
  - `revisions`: calcula metricas de texto a partir de `content` (LONGTEXT)
    em streaming e descarta o texto bruto antes da window function,
    reduzindo o tamanho de ~4 GB para dezenas de MB.
  - `wiki_references`: mapeia `link_type` para descricoes legiveis e resolve
    `target_page_id` faltantes (links sinteticos) via lookup por nome em `pages`.
"""

from __future__ import annotations

import logging
from datetime import date

from config import (
    BUCKET_BRONZE,
    BUCKET_PRATA,
    get_duckdb_conn,
    get_s3,
    limpar_prefixo_s3,
)

log = logging.getLogger(__name__)


def _bronze_glob(tabela: str, dt_ingestao: str) -> str:
    return f"s3://{BUCKET_BRONZE}/{tabela}/dt_ingestao={dt_ingestao}/*.parquet"


def _prata_file(tabela: str) -> str:
    return f"s3://{BUCKET_PRATA}/{tabela}/{tabela}.parquet"


def _executar_copy_prata(con, s3, tabela: str, query_sql: str) -> int:
    prefix = f"{tabela}/"
    limpar_prefixo_s3(s3, BUCKET_PRATA, prefix)

    destino = _prata_file(tabela)
    con.execute(
        f"""
        COPY (
            {query_sql}
        ) TO '{destino}'
        (FORMAT PARQUET, COMPRESSION SNAPPY);
        """
    )
    linhas = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{destino}')").fetchone()[0])
    log.info(f"    OK {linhas:,} linhas -> {destino}")
    return linhas


def _sql_webs(dt_ingestao: str) -> str:
    src = _bronze_glob("webs", dt_ingestao)
    return f"""
        SELECT
            CAST(id AS BIGINT) AS id,
            TRIM(name) AS name,
            TRIM(address) AS address,
            CAST(COALESCE(allow_uploads, 0) AS BIGINT) AS allow_uploads,
            CAST(COALESCE(published, 0) AS BIGINT) AS published,
            CAST(COALESCE(count_pages, 0) AS BIGINT) AS count_pages,
            TRIM(COALESCE(markup, 'markdownMML')) AS markup,
            UPPER(TRIM(COALESCE(color, '008B26'))) AS color,
            CAST(COALESCE(max_upload_size, 0) AS BIGINT) AS max_upload_size,
            CAST(COALESCE(safe_mode, 0) AS BIGINT) AS safe_mode,
            CAST(COALESCE(brackets_only, 0) AS BIGINT) AS brackets_only,
            CAST(created_at AS TIMESTAMP) AS created_at,
            CAST(updated_at AS TIMESTAMP) AS updated_at
        FROM read_parquet('{src}')
        WHERE id IS NOT NULL
    """


def _sql_users(dt_ingestao: str) -> str:
    src = _bronze_glob("users", dt_ingestao)
    return f"""
        SELECT
            CAST(id AS BIGINT) AS id,
            TRIM(name) AS name,
            CAST(COALESCE(total_revisions, 0) AS BIGINT) AS total_revisions,
            CAST(COALESCE(pages_created, 0) AS BIGINT) AS pages_created,
            ROUND(
                CAST(COALESCE(total_revisions, 0) AS DOUBLE) / NULLIF(CAST(pages_created AS DOUBLE), 0.0),
                2
            ) AS avg_revisions_per_page,
            CAST(first_edit_at AS TIMESTAMP) AS first_edit_at,
            CAST(last_edit_at AS TIMESTAMP) AS last_edit_at,
            CASE
                WHEN COALESCE(total_revisions, 0) >= 1000 THEN 'Core'
                WHEN COALESCE(total_revisions, 0) >= 100  THEN 'Ativo'
                WHEN COALESCE(total_revisions, 0) >= 10   THEN 'Regular'
                ELSE 'Casual'
            END AS tier_atividade
        FROM read_parquet('{src}')
        WHERE id IS NOT NULL AND name IS NOT NULL AND TRIM(name) <> ''
    """


def _sql_categories(dt_ingestao: str) -> str:
    src = _bronze_glob("categories", dt_ingestao)
    return f"""
        SELECT
            CAST(id AS BIGINT) AS id,
            TRIM(name) AS name,
            CAST(COALESCE(page_count, 0) AS BIGINT) AS page_count
        FROM read_parquet('{src}')
        WHERE id IS NOT NULL AND name IS NOT NULL AND TRIM(name) <> ''
    """


def _sql_wiki_files(dt_ingestao: str) -> str:
    src = _bronze_glob("wiki_files", dt_ingestao)
    return f"""
        SELECT
            CAST(id AS BIGINT) AS id,
            CAST(web_id AS BIGINT) AS web_id,
            TRIM(file_name) AS file_name,
            COALESCE(NULLIF(LOWER(TRIM(file_type)), ''), 'unknown') AS file_type,
            NULLIF(TRIM(description), '') AS description,
            CAST(created_at AS TIMESTAMP) AS created_at,
            CAST(updated_at AS TIMESTAMP) AS updated_at
        FROM read_parquet('{src}')
        WHERE id IS NOT NULL AND file_name IS NOT NULL AND TRIM(file_name) <> ''
    """


def _sql_pages(dt_ingestao: str) -> str:
    src = _bronze_glob("pages", dt_ingestao)
    return f"""
        SELECT
            CAST(id AS BIGINT) AS id,
            CAST(web_id AS BIGINT) AS web_id,
            TRIM(name) AS name,
            CAST(COALESCE(revision_count, 0) AS BIGINT) AS revision_count,
            CAST(created_at AS TIMESTAMP) AS created_at,
            CAST(updated_at AS TIMESTAMP) AS updated_at,
            CAST(first_revised_at AS TIMESTAMP) AS first_revised_at,
            CAST(last_revised_at AS TIMESTAMP) AS last_revised_at,
            CAST(created_at AS DATE) AS dt_criacao,
            CAST(COALESCE(last_revised_at, updated_at, created_at) AS DATE) AS dt_ultima_revisao,
            (locked_by IS NOT NULL AND TRIM(locked_by) <> '') AS fl_bloqueada,
            (COALESCE(revision_count, 0) > 1) AS fl_multi_revisao
        FROM read_parquet('{src}')
        WHERE id IS NOT NULL AND name IS NOT NULL AND TRIM(name) <> ''
    """


def _sql_revisions(dt_ingestao: str) -> str:
    src = _bronze_glob("revisions", dt_ingestao)
    # A CTE calcula metricas de texto e descarta `content` (LONGTEXT) antes
    # do operador de janela ROW_NUMBER(), evitando estourar RAM.
    return f"""
        WITH metrics AS (
            SELECT
                CAST(id AS BIGINT) AS id,
                CAST(page_id AS BIGINT) AS page_id,
                CAST(author_id AS BIGINT) AS author_id,
                CAST(web_id AS BIGINT) AS web_id,
                CAST(created_at AS TIMESTAMP) AS created_at,
                CAST(updated_at AS TIMESTAMP) AS updated_at,
                CAST(revised_at AS TIMESTAMP) AS revised_at,
                CAST(revised_at AS DATE) AS dt_revisao,
                CAST(EXTRACT(HOUR FROM revised_at) AS BIGINT) AS hr_revisao,
                CAST(COALESCE(OCTET_LENGTH(content), 0) AS BIGINT) AS content_length,
                CAST(
                    CASE
                        WHEN content IS NULL OR TRIM(content) = '' THEN 0
                        ELSE LENGTH(TRIM(content)) - LENGTH(REPLACE(TRIM(content), ' ', '')) + 1
                    END
                AS BIGINT) AS word_count,
                CAST(
                    COALESCE((LENGTH(content) - LENGTH(REPLACE(content, '[[', ''))) / 2, 0)
                AS BIGINT) AS internal_link_count,
                COALESCE(CONTAINS(content, '$') OR CONTAINS(content, '\\['), FALSE) AS fl_possui_latex,
                (content IS NULL OR TRIM(content) = '') AS fl_conteudo_vazio
            FROM read_parquet('{src}')
            WHERE id IS NOT NULL AND page_id IS NOT NULL AND revised_at IS NOT NULL
        ),
        sequenciado AS (
            SELECT
                *,
                CAST(ROW_NUMBER() OVER (PARTITION BY page_id ORDER BY revised_at ASC, id ASC) AS BIGINT) AS nr_sequencia_revisao
            FROM metrics
        )
        SELECT
            id,
            page_id,
            author_id,
            web_id,
            created_at,
            updated_at,
            revised_at,
            dt_revisao,
            hr_revisao,
            nr_sequencia_revisao,
            (nr_sequencia_revisao = 1) AS fl_criacao_pagina,
            content_length,
            word_count,
            internal_link_count,
            fl_possui_latex,
            fl_conteudo_vazio
        FROM sequenciado
    """


def _sql_wiki_references(dt_ingestao: str) -> str:
    src_refs = _bronze_glob("wiki_references", dt_ingestao)
    prata_pages = _prata_file("pages")
    return f"""
        WITH page_lookup AS (
            SELECT name, MIN(id) AS resolved_page_id
            FROM read_parquet('{prata_pages}')
            GROUP BY name
        ),
        base AS (
            SELECT
                CAST(r.id AS BIGINT) AS id,
                CAST(r.page_id AS BIGINT) AS page_id,
                TRIM(r.referenced_name) AS referenced_name,
                UPPER(TRIM(r.link_type)) AS link_type,
                CASE
                    WHEN r.target_page_id IS NOT NULL THEN CAST(r.target_page_id AS BIGINT)
                    WHEN UPPER(TRIM(r.link_type)) IN ('L', 'I') THEN CAST(p.resolved_page_id AS BIGINT)
                    ELSE NULL
                END AS target_page_id,
                CAST(r.category_id AS BIGINT) AS category_id,
                CAST(r.file_id AS BIGINT) AS file_id,
                CAST(r.created_at AS TIMESTAMP) AS created_at,
                CAST(r.updated_at AS TIMESTAMP) AS updated_at
            FROM read_parquet('{src_refs}') r
            LEFT JOIN page_lookup p
              ON UPPER(TRIM(r.link_type)) IN ('L', 'I')
             AND r.target_page_id IS NULL
             AND TRIM(r.referenced_name) = p.name
            WHERE r.id IS NOT NULL AND r.page_id IS NOT NULL
        )
        SELECT
            id,
            page_id,
            referenced_name,
            link_type,
            CASE link_type
                WHEN 'L' THEN 'Link Interno'
                WHEN 'W' THEN 'Link Quebrado (Wanted)'
                WHEN 'R' THEN 'Redirect'
                WHEN 'I' THEN 'Include'
                WHEN 'C' THEN 'Categoria'
                WHEN 'F' THEN 'Arquivo'
                WHEN 'E' THEN 'Arquivo/Externo'
                ELSE 'Outro'
            END AS ds_tipo_referencia,
            CASE link_type
                WHEN 'L' THEN 'Grafo Wiki'
                WHEN 'W' THEN 'Grafo Wiki'
                WHEN 'R' THEN 'Grafo Wiki'
                WHEN 'I' THEN 'Grafo Wiki'
                WHEN 'C' THEN 'Taxonomia'
                WHEN 'F' THEN 'Midia'
                WHEN 'E' THEN 'Midia'
                ELSE 'Outro'
            END AS ds_grupo,
            target_page_id,
            category_id,
            file_id,
            (target_page_id IS NOT NULL OR category_id IS NOT NULL OR file_id IS NOT NULL) AS fl_resolvido,
            created_at,
            updated_at
        FROM base
    """


# Ordem importante: `pages` deve rodar antes de `wiki_references`
ETAPAS_PRATA = [
    ("webs", _sql_webs),
    ("users", _sql_users),
    ("categories", _sql_categories),
    ("wiki_files", _sql_wiki_files),
    ("pages", _sql_pages),
    ("revisions", _sql_revisions),
    ("wiki_references", _sql_wiki_references),
]


def executar_prata(dt_ingestao: str | None = None, tabelas: list[str] | None = None) -> list[dict]:
    """Ponto de entrada da camada Prata."""
    if dt_ingestao is None:
        dt_ingestao = date.today().isoformat()

    etapas = ETAPAS_PRATA
    if tabelas:
        alvo_set = set(tabelas)
        etapas = [(nome, fn) for nome, fn in ETAPAS_PRATA if nome in alvo_set]

    log.info("=" * 60)
    log.info(f" PRATA — Limpeza e enriquecimento via DuckDB ({len(etapas)} tabelas)")
    log.info(f" Particao Bronze de origem: dt_ingestao={dt_ingestao}")
    log.info("=" * 60)

    s3 = get_s3()
    con = get_duckdb_conn(attach_mariadb=False)
    metricas: list[dict] = []

    try:
        for tabela, sql_builder in etapas:
            log.info(f"  > Transformando {tabela}...")
            try:
                linhas = _executar_copy_prata(con, s3, tabela, sql_builder(dt_ingestao))
                metricas.append({"tabela": tabela, "linhas": linhas, "status": "ok"})
            except Exception as exc:
                log.error(f"  ERRO em {tabela}: {exc}", exc_info=True)
                metricas.append({"tabela": tabela, "linhas": 0, "status": "erro", "erro": str(exc)})
    finally:
        con.close()

    total = sum(m["linhas"] for m in metricas)
    ok = sum(1 for m in metricas if m["status"] == "ok")
    log.info(f"Resumo Prata: {ok}/{len(metricas)} tabelas OK · {total:,} linhas totais")
    return metricas
