"""
ouro.py — Camada Ouro / Star Schema (NLab-DL)
=============================================
Le a camada Prata no Silo via DuckDB e constroi o modelo dimensional do nLab:

  Dimensoes:
    - dim_tempo
    - dim_usuario
    - dim_categoria
    - dim_tipo_referencia
    - dim_pagina (enriquecida com categoria principal e in/out-degree)

  Fatos:
    - fato_revisoes    (1 linha por revisao)
    - fato_referencias (1 linha por aresta/referencia)
"""

from __future__ import annotations

import logging
from datetime import date

from config import (
    BUCKET_OURO,
    BUCKET_PRATA,
    get_duckdb_conn,
    get_s3,
    limpar_prefixo_s3,
)

log = logging.getLogger(__name__)


def _prata_file(tabela: str) -> str:
    return f"s3://{BUCKET_PRATA}/{tabela}/{tabela}.parquet"


def _ouro_file(tabela: str) -> str:
    return f"s3://{BUCKET_OURO}/{tabela}/{tabela}.parquet"


def _executar_copy_ouro(con, s3, tabela: str, query_sql: str) -> int:
    prefix = f"{tabela}/"
    limpar_prefixo_s3(s3, BUCKET_OURO, prefix)

    destino = _ouro_file(tabela)
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


def _sql_dim_tempo() -> str:
    prata_revs = _prata_file("revisions")
    prata_pages = _prata_file("pages")
    return f"""
        WITH limites AS (
            SELECT
                LEAST(
                    (SELECT MIN(dt_revisao) FROM read_parquet('{prata_revs}')),
                    (SELECT MIN(dt_criacao) FROM read_parquet('{prata_pages}'))
                ) AS dt_min,
                GREATEST(
                    (SELECT MAX(dt_revisao) FROM read_parquet('{prata_revs}')),
                    (SELECT MAX(dt_ultima_revisao) FROM read_parquet('{prata_pages}'))
                ) AS dt_max
        ),
        calendario AS (
            SELECT CAST(gs AS DATE) AS dt_data
            FROM limites, generate_series(dt_min, dt_max, INTERVAL 1 DAY) AS t(gs)
        )
        SELECT
            CAST(STRFTIME(dt_data, '%Y%m%d') AS BIGINT) AS sk_data,
            dt_data,
            CAST(EXTRACT(YEAR FROM dt_data) AS BIGINT) AS nr_ano,
            CAST(EXTRACT(MONTH FROM dt_data) AS BIGINT) AS nr_mes,
            CAST(EXTRACT(DAY FROM dt_data) AS BIGINT) AS nr_dia,
            CAST(((EXTRACT(MONTH FROM dt_data) - 1) // 3) + 1 AS BIGINT) AS nr_trimestre,
            'T' || CAST(((EXTRACT(MONTH FROM dt_data) - 1) // 3) + 1 AS VARCHAR) || '/' || CAST(EXTRACT(YEAR FROM dt_data) AS VARCHAR) AS ds_trimestre,
            CASE EXTRACT(MONTH FROM dt_data)
                WHEN 1 THEN 'Janeiro'   WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Marco'
                WHEN 4 THEN 'Abril'     WHEN 5 THEN 'Maio'      WHEN 6 THEN 'Junho'
                WHEN 7 THEN 'Julho'     WHEN 8 THEN 'Agosto'    WHEN 9 THEN 'Setembro'
                WHEN 10 THEN 'Outubro'  WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
            END AS nm_mes,
            STRFTIME(dt_data, '%Y-%m') AS ds_ano_mes,
            CAST(EXTRACT(ISODOW FROM dt_data) - 1 AS BIGINT) AS nr_dia_semana,
            CASE EXTRACT(ISODOW FROM dt_data)
                WHEN 1 THEN 'Segunda' WHEN 2 THEN 'Terca'  WHEN 3 THEN 'Quarta'
                WHEN 4 THEN 'Quinta'  WHEN 5 THEN 'Sexta'  WHEN 6 THEN 'Sabado'
                WHEN 7 THEN 'Domingo'
            END AS ds_dia_semana,
            (EXTRACT(ISODOW FROM dt_data) >= 6) AS fl_fim_semana
        FROM calendario
        ORDER BY dt_data
    """


def _sql_dim_usuario() -> str:
    prata_users = _prata_file("users")
    return f"""
        SELECT
            CAST(id AS BIGINT) AS sk_usuario,
            name AS nm_usuario,
            CAST(total_revisions AS BIGINT) AS qtd_total_revisoes,
            CAST(pages_created AS BIGINT) AS qtd_paginas_criadas,
            CAST(avg_revisions_per_page AS DOUBLE) AS vl_media_revisoes_pagina,
            CAST(first_edit_at AS TIMESTAMP) AS dt_primeira_edicao,
            CAST(last_edit_at AS TIMESTAMP) AS dt_ultima_edicao,
            tier_atividade AS ds_tier_atividade
        FROM read_parquet('{prata_users}')

        UNION ALL

        SELECT
            CAST(-1 AS BIGINT) AS sk_usuario,
            'Desconhecido' AS nm_usuario,
            CAST(0 AS BIGINT) AS qtd_total_revisoes,
            CAST(0 AS BIGINT) AS qtd_paginas_criadas,
            CAST(NULL AS DOUBLE) AS vl_media_revisoes_pagina,
            CAST(NULL AS TIMESTAMP) AS dt_primeira_edicao,
            CAST(NULL AS TIMESTAMP) AS dt_ultima_edicao,
            'Desconhecido' AS ds_tier_atividade
    """


def _sql_dim_categoria() -> str:
    prata_cats = _prata_file("categories")
    return f"""
        SELECT
            CAST(id AS BIGINT) AS sk_categoria,
            name AS nm_categoria,
            CAST(page_count AS BIGINT) AS qtd_paginas
        FROM read_parquet('{prata_cats}')
    """


def _sql_dim_tipo_referencia() -> str:
    return """
        SELECT CAST(sk_tipo_referencia AS BIGINT) AS sk_tipo_referencia,
               cd_link_type,
               ds_tipo_referencia,
               ds_grupo
        FROM (VALUES
            (1,  'L', 'Link Interno',           'Grafo Wiki'),
            (2,  'W', 'Link Quebrado (Wanted)', 'Grafo Wiki'),
            (3,  'R', 'Redirect',               'Grafo Wiki'),
            (4,  'I', 'Include',                'Grafo Wiki'),
            (5,  'C', 'Categoria',              'Taxonomia'),
            (6,  'F', 'Arquivo',                'Midia'),
            (7,  'E', 'Arquivo/Externo',        'Midia'),
            (-1, '?', 'Outro',                  'Outro')
        ) AS t(sk_tipo_referencia, cd_link_type, ds_tipo_referencia, ds_grupo)
    """


def _sql_dim_pagina() -> str:
    prata_pages = _prata_file("pages")
    prata_refs = _prata_file("wiki_references")
    return f"""
        WITH cat_principal AS (
            SELECT
                page_id,
                MIN(referenced_name) AS nm_categoria_principal
            FROM read_parquet('{prata_refs}')
            WHERE link_type = 'C'
            GROUP BY page_id
        ),
        out_deg AS (
            SELECT
                page_id,
                COUNT(*) AS qtd_links_saida
            FROM read_parquet('{prata_refs}')
            WHERE link_type IN ('L', 'I')
            GROUP BY page_id
        ),
        in_deg AS (
            SELECT
                target_page_id AS page_id,
                COUNT(*) AS qtd_links_entrada
            FROM read_parquet('{prata_refs}')
            WHERE link_type IN ('L', 'I') AND target_page_id IS NOT NULL
            GROUP BY target_page_id
        )
        SELECT
            CAST(p.id AS BIGINT) AS sk_pagina,
            p.name AS nm_pagina,
            COALESCE(c.nm_categoria_principal, 'Sem Categoria') AS nm_categoria_principal,
            CAST(p.revision_count AS BIGINT) AS qtd_revisoes,
            CAST(p.dt_criacao AS DATE) AS dt_criacao,
            CAST(p.dt_ultima_revisao AS DATE) AS dt_ultima_revisao,
            CAST(COALESCE(i.qtd_links_entrada, 0) AS BIGINT) AS qtd_links_entrada,
            CAST(COALESCE(o.qtd_links_saida, 0) AS BIGINT) AS qtd_links_saida,
            p.fl_multi_revisao,
            p.fl_bloqueada
        FROM read_parquet('{prata_pages}') p
        LEFT JOIN cat_principal c ON c.page_id = p.id
        LEFT JOIN in_deg i        ON i.page_id = p.id
        LEFT JOIN out_deg o       ON o.page_id = p.id
    """


def _sql_fato_revisoes() -> str:
    prata_revs = _prata_file("revisions")
    return f"""
        SELECT
            CAST(id AS BIGINT) AS cd_revisao,
            CAST(STRFTIME(dt_revisao, '%Y%m%d') AS BIGINT) AS sk_data,
            CAST(page_id AS BIGINT) AS sk_pagina,
            CAST(COALESCE(author_id, -1) AS BIGINT) AS sk_usuario,
            CAST(hr_revisao AS BIGINT) AS hr_revisao,
            CAST(nr_sequencia_revisao AS BIGINT) AS nr_sequencia_revisao,
            fl_criacao_pagina,
            CAST(content_length AS BIGINT) AS vl_bytes_conteudo,
            CAST(word_count AS BIGINT) AS vl_qtd_palavras,
            CAST(internal_link_count AS BIGINT) AS vl_qtd_links_internos,
            fl_possui_latex,
            fl_conteudo_vazio
        FROM read_parquet('{prata_revs}')
    """


def _sql_fato_referencias() -> str:
    prata_refs = _prata_file("wiki_references")
    return f"""
        SELECT
            CAST(id AS BIGINT) AS cd_referencia,
            CAST(page_id AS BIGINT) AS sk_pagina_origem,
            CAST(target_page_id AS BIGINT) AS sk_pagina_destino,
            CAST(category_id AS BIGINT) AS sk_categoria,
            CAST(
                CASE link_type
                    WHEN 'L' THEN 1
                    WHEN 'W' THEN 2
                    WHEN 'R' THEN 3
                    WHEN 'I' THEN 4
                    WHEN 'C' THEN 5
                    WHEN 'F' THEN 6
                    WHEN 'E' THEN 7
                    ELSE -1
                END
            AS BIGINT) AS sk_tipo_referencia,
            CAST(file_id AS BIGINT) AS cd_arquivo,
            referenced_name AS nm_referenciado,
            fl_resolvido
        FROM read_parquet('{prata_refs}')
    """


ETAPAS_OURO = [
    ("dim_tempo", _sql_dim_tempo),
    ("dim_usuario", _sql_dim_usuario),
    ("dim_categoria", _sql_dim_categoria),
    ("dim_tipo_referencia", _sql_dim_tipo_referencia),
    ("dim_pagina", _sql_dim_pagina),
    ("fato_revisoes", _sql_fato_revisoes),
    ("fato_referencias", _sql_fato_referencias),
]


def executar_ouro(dt_ingestao: str | None = None, tabelas: list[str] | None = None) -> list[dict]:
    """Ponto de entrada da camada Ouro."""
    if dt_ingestao is None:
        dt_ingestao = date.today().isoformat()

    etapas = ETAPAS_OURO
    if tabelas:
        alvo_set = set(tabelas)
        etapas = [(nome, fn) for nome, fn in ETAPAS_OURO if nome in alvo_set]

    log.info("=" * 60)
    log.info(f" OURO — Modelagem dimensional Star Schema ({len(etapas)} tabelas)")
    log.info("=" * 60)

    s3 = get_s3()
    con = get_duckdb_conn(attach_mariadb=False)
    metricas: list[dict] = []

    try:
        for tabela, sql_builder in etapas:
            log.info(f"  > Construindo {tabela}...")
            try:
                linhas = _executar_copy_ouro(con, s3, tabela, sql_builder())
                metricas.append({"tabela": tabela, "linhas": linhas, "status": "ok"})
            except Exception as exc:
                log.error(f"  ERRO em {tabela}: {exc}", exc_info=True)
                metricas.append({"tabela": tabela, "linhas": 0, "status": "erro", "erro": str(exc)})
    finally:
        con.close()

    total = sum(m["linhas"] for m in metricas)
    ok = sum(1 for m in metricas if m["status"] == "ok")
    log.info(f"Resumo Ouro: {ok}/{len(metricas)} tabelas OK · {total:,} linhas totais")
    return metricas
