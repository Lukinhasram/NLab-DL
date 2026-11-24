-- ============================================================
--  consultas_analiticas.sql — NLab-DL (Trino CLI & Superset)
--
--  Use no Superset via: SQL Lab -> New Query
--  Ou no CLI: docker exec -it trino trino
-- ============================================================


-- ══════════════════════════════════════════════════════════════
--  1. VISAO GERAL — KPIs executivos do nLab
-- ══════════════════════════════════════════════════════════════
SELECT
    (SELECT COUNT(*) FROM hive.ouro.dim_pagina)                    AS total_paginas,
    COUNT(*)                                                       AS total_revisoes,
    COUNT(DISTINCT CASE WHEN sk_usuario > 0 THEN sk_usuario END)   AS autores_ativos,
    (SELECT COUNT(*) FROM hive.ouro.dim_categoria)                 AS total_categorias,
    (SELECT COUNT(*) FROM hive.ouro.fato_referencias)              AS total_referencias,
    ROUND(AVG(vl_qtd_palavras), 1)                                 AS media_palavras_revisao,
    ROUND(AVG(CASE WHEN fl_possui_latex THEN 100.0 ELSE 0.0 END), 1) AS pct_revisoes_com_latex
FROM hive.ouro.fato_revisoes;


-- ══════════════════════════════════════════════════════════════
--  2. EVOLUCAO ANUAL DE EDICOES E CRIACAO DE PAGINAS
-- ══════════════════════════════════════════════════════════════
SELECT
    t.nr_ano                                                       AS ano,
    COUNT(*)                                                       AS total_revisoes,
    SUM(CASE WHEN f.fl_criacao_pagina THEN 1 ELSE 0 END)           AS novas_paginas,
    COUNT(DISTINCT CASE WHEN f.sk_usuario > 0 THEN f.sk_usuario END) AS autores_distintos,
    ROUND(AVG(f.vl_qtd_palavras), 1)                               AS media_palavras
FROM hive.ouro.fato_revisoes f
JOIN hive.ouro.dim_tempo t ON f.sk_data = t.sk_data
GROUP BY t.nr_ano
ORDER BY t.nr_ano;


-- ══════════════════════════════════════════════════════════════
--  3. SERIE MENSAL DE ATIVIDADE EDITORIAL (linha temporal)
-- ══════════════════════════════════════════════════════════════
SELECT
    t.ds_ano_mes                                                   AS ano_mes,
    t.nr_ano                                                       AS ano,
    t.nm_mes                                                       AS mes,
    COUNT(*)                                                       AS revisoes,
    SUM(CASE WHEN f.fl_criacao_pagina THEN 1 ELSE 0 END)           AS paginas_criadas,
    SUM(f.vl_bytes_conteudo)                                       AS total_bytes_editados
FROM hive.ouro.fato_revisoes f
JOIN hive.ouro.dim_tempo t ON f.sk_data = t.sk_data
GROUP BY t.ds_ano_mes, t.nr_ano, t.nr_mes, t.nm_mes
ORDER BY t.nr_ano, t.nr_mes;


-- ══════════════════════════════════════════════════════════════
--  4. TOP 20 AUTORES POR REVISOES E VOLUME ESCRITO
-- ══════════════════════════════════════════════════════════════
SELECT
    u.nm_usuario                                                   AS autor,
    u.ds_tier_atividade                                            AS tier,
    COUNT(*)                                                       AS total_revisoes,
    SUM(CASE WHEN f.fl_criacao_pagina THEN 1 ELSE 0 END)           AS paginas_criadas,
    ROUND(AVG(f.vl_qtd_palavras), 1)                               AS media_palavras,
    ROUND(AVG(f.vl_qtd_links_internos), 1)                         AS media_links_internos
FROM hive.ouro.fato_revisoes f
JOIN hive.ouro.dim_usuario u ON f.sk_usuario = u.sk_usuario
WHERE u.sk_usuario > 0
GROUP BY u.nm_usuario, u.ds_tier_atividade
ORDER BY total_revisoes DESC
LIMIT 20;


-- ══════════════════════════════════════════════════════════════
--  5. DISTRIBUICAO DE AUTORES POR TIER DE ATIVIDADE
-- ══════════════════════════════════════════════════════════════
SELECT
    ds_tier_atividade                                              AS tier,
    COUNT(*)                                                       AS qtd_autores,
    SUM(qtd_total_revisoes)                                        AS total_revisoes,
    SUM(qtd_paginas_criadas)                                       AS total_paginas_criadas,
    ROUND(SUM(qtd_total_revisoes) * 100.0 /
          SUM(SUM(qtd_total_revisoes)) OVER (), 2)                 AS share_revisoes_pct
FROM hive.ouro.dim_usuario
WHERE sk_usuario > 0
GROUP BY ds_tier_atividade
ORDER BY total_revisoes DESC;


-- ══════════════════════════════════════════════════════════════
--  6. TOP 20 CATEGORIAS POR VOLUME DE PAGINAS E REVISOES
-- ══════════════════════════════════════════════════════════════
SELECT
    p.nm_categoria_principal                                       AS categoria,
    COUNT(DISTINCT p.sk_pagina)                                    AS total_paginas,
    SUM(p.qtd_revisoes)                                            AS total_revisoes,
    ROUND(AVG(p.qtd_revisoes), 2)                                  AS media_revisoes_pagina,
    SUM(p.qtd_links_entrada)                                       AS total_citacoes_recebidas
FROM hive.ouro.dim_pagina p
GROUP BY p.nm_categoria_principal
ORDER BY total_paginas DESC
LIMIT 20;


-- ══════════════════════════════════════════════════════════════
--  7. MIX DE TIPOS DE REFERENCIA E TAXA DE RESOLUCAO
-- ══════════════════════════════════════════════════════════════
SELECT
    tr.cd_link_type                                                AS codigo,
    tr.ds_tipo_referencia                                          AS tipo_referencia,
    tr.ds_grupo                                                    AS grupo,
    COUNT(*)                                                       AS total_referencias,
    SUM(CASE WHEN f.fl_resolvido THEN 1 ELSE 0 END)                AS referencias_resolvidas,
    ROUND(SUM(CASE WHEN f.fl_resolvido THEN 100.0 ELSE 0.0 END) /
          COUNT(*), 2)                                             AS taxa_resolucao_pct
FROM hive.ouro.fato_referencias f
JOIN hive.ouro.dim_tipo_referencia tr ON f.sk_tipo_referencia = tr.sk_tipo_referencia
GROUP BY tr.cd_link_type, tr.ds_tipo_referencia, tr.ds_grupo
ORDER BY total_referencias DESC;


-- ══════════════════════════════════════════════════════════════
--  8. GRAFO DO CONHECIMENTO — TOP 20 PAGINAS MAIS CITADAS (IN-DEGREE)
-- ══════════════════════════════════════════════════════════════
SELECT
    nm_pagina,
    nm_categoria_principal                                         AS categoria,
    qtd_links_entrada                                              AS in_degree,
    qtd_links_saida                                                AS out_degree,
    qtd_revisoes
FROM hive.ouro.dim_pagina
ORDER BY qtd_links_entrada DESC
LIMIT 20;


-- ══════════════════════════════════════════════════════════════
--  9. TOP 20 LINKS QUEBRADOS / PAGINAS DESEJADAS (WANTED - 'W')
-- ══════════════════════════════════════════════════════════════
SELECT
    f.nm_referenciado                                              AS pagina_desejada,
    COUNT(*)                                                       AS qtd_citacoes,
    COUNT(DISTINCT f.sk_pagina_origem)                             AS paginas_que_citam
FROM hive.ouro.fato_referencias f
JOIN hive.ouro.dim_tipo_referencia tr ON f.sk_tipo_referencia = tr.sk_tipo_referencia
WHERE tr.cd_link_type = 'W'
GROUP BY f.nm_referenciado
ORDER BY qtd_citacoes DESC
LIMIT 20;


-- ══════════════════════════════════════════════════════════════
--  10. PADRAO DE EDICAO POR DIA DA SEMANA E HORA (HEATMAP)
-- ══════════════════════════════════════════════════════════════
SELECT
    t.nr_dia_semana,
    t.ds_dia_semana                                                AS dia_semana,
    f.hr_revisao                                                   AS hora_utc,
    COUNT(*)                                                       AS qtd_revisoes
FROM hive.ouro.fato_revisoes f
JOIN hive.ouro.dim_tempo t ON f.sk_data = t.sk_data
GROUP BY t.nr_dia_semana, t.ds_dia_semana, f.hr_revisao
ORDER BY t.nr_dia_semana, f.hr_revisao;


-- ══════════════════════════════════════════════════════════════
--  11. QUERY FEDERADA — OURO (SILO) + WIKITEXT BRUTO (MARIADB)
--      Busca o trecho inicial do texto das 10 maiores revisoes
-- ══════════════════════════════════════════════════════════════
WITH top_revs AS (
    SELECT
        f.cd_revisao,
        p.nm_pagina,
        u.nm_usuario,
        f.vl_bytes_conteudo,
        f.vl_qtd_palavras
    FROM hive.ouro.fato_revisoes f
    JOIN hive.ouro.dim_pagina p  ON f.sk_pagina = p.sk_pagina
    JOIN hive.ouro.dim_usuario u ON f.sk_usuario = u.sk_usuario
    ORDER BY f.vl_bytes_conteudo DESC
    LIMIT 10
)
SELECT
    tr.cd_revisao,
    tr.nm_pagina,
    tr.nm_usuario,
    tr.vl_bytes_conteudo,
    tr.vl_qtd_palavras,
    SUBSTR(r.content, 1, 200)                                      AS preview_wikitext
FROM top_revs tr
JOIN mariadb_fonte.nlab_db.revisions r ON r.id = tr.cd_revisao
ORDER BY tr.vl_bytes_conteudo DESC;
