-- ============================================================
--  verificar.sql — Smoke test do catalogo NLab-DL no Trino
--  Uso: docker exec -i trino trino < platform/sql/verificar.sql
-- ============================================================

SHOW SCHEMAS IN hive;
SHOW TABLES IN hive.bronze;
SHOW TABLES IN hive.prata;
SHOW TABLES IN hive.ouro;

-- Contagens gerais no Star Schema (Ouro)
SELECT 'dim_tempo'           AS tabela, COUNT(*) AS linhas FROM hive.ouro.dim_tempo
UNION ALL
SELECT 'dim_usuario'         AS tabela, COUNT(*) AS linhas FROM hive.ouro.dim_usuario
UNION ALL
SELECT 'dim_categoria'       AS tabela, COUNT(*) AS linhas FROM hive.ouro.dim_categoria
UNION ALL
SELECT 'dim_tipo_referencia' AS tabela, COUNT(*) AS linhas FROM hive.ouro.dim_tipo_referencia
UNION ALL
SELECT 'dim_pagina'          AS tabela, COUNT(*) AS linhas FROM hive.ouro.dim_pagina
UNION ALL
SELECT 'fato_revisoes'       AS tabela, COUNT(*) AS linhas FROM hive.ouro.fato_revisoes
UNION ALL
SELECT 'fato_referencias'    AS tabela, COUNT(*) AS linhas FROM hive.ouro.fato_referencias;

-- Amostra de join na Ouro (Top 5 categorias por paginas)
SELECT nm_categoria, qtd_paginas
FROM hive.ouro.dim_categoria
ORDER BY qtd_paginas DESC
LIMIT 5;

-- Verificacao rapida do catalogo federado MariaDB
SHOW TABLES IN mariadb_fonte.nlab_db;
