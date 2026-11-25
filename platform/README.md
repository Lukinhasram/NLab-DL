# Plataforma de Dados — NLab-DL Lakehouse

Arquitetura Medallion integrada ao banco relacional `nlab_db` (MariaDB 11), usando **Silo (`pgsty/silo`)** no lugar do MinIO e **DuckDB** dentro do Airflow para processar milhões de registros dentro de um host com até **16 GB de RAM**.

```text
MariaDB 11 (nlab_db — 7 tabelas, ~4.88 GB)
      ↓ Airflow DAG (pipeline_nlab) + DuckDB (mysql + httpfs, paginado por ID)
  Bronze (Silo S3 — Parquet Snappy raw, particionado por dt_ingestao)
      ↓ DuckDB SQL (streaming/out-of-core + métricas de texto do LONGTEXT)
  Prata  (Silo S3 — tipado, normalizado e enriquecido)
      ↓ DuckDB SQL (Star Schema dimensional)
  Ouro   (Silo S3 — 5 dimensões + 2 fatos)
      ↓ Hive Metastore (MariaDB 10.5 isolado)
  Trino  (queries SQL sobre Silo + federadas com MariaDB)
      ↓
  Superset (dashboards e SQL Lab)
```

## Comandos principais (executar na raiz do repositório)

```bash
# 0. Criar .env a partir do exemplo
cp .env.example .env

# 1. Build das imagens customizadas (Airflow+DuckDB e Superset+Trino)
docker compose build airflow superset

# 2. Subir toda a stack unificada
docker compose up -d

# 3. Acompanhar saúde e consumo de RAM dos serviços
docker compose ps
docker stats --no-stream
docker logs -f nlab_mariadb
```

> **Base padrão leve incluída no Git (`data/dumps/dump_dev.sql.gz`, ~29 MB):** já vem montada por padrão no `docker-compose.yml` com todas as 7 tabelas e 8 FKs (4.706 páginas reais+sintéticas, 17.114 revisões, 80.096 referências). Sobe o MariaDB em **< 15s** e roda o pipeline Medallion completo em **~80s**. Para usar o dump completo de 1M páginas (4,88 GB), basta descomentar a linha do `dump_completo.sql` no `docker-compose.yml`.
>
> **Nota para Windows/WSL2 (16 GB de RAM):** os limites padrão (`DUCKDB_MEMORY_LIMIT=1000MB`, `revisions` em lotes de `5.000` IDs, `Trino -Xmx1536m`, `MariaDB 512M` buffer pool) foram calibrados para rodar dentro dos 8 GB padrão do WSL2. Se tiver mais RAM disponível, pode aumentar via `.env` ou `.wslconfig`.

## Execução do pipeline

* **Via Airflow UI (`http://localhost:8082`):** login `admin` / `admin` → ativar e disparar a DAG **`pipeline_nlab`**.
* **Via CLI dentro do container:**
  ```bash
  # Pipeline completo (Bronze -> Prata -> Ouro -> HMS)
  docker exec -it airflow python /opt/airflow/pipeline/run.py

  # Apenas tabelas pequenas para teste rápido
  docker exec -it airflow python /opt/airflow/pipeline/run.py --camada bronze --tabelas webs,users,categories
  ```

## Validação no Trino

```bash
docker exec -i trino trino < platform/sql/verificar.sql
```

## Interfaces e Credenciais

| Serviço | URL | Usuário / Senha |
| :--- | :--- | :--- |
| **Silo Console (S3)** | `http://localhost:9001` | `siloadmin` / `siloadmin123` |
| **Airflow UI** | `http://localhost:8082` | `admin` / `admin` |
| **Trino Web UI** | `http://localhost:8080` | *(sem senha)* |
| **Superset** | `http://localhost:8088` | `admin` / `admin` |

### Conectar o Superset ao Trino
No Superset: **Settings → Database Connections → + Database → Trino** e informe a SQLAlchemy URI:

```text
trino://trino@trino:8080/hive
```
