# NLab-DL — nLab MariaDB Source & Data Lake Foundation

This repository provides a clean, reproducible setup to rebuild the complete **nLab MariaDB relational database** (real wiki content + full git revision history + relational enrichment + optional 1,000,000-page scaling), organized as the OLTP source foundation for a **Data Lake boilerplate**.

---

## Repository Structure

```text
NLab-DL/
├── .env.example                          # Default MariaDB & pipeline configuration
├── .gitignore
├── docker-compose.yml                    # MariaDB service (ready to extend with Data Lake services)
├── requirements.txt                      # Python dependencies
├── docs/
│   └── er-diagram.png                    # Final 7-table enriched ER diagram
├── data/
│   └── dumps/
│       ├── .gitkeep                      # Place SQL dumps (dump_completo.sql) here
│       └── dump_completo.sql             # (gitignored) Full 1M-page MariaDB dump
└── database/
    ├── sql/
    │   └── 01_schema.sql                 # Base 5-table MariaDB DDL (for rebuild from scratch)
    └── pipeline/
        ├── config.py                     # Shared .env / CLI config & DB connection helper
        ├── restore_dump.py               # Restores dump_completo.sql into MariaDB + verifies counts
        ├── rebuild_db.py                 # End-to-end orchestrator (--from-dump or full ETL rebuild)
        ├── 00_fetch_source.py            # Clones ncatlab/nlab-content & generates full_log.txt
        ├── 01_etl_base.py                # Loads webs, pages, wiki_references (L/W/R/I)
        ├── 02_etl_revisions.py           # Reconstructs full chronological git revision history
        ├── 03_mock_wiki_files.py         # Populates wiki_files from earliest citation in revisions
        ├── 04_etl_extra_reftypes.py      # Extracts C (categories) & F/E (file links)
        ├── 05_enrich_model.py            # Normalizes users & categories, adds metrics & 8 FKs
        ├── 06_fix_temporal_meta.py       # Optional: mocks coherent monotonic timestamps
        ├── 07_generate_1m_pages.py       # Scales database to 1,000,000 pages with full integrity
        └── check_cardinality.py          # Prints exact row counts for all tables
```

---

## Final Relational Schema (7 Tables)

The base DDL (`database/sql/01_schema.sql`) creates the 5 core wiki tables (`webs`, `pages`, `revisions`, `wiki_files`, `wiki_references`). Step 5 (`05_enrich_model.py`) normalizes `users` and `categories`, backfills `wiki_files.file_type`, adds denormalized page metrics, resolves polymorphic references, and enforces 8 InnoDB foreign keys:

![nLab MariaDB ER Diagram](docs/er-diagram.png)

| Table | Role |
| :--- | :--- |
| `webs` | Root wiki instance (`nLab`) |
| `pages` | Wiki pages with denormalized `revision_count`, `first_revised_at`, `last_revised_at` |
| `revisions` | Full chronological edit history (`content`, `revised_at`, `author_id -> users.id`) |
| `users` | Normalized authors with `total_revisions`, `pages_created`, `first_edit_at`, `last_edit_at` |
| `categories` | Normalized page categories (`page_count`) |
| `wiki_files` | Referenced media/files (`file_name`, `file_type`, `description`) |
| `wiki_references` | Wiki link graph (`L`, `W`, `R`, `I`, `C`, `F`, `E`) with resolved nullable FKs (`target_page_id`, `category_id`, `file_id`) |

---

## Prerequisites

- **Docker** & **Docker Compose**
- **Python 3.10+**
- **Git** (needed only for Option A: full rebuild from `ncatlab/nlab-content`)

---

## Quickstart

### 1. Configure environment & Python virtualenv

```bash
cp .env.example .env

python -m venv .venv
# Linux / macOS:
source .venv/bin/activate
# Windows (PowerShell):
# .venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

### 2. Start MariaDB (loads `dump_completo.sql` automatically)

Place `dump_completo.sql` in `data/dumps/dump_completo.sql` and start the container:

```bash
docker compose up -d mariadb
```

On first startup (empty `mariadb_data` volume), MariaDB automatically imports `data/dumps/dump_completo.sql` (1,000,000 pages, 1,536,920 revisions, 5,931,831 references, all 7 tables and 8 FKs). Follow progress with:

```bash
docker logs -f nlab_mariadb
```

---

## Option A: Restore from `dump_completo.sql` via Pipeline (Running Container)

If MariaDB is already running and you want to (re)load the dump manually with live progress and automatic row-count verification:

```bash
# Stream data/dumps/dump_completo.sql into nlab_mariadb and verify counts
python database/pipeline/restore_dump.py --recreate-db

# Or via the unified orchestrator:
python database/pipeline/rebuild_db.py --from-dump --reset
```

---

## Option B: Full Rebuild from Scratch (Upstream `nlab-content`)

If you don't have `dump_completo.sql` and want to rebuild the entire database from `ncatlab/nlab-content`:

1. In `docker-compose.yml`, swap the init mount from `dump_completo.sql` to `database/sql/01_schema.sql` and run `docker compose down -v && docker compose up -d mariadb`.
2. Run the end-to-end orchestrator:

```bash
# Complete rebuild (real nLab data + full git history + enrichment + 1M pages)
python database/pipeline/rebuild_db.py
```

Useful modes:

```bash
# Real nLab dataset only (~20.7k pages, ~206k revisions), without 1M synthetic scaling
python database/pipeline/rebuild_db.py --skip-scale

# Fast dev mode (snapshot revisions only, no git history walk, no 1M scaling)
python database/pipeline/rebuild_db.py --snapshot-only --skip-scale

# Custom scale target (e.g. 100k pages)
python database/pipeline/rebuild_db.py --target-pages 100000
```

You can also run any step individually (all scripts accept `--host`, `--port`, `--user`, `--password`, `--database`, and read defaults from `.env`):

```bash
python database/pipeline/00_fetch_source.py
python database/pipeline/01_etl_base.py --skip-revisions
python database/pipeline/02_etl_revisions.py
python database/pipeline/03_mock_wiki_files.py
python database/pipeline/04_etl_extra_reftypes.py
python database/pipeline/05_enrich_model.py
python database/pipeline/07_generate_1m_pages.py --target 1000000
```

---

## Verification

Check exact row counts across all tables:

```bash
python database/pipeline/check_cardinality.py
```

---

## Next Steps (Data Lake Boilerplate)

With the MariaDB OLTP source isolated under `database/`, this repo is ready to be extended with Data Lake components (for example: object storage like MinIO, ingestion/CDC, Bronze/Silver/Gold layers, Spark/DuckDB/Trino, and orchestration) by adding new services to `docker-compose.yml` and modules at the repository root.
