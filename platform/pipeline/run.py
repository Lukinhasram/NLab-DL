"""
run.py — Orquestrador CLI do pipeline NLab-DL
==============================================
Executa as camadas Medallion e o registro no Hive Metastore em sequencia:
    1. Bronze -> ingestao raw paginada do MariaDB (nlab_db) para o Silo
    2. Prata  -> limpeza, metricas de texto e enriquecimento via DuckDB
    3. Ouro   -> Star Schema dimensional do nLab via DuckDB
    4. HMS    -> registro das tabelas e particoes no Hive Metastore

Uso:
    python platform/pipeline/run.py
    python platform/pipeline/run.py --camada bronze --tabelas webs,users,categories
    python platform/pipeline/run.py --camada todas --data 2026-11-23
    python platform/pipeline/run.py --camada hms --recreate-hms
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

_PIPELINE_DIR = str(Path(__file__).resolve().parent)
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

from bronze import executar_bronze
from ouro import executar_ouro
from prata import executar_prata
from registrar_tabelas import main as registrar_hms

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def imprimir_resumo(metricas: list[dict], camada: str, elapsed: float) -> None:
    ok = sum(1 for m in metricas if m["status"] == "ok")
    total = sum(m["linhas"] for m in metricas)
    erros = [m for m in metricas if m["status"] != "ok"]

    log.info("-" * 55)
    log.info(f"  {camada.upper()} concluido em {elapsed:.1f}s")
    log.info(f"  Tabelas OK: {ok}/{len(metricas)}")
    log.info(f"  Linhas:     {total:,}")
    if erros:
        for e in erros:
            log.warning(f"  AVISO {e['tabela']}: {e.get('erro', e['status'])}")
    log.info("-" * 55)


def executar_pipeline(
    camada: str = "todas",
    dt_ingestao: str | None = None,
    tabelas: list[str] | None = None,
    recreate_hms: bool = False,
) -> None:
    if dt_ingestao is None:
        dt_ingestao = date.today().isoformat()

    log.info("=" * 60)
    log.info("  Pipeline NLab-DL Lakehouse (MariaDB -> Silo -> Trino)")
    log.info(f"  Data de ingestao: {dt_ingestao}")
    log.info(f"  Camada:           {camada}")
    if tabelas:
        log.info(f"  Tabelas filtro:   {', '.join(tabelas)}")
    log.info("=" * 60)

    inicio_total = time.time()

    if camada in ("todas", "bronze"):
        t = time.time()
        m = executar_bronze(dt_ingestao=dt_ingestao, tabelas=tabelas)
        imprimir_resumo(m, "bronze", time.time() - t)

    if camada in ("todas", "prata"):
        t = time.time()
        m = executar_prata(dt_ingestao=dt_ingestao, tabelas=tabelas)
        imprimir_resumo(m, "prata", time.time() - t)

    if camada in ("todas", "ouro"):
        t = time.time()
        m = executar_ouro(dt_ingestao=dt_ingestao, tabelas=tabelas)
        imprimir_resumo(m, "ouro", time.time() - t)

    if camada in ("todas", "hms"):
        t = time.time()
        registrar_hms(dt_ingestao=dt_ingestao, recreate=recreate_hms)
        log.info(f"  HMS concluido em {time.time() - t:.1f}s")

    total = time.time() - inicio_total
    log.info(f"Pipeline concluido em {total:.1f}s")
    log.info("Proximos passos:")
    log.info("  1. Verificar no Trino: docker exec -i trino trino < platform/sql/verificar.sql")
    log.info("  2. Silo Console:       http://localhost:9001")
    log.info("  3. Trino Web UI:       http://localhost:8080")
    log.info("  4. Superset:           http://localhost:8088 (admin/admin)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline NLab-DL Lakehouse")
    parser.add_argument(
        "--camada",
        choices=["todas", "bronze", "prata", "ouro", "hms"],
        default="todas",
        help="Camada a executar (default: todas)",
    )
    parser.add_argument(
        "--data",
        default=date.today().isoformat(),
        help="Data de ingestao YYYY-MM-DD (default: hoje)",
    )
    parser.add_argument(
        "--tabelas",
        default=None,
        help="Lista opcional de tabelas separadas por virgula (ex: webs,users,pages)",
    )
    parser.add_argument(
        "--recreate-hms",
        action="store_true",
        help="Recria metadados das tabelas no Hive Metastore",
    )
    args = parser.parse_args()
    lista_tabelas = [t.strip() for t in args.tabelas.split(",") if t.strip()] if args.tabelas else None
    executar_pipeline(
        camada=args.camada,
        dt_ingestao=args.data,
        tabelas=lista_tabelas,
        recreate_hms=args.recreate_hms,
    )
