"""
pipeline_nlab.py — DAG do Airflow (NLab-DL)
============================================
Orquestra o pipeline Medallion completo do nLab:
    ingestao_bronze -> transformacao_prata -> modelagem_ouro -> registrar_metastore

Suporte a parametros via Trigger DAG w/ config (JSON opcional):
    {
      "dt_ingestao": "2026-11-23",
      "tabelas": ["webs", "users", "categories"],
      "recreate_hms": false
    }
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

if "/opt/airflow/pipeline" not in sys.path:
    sys.path.insert(0, "/opt/airflow/pipeline")

log = logging.getLogger(__name__)

default_args = {
    "owner": "nlab-data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _get_conf(kwargs: dict, default_ds: str) -> tuple[str, list[str] | None, bool]:
    dag_run = kwargs.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
    dt_ingestao = conf.get("dt_ingestao") or default_ds
    tabelas = conf.get("tabelas")
    recreate_hms = bool(conf.get("recreate_hms", False))
    return dt_ingestao, tabelas, recreate_hms


def task_bronze(ds: str, **kwargs):
    """Ingere as 7 tabelas do MariaDB (nlab_db) para Bronze no Silo."""
    from bronze import executar_bronze

    dt_ingestao, tabelas, _ = _get_conf(kwargs, ds)
    log.info(f"Iniciando ingestao Bronze para dt_ingestao={dt_ingestao}")
    metricas = executar_bronze(dt_ingestao=dt_ingestao, tabelas=tabelas)
    erros = [m for m in metricas if m["status"] != "ok"]
    if erros:
        raise ValueError(f"Bronze com erros: {erros}")
    total = sum(m["linhas"] for m in metricas)
    log.info(f"Bronze OK — {total:,} linhas ingeridas")
    return {"linhas": total, "tabelas": len(metricas), "dt_ingestao": dt_ingestao}


def task_prata(ds: str, **kwargs):
    """Limpa, tipa e extrai metricas de texto da Bronze para Prata via DuckDB."""
    from prata import executar_prata

    dt_ingestao, tabelas, _ = _get_conf(kwargs, ds)
    log.info(f"Iniciando transformacao Prata para dt_ingestao={dt_ingestao}")
    metricas = executar_prata(dt_ingestao=dt_ingestao, tabelas=tabelas)
    erros = [m for m in metricas if m["status"] != "ok"]
    if erros:
        raise ValueError(f"Prata com erros: {erros}")
    total = sum(m["linhas"] for m in metricas)
    log.info(f"Prata OK — {total:,} linhas transformadas")
    return {"linhas": total, "tabelas": len(metricas)}


def task_ouro(ds: str, **kwargs):
    """Constroi o Star Schema dimensional do nLab na camada Ouro via DuckDB."""
    from ouro import executar_ouro

    dt_ingestao, _, _ = _get_conf(kwargs, ds)
    log.info(f"Iniciando modelagem Ouro (ref dt_ingestao={dt_ingestao})")
    metricas = executar_ouro(dt_ingestao=dt_ingestao)
    erros = [m for m in metricas if m["status"] != "ok"]
    if erros:
        raise ValueError(f"Ouro com erros: {erros}")
    total = sum(m["linhas"] for m in metricas)
    log.info(f"Ouro OK — {len(metricas)} tabelas ({total:,} linhas)")
    return {"tabelas": len(metricas), "linhas": total}


def task_registrar_hms(ds: str, **kwargs):
    """Registra tabelas e particoes no Hive Metastore via Thrift."""
    from registrar_tabelas import main as registrar

    dt_ingestao, _, recreate_hms = _get_conf(kwargs, ds)
    log.info(f"Registrando catalogo HMS para dt_ingestao={dt_ingestao} (recreate={recreate_hms})")
    registrar(dt_ingestao=dt_ingestao, recreate=recreate_hms)
    log.info("HMS OK — catalogo e particoes atualizados")


with DAG(
    dag_id="pipeline_nlab",
    description="Pipeline Medallion NLab-DL: MariaDB -> Bronze -> Prata -> Ouro -> HMS",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nlab", "medallion", "duckdb", "lakehouse"],
    doc_md="""
## Pipeline NLab-DL Lakehouse

Pipeline Medallion para o banco relacional **nLab** (`nlab_db` em MariaDB 11):

| Camada | O que faz |
| :--- | :--- |
| **Bronze** | Extracao raw paginada por faixa de ID (`MariaDB -> Parquet Snappy` no Silo, particionada por `dt_ingestao`) |
| **Prata** | Limpeza, tipagem estrita, extracao de metricas do `LONGTEXT` (`revisions.content`) e resolucao complementar de referencias via DuckDB |
| **Ouro** | Star Schema dimensional (`dim_tempo`, `dim_usuario`, `dim_categoria`, `dim_tipo_referencia`, `dim_pagina`, `fato_revisoes`, `fato_referencias`) |
| **HMS** | Registro idempotente das tabelas externas e particoes no Hive Metastore para consumo no Trino e Superset |
    """,
) as dag:

    ingestao_bronze = PythonOperator(
        task_id="ingestao_bronze",
        python_callable=task_bronze,
        doc_md="Extrai as 7 tabelas do MariaDB em chunks Parquet para s3://bronze/",
    )

    transformacao_prata = PythonOperator(
        task_id="transformacao_prata",
        python_callable=task_prata,
        doc_md="Processa Bronze -> Prata com DuckDB (out-of-core, sem carregar LONGTEXT na Prata)",
    )

    modelagem_ouro = PythonOperator(
        task_id="modelagem_ouro",
        python_callable=task_ouro,
        doc_md="Constroi o Star Schema dimensional do nLab em s3://ouro/",
    )

    registrar_metastore = PythonOperator(
        task_id="registrar_metastore",
        python_callable=task_registrar_hms,
        doc_md="Registra tabelas e particoes no Hive Metastore e sincroniza o Trino",
    )

    ingestao_bronze >> transformacao_prata >> modelagem_ouro >> registrar_metastore
