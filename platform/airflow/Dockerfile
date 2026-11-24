FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc libpq-dev \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /tmp/duckdb_tmp \
 && chown -R airflow:0 /tmp/duckdb_tmp

COPY start.sh /opt/airflow/start.sh
RUN sed -i 's/\r$//' /opt/airflow/start.sh && chmod +x /opt/airflow/start.sh

USER airflow
COPY requirements.txt /tmp/airflow-requirements.txt

# Nao atualizar SQLAlchemy — Airflow 2.10.x exige SQLAlchemy 1.4.x internamente
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt

# Pre-instala extensoes DuckDB (httpfs para S3/Silo e mysql para MariaDB)
RUN python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs; INSTALL mysql;'); con.close()"

ENTRYPOINT ["/opt/airflow/start.sh"]
