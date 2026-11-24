#!/bin/bash
set -e

mkdir -p /tmp/duckdb_tmp

echo "Inicializando banco do Airflow..."
airflow db migrate

echo "Criando usuario admin..."
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@nlab-lakehouse.local 2>/dev/null || echo "Usuario ja existe."

echo "Iniciando scheduler..."
airflow scheduler &

echo "Iniciando webserver..."
exec airflow webserver
