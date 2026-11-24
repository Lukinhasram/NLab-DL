FROM apache/superset:3.1.0

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc python3-dev \
 && rm -rf /var/lib/apt/lists/*

USER superset

# Driver do Trino para SQLAlchemy
RUN pip install --no-cache-dir \
    sqlalchemy-trino \
    trino
