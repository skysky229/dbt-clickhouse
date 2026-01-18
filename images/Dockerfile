FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/usr/app/dbt_project/config

RUN pip install --no-cache-dir \
    dbt-core==1.11.2 \
    dbt-clickhouse==1.9.8 \
    "apache-airflow-providers-cncf-kubernetes>=8.3.0"

COPY dbt/ /usr/app/dbt_project
COPY dbt/config/profiles.yml /root/.dbt/profiles.yml
WORKDIR /usr/app/dbt_project

ENTRYPOINT ["dbt"]