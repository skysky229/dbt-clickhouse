FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-core dbt-clickhouse

RUN pip install "apache-airflow-providers-cncf-kubernetes>=8.3.0"

COPY dbt/ /usr/app/dbt_project
COPY dbt/config/profiles.yml /root/.dbt/profiles.yml
WORKDIR /usr/app/dbt_project

ENTRYPOINT ["dbt"]