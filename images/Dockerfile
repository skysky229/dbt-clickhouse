FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-core dbt-clickhouse

COPY ../dbt/ /usr/app/dbt_project
WORKDIR /usr/app/dbt_project

ENTRYPOINT ["dbt"]