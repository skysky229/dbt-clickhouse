from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

ch_user_secret = k8s.V1SecretEnvSource(name='clickhouse-creds')

with DAG(
    'dbt_clickhouse_stg_users',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'clickhouse'],
) as dag:

    run_stg_users = KubernetesPodOperator(
        task_id='run_stg_users',
        name='run-stg-users',
        namespace='default',
        image='skysky229/dbt-clickhouse:latest',
        env_from=[k8s.V1EnvFromSource(secret_ref=ch_user_secret)],
        image_pull_policy='IfNotPresent',
        arguments=['run', '--select', 'stg_users'],
        is_delete_operator_pod=True,
        get_logs=True,
    )
