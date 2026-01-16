from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Variable JSON for dbt. 
# We use Airflow templates:
# {{ ds }} = Execution Date (YYYY-MM-DD)
# {{ data_interval_start }} = Start of the schedule interval (ISO 8601)
# {{ data_interval_end }} = End of the schedule interval (ISO 8601)
# Note: We wrap the JSON in single quotes for the command line.
DBT_VARS = "'{\"start_date\": \"{{ data_interval_start }}\", \"end_date\": \"{{ data_interval_end }}\", \"job_date\": \"{{ ds }}\"}'"

ch_user_secret = k8s.V1SecretEnvSource(name='clickhouse-creds')

with DAG(
    'dbt_consumer_360_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['dbt', 'clickhouse', 'consumer_360'],
) as dag:

    # 1. Snapshot Task: Capture changes in order statuses
    dbt_snapshot = KubernetesPodOperator(
        task_id='dbt_snapshot',
        name='dbt-snapshot-orders',
        namespace='dbt-clickhouse',
        image='skysky229/dbt-clickhouse:latest',
        env_from=[k8s.V1EnvFromSource(secret_ref=ch_user_secret)],
        # Run only the specific snapshot
        arguments=['snapshot', '--select', 'orders_snapshot'],
        get_logs=True
    )

    # # 2. Intermediate Task: Calculate daily metrics
    # # We select both intermediate models. 
    # # They will run with the variables defined above.
    # dbt_run_intermediate = KubernetesPodOperator(
    #     task_id='dbt_run_intermediate',
    #     name='dbt-run-intermediate',
    #     namespace='dbt-clickhouse',
    #     image='skysky229/dbt-clickhouse:latest',
    #     env_from=[k8s.V1EnvFromSource(secret_ref=ch_user_secret)],
    #     arguments=[
    #         'run', 
    #         '--select', 'int_daily_user_orders int_daily_user_events',
    #         '--vars', DBT_VARS
    #     ],
    #     get_logs=True,
    #     on_finish_action='delete_pod',
    # )

    # # 3. Final Task: Aggregate Behavior Features
    # dbt_run_features = KubernetesPodOperator(
    #     task_id='dbt_run_features',
    #     name='dbt-run-features',
    #     namespace='dbt-clickhouse',
    #     image='skysky229/dbt-clickhouse:latest',
    #     env_from=[k8s.V1EnvFromSource(secret_ref=ch_user_secret)],
    #     arguments=[
    #         'run', 
    #         '--select', 'behavior_features',
    #         '--vars', DBT_VARS
    #     ],
    #     get_logs=True,
    #     on_finish_action='delete_pod',
    # )

    # # Define Dependencies
    # dbt_snapshot >> dbt_run_intermediate >> dbt_run_features
