"""DAG KubernetesPodOperator : python echo (Airflow 2)."""
from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    dag_id="af2_python_k8s",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "kubernetes", "python", "airflow2"],
) as dag:
    KubernetesPodOperator(
        task_id="python_echo",
        name="af2-python-echo",
        namespace="airflow2-python-jobs",
        image="python:3.11-slim",
        cmds=["python", "-c"],
        arguments=[
            "import time; print('hello from airflow2-python-jobs'); time.sleep(3); print('done')"
        ],
        service_account_name="airflow-task",
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        queue="default",
    )
