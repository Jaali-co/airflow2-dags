"""DAG Celery : echo + sleep (Airflow 2)."""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="af2_echo_sleep",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "celery", "echo", "airflow2"],
) as dag:
    echo = BashOperator(
        task_id="echo_hello",
        bash_command='echo "hello from airflow2 celery $(date -u +%Y-%m-%dT%H:%M:%SZ)"',
        queue="default",
    )
    nap = BashOperator(
        task_id="sleep_5s",
        bash_command="echo sleeping && sleep 5 && echo done",
        queue="default",
    )
    echo >> nap
