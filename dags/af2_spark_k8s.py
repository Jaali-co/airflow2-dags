"""DAG Spark via SparkK8sOperator (package vigie-spark-operator)."""
from datetime import datetime

from airflow import DAG

from vigie_spark_operator import SparkK8sOperator

with DAG(
    dag_id="af2_spark_k8s",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "spark", "airflow2"],
) as dag:
    SparkK8sOperator(
        task_id="spark_pi",
        name="af2-pi",
        namespace="airflow2-spark-jobs",
        image="spark:3.5.1",
        application_type="Scala",
        main_class="org.apache.spark.examples.SparkPi",
        main_application_file="local:///opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar",
        arguments=["10"],
        spark_version="3.5.1",
        driver_cores=1,
        driver_cores_request="100m",
        driver_memory="512m",
        executor_cores=1,
        executor_cores_request="100m",
        executor_memory="512m",
        executor_instances=1,
        service_account="spark-operator-spark",
        # Image spark:3.5.1 = uid spark(185). Ne pas utiliser run_as_user=
        # (conflit BaseOperator Airflow → sudo -u). UID 0 laisse le token SA
        # illisible après drop entrypoint → 401 fabric8.
        driver_run_as_user=185,
        driver_fs_group=185,
        executor_run_as_user=185,
        executor_fs_group=185,
        timeout_job=600,
        queue="default",
        managed_by="vigie",
    )
