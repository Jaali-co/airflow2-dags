# airflow2-dags

DAGs pour Airflow 2 (`airflow2.jaali.dev`) — sync via git-sync.

## Contenu

| DAG | Runtime |
|-----|---------|
| `af2_echo_sleep` | Celery (BashOperator) |
| `af2_python_k8s` | KubernetesPodOperator → `airflow2-python-jobs` |
| `af2_spark_k8s` | `SparkK8sOperator` custom → `airflow2-spark-jobs` |

Branche sync : `main`.
