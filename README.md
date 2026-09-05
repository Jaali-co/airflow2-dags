# airflow2-dags

DAGs pour Airflow 2 (`airflow2.jaali.dev`) — sync via git-sync.

## Contenu

| DAG | Runtime |
|-----|---------|
| `af2_echo_sleep` | Celery (BashOperator) |
| `af2_python_k8s` | KubernetesPodOperator → `airflow2-python-jobs` |
| `af2_spark_k8s` | `vigie_spark_operator.SparkK8sOperator` → `airflow2-spark-jobs` |

Le package `vigie_spark_operator` (labels `dag_id` / `task_id` / `run_id` pour Supervision)
est vendu sous `dags/vigie_spark_operator/` (source monorepo `vigie-spark-operator` v0.1.0).

```python
from vigie_spark_operator import SparkK8sOperator
```

Branche sync : `main`.
