from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum


with DAG(
    dag_id="test",
    default_args={
        "owner": "annakor",
    },
    schedule_interval="@once",
    start_date=pendulum.today('Europe/Moscow').add(days=-1),
    max_active_tasks=1,
    max_active_runs=1,
    tags=["custom_dag"],
) as dag:
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    start >> end
