from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

def hello_world(name):
    print(f"Hello, {name}!")

with DAG(
    dag_id="test",
    default_args={
        "owner": "annakor",
    },
    max_active_tasks=1,
    max_active_runs=1,
    tags=["custom_dag"],
) as dag:
    start = EmptyOperator(task_id="start")

    hello = PythonOperator(
        task_id="hello",
        python_callable=hello_world,
        op_kwargs={"name": "Anya"},
        schedule_intreval="@once",
    )

    end = EmptyOperator(task_id="end")

    start >> hello >> end

