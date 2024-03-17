from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum
from airflow.operators.docker_operator import DockerOperator


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

    run = DockerOperator(
        task_id="run_app",
        image="ubuntu",
        docker_url='tcp://docker-proxy:2375',
        network_mode="bridge",
        command="echo Hello",
        api_verison="auto",
    )

    end = EmptyOperator(task_id="end")

    start >> run >> end
