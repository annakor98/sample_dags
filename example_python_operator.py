from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from airflow.providers.telegram.operators.telegram import TelegramOperator

def hello_world(name):
    print(f"Hello, {name}!")

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

    send_message_telegram_task = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='6435473792:AAEZwLd-Lq3Y8pZziLaKQ4X6hcx0bmO4un8',
        #chat_id='-3222103937',
        text='Hello from Airflow!',
        dag=dag,
    )

    end = EmptyOperator(task_id="end")

    start >> send_message_telegram_task >> end

