from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.notifications.basenotifier import BaseNotifier
from airflow.providers.telegram.hooks.telegram import TelegramHook


class TelegramNotifier(BaseNotifier):
    def __init__(self, token, chat_id, telegram_conn_id="telegram_default"):
        self.telegram_conn_id = telegram_conn_id
        self.token = token
        self.chat_id = chat_id

    def notify(self, context):
        task_id = context["ti"].task_id
        task_state = context["ti"].state
        dag_name = context["ti"].dag_id

        message_template = (f"Dag name: `{dag_name}` \n"
                            f"Task id: `{task_id}` \n"
                            f"Task State: `{task_state}` \n"
                            )

        telegram_hook = TelegramHook(
            telegram_conn_id=self.telegram_conn_id,
            token=self.token,
            chat_id=self.chat_id,
        )

        telegram_hook.send_message(message_template)


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
    on_success_callback=TelegramNotifier(
        token='6435473792:AAEZwLd-Lq3Y8pZziLaKQ4X6hcx0bmO4un8',
        chat_id='-4177695418'
    ),
    on_failure_callback=TelegramNotifier(
        token='6435473792:AAEZwLd-Lq3Y8pZziLaKQ4X6hcx0bmO4un8',
        chat_id='-4177695418'
    )
) as dag:
    start = EmptyOperator(task_id="start")

    send_message_telegram_task = TelegramOperator(
        task_id='send_message_telegram',
        token='6435473792:AAEZwLd-Lq3Y8pZziLaKQ4X6hcx0bmO4un8',
        chat_id='-4177695418',
        text='Hello from Airflow!',
        dag=dag,
    )

    end = EmptyOperator(task_id="end")

    start >> send_message_telegram_task >> end
