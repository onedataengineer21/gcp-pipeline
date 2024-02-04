# second_dag.py
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from python_scripts import second_script

default_args = {
    'owner': 'DE Book',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=30),
    'start_date': datetime.datetime(2020, 10, 17),
}

dag = DAG(
    'second_dag',
    schedule_interval="0 0 * * *",   # run every day at midnight UTC
    max_active_runs=1,
    catchup=False,
    default_args=default_args
)


t_run_second_script = PythonOperator(
    task_id="run_second_script",
    python_callable=second_script.say_hello,
    dag=dag
)
