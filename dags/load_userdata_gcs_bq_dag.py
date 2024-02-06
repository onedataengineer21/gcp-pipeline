from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from scripts.userdata_gcs_bq import load_userdata_gcs_bq
import datetime

# Get the current date and time
current_datetime = datetime.datetime.now()
# Format the date and time in YYYYMMDDHHMMSS format
foldername = current_datetime.strftime('%Y%m%d')
filetimestamp = current_datetime.strftime('%Y%m%d%H%M%S')

## Setting the path and file name
filename = f'{filetimestamp}.parquet'

default_args = {
    'owner': 'OneDataEngineer',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=30),
    'start_date': datetime.datetime(2024, 2, 4),
}

dag = DAG(
    'load_userdata_gcs_bq',
    schedule_interval="*/25 * * * *",   # run every 2 minutes
    max_active_runs=1,
    catchup=False,
    default_args=default_args
)

t_extract_app_users_data = PythonOperator(
    task_id="extract_app_users_data",
    python_callable=load_userdata_gcs_bq.app,
    op_kwargs={"filename": filename, "foldername": foldername},
    dag=dag,
)

# A task to print that the product data has been downloaded.
t_print_message = BashOperator(
    task_id='print_message',
    bash_command='echo "Users data has been downloaded. Congrats!!!!!!!"',
    dag=dag
)

# Setting the first task as a dependency for the second task.
t_print_message.set_upstream(t_extract_app_users_data)

