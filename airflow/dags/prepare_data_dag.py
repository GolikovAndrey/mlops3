import pendulum
from airflow.models import DAG
from scripts.prepare_data import prepare_data
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="prepare_data_dag",
    start_date=pendulum.datetime(2023, 12, 18),
    schedule_interval=None,
    tags=["WORK"],
    catchup=False
):
    
    prepare_data_task = PythonOperator(
        task_id="prepare_data_task",
        python_callable=prepare_data
    )

prepare_data_task