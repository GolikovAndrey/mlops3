import pendulum
from airflow.models import DAG
from scripts.evaluate_model import evaluate_model
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="evaluate_model_dag",
    start_date=pendulum.datetime(2023, 12, 18),
    schedule_interval=None,
    tags=["WORK"],
    catchup=False
):
    
    evaluate_model_task = PythonOperator(
        task_id="evaluate_model_task",
        python_callable=evaluate_model
    )

evaluate_model_task