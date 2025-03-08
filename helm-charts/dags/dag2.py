from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Olá Mundo!")

with DAG(
    dag_id='DAG1',
    owner='Bruno Borges',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # Cria uma tarefa Python para imprimir "Olá Mundo!"
    hello_task = PythonOperator(
        task_id='hello_world_task1',  # task_id corrigido
        python_callable=hello_world
    )