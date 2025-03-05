from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Olá Marte!")

with DAG(
    dag_id='DAG2',
    owner='Bruno Borges',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # Cria uma tarefa Python para imprimir "Hello, World!"
    hello_task = PythonOperator(
        task_id='Olá Marte!',
        python_callable=hello_world
    )