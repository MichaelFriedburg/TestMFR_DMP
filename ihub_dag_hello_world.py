# coding=utf-8
# DAG AIRFLOW für ihub
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from dxd_core.modules.dxd_exception_email import send_error_email


doc_md = """
### For proper documentation template

Das ist ein Test-DAG

#### documentation
"""

# Standard auto defining variables.
dag_name = os.path.splitext(os.path.basename(__file__))[0]
dag_dir = os.path.dirname(__file__)
dag_project_id = dag_name.split('_', 1)[0]

default_args = {
    'owner': dag_project_id.upper(),
    'pool': dag_project_id.upper(),
    'wait_for_downstream': False,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': send_error_email,
    'provide_context': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Defining / Creating DAG Instance
dag = DAG(
    dag_id=dag_name,
    start_date=datetime(2022, 1, 25, 8, 30),
    schedule=None,
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    doc_md=doc_md,
    tags=[dag_project_id, "test"],
    default_args=default_args,
)


def test_function(test_variable: str, **kwargs):
    print(f"Hello World at {kwargs.get('ds')}")
    print(f"Variable: {test_variable}")


def test_function_v2(**kwargs):
    print(f"Hello World at {kwargs.get('ds')} from second function")


# Individual defining the DAG (workflow) tasks and its processing order.
with dag:
    test_variable = "Test"

    task = PythonOperator(
        task_id='test_task',
        python_callable=test_function,
        op_kwargs={
            "test_variable": test_variable,
        },
    )

    task_v1 = PythonOperator(
        task_id='test_task_v1',
        python_callable=test_function_v2,
    )

    task >> task_v1
