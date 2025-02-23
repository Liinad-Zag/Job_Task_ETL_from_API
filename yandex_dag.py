import ydirect
import ydirect_constants
import amount
import amount_constraints
import metrics
import metrics_constants
import common_to_all
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
dag = DAG(
    dag_id='parallel_dag_yandex',
    start_date=datetime.datetime(2025, 2, 9, 3, 0, 0),
    schedule_interval='@daily',
    catchup=False,
)
execute_get_all_metrics = PythonOperator(
    task_id = 'get_all_metrics',
    python_callable = metrics.get_all_metrics,
    dag = dag,
)
execute_get_all_direct = PythonOperator(
    task_id = 'get_all_direct',
    python_callable = ydirect.get_all_direct,
    dag = dag,
)