from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'zaid.ryu'
}

dag = DAG('Slave_Dag_for_TDR',
          description = 'Slave Dag',
          schedule_interval = None,
          default_args=default_args,
          start_date = datetime(2022, 2, 19),
          catchup = False)

def print_conf(**context):
    dag_run = context['dag_run'].conf
    print('{name} said \"{talk}\"'.format(**dag_run))

def print_ds(ds):
    print(f'ds : {ds}')



s_task1 = DummyOperator(task_id = 'slave_task',
                        dag = dag)

s_task2 = PythonOperator(task_id = 'printDagRun',
                         python_callable=print_conf,
                         provide_context=True,
                         dag = dag)

s_task3 = PythonOperator(task_id = 'printDS',
                         python_callable = print_ds,
                         op_args=["{{ ds }}"],
                         dag = dag)

s_task1 >> s_task2 >> s_task3