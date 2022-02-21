from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner': 'zaid.ryu'
}

dag = DAG('Master_Dag_for_TDR',
          description = 'Master Dag',
          schedule_interval = '0 17 * * *',
          default_args = default_args,
          start_date = datetime(2022, 2, 19),
          catchup = False)


def trigger_func(context, obj):
    obj.payload = {'name':'Zaid.Ryu',
                   'talk':'this is my first code for TriggerDagRunOperator'}
    return obj

def print_ds(ds):
    print(f'ds : {ds}')



m_task1 = DummyOperator(task_id = 'master_task',
                        dag = dag)

m_task2 = TriggerDagRunOperator(task_id = 'trigger_slave',
                                trigger_dag_id = 'Slave_Dag_for_TDR',
                                python_callable = trigger_func,
                                execution_date = "{{ ds }}",
                                dag = dag)

m_task3 = PythonOperator(task_id = 'printDS',
                         python_callable = print_ds,
                         op_args=["{{ ds }}"],
                         dag = dag)


m_task1 >> m_task2 >> m_task3