from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskMarker, ExternalTaskSensor


default_args = {
    'owner' : 'zaid.ryu',
}

dag = DAG('TEST_Sensor_Master_Dag',
        description = 'Test ExternalTaskSensor Master Dag',
        schedule_interval = '0 17 * * *',
        default_args = default_args,
        start_date = datetime(2022, 2, 18),
        catchup = False)


m_task1 = DummyOperator(task_id = 'master_task1',
                        dag = dag)

m_task2 = DummyOperator(task_id = 'master_task2',
                        dag= dag)

m_task3 = DummyOperator(task_id = 'master_task3',
                        dag= dag)

m_task4 = DummyOperator(task_id = 'master_task4',
                        dag= dag)

m_task5 = DummyOperator(task_id = 'master_task5',
                        dag= dag)

m_task1 >> m_task2
m_task1 >> m_task3
m_task3 >> m_task4
m_task3 >> m_task5