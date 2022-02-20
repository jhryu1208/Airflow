from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.state import State

default_args = {
    'owner' : 'zaid.ryu'
}

dag = DAG('TEST_Sensor_Slave_Dag2',
          description = 'Test ExternalTaskSensor Slave Dag2',
          schedule_interval = '0 17 * * *',
          default_args = default_args,
          start_date = datetime(2022, 2, 18),
          catchup = False)

s_task1 = ExternalTaskSensor(task_id = 'slave_task1_sensor',
                             external_dag_id = 'TEST_Sensor_Master_Dag',
                             external_task_id = 'master_task1',
                             allowed_states = [State.SUCCESS, State.SKIPPED],
                             failed_states = [State.FAILED],
                             check_existence = True,
                             execution_date_fn = lambda dt : dt,
                             mode='reschedule',
                             #timeout=3600,
                             dag = dag)

s_task2 = ExternalTaskSensor(task_id = 'slave_task2_sensor',
                             external_dag_id = 'TEST_Sensor_Master_Dag',
                             external_task_id = 'master_task5',
                             allowed_states = [State.SUCCESS, State.SKIPPED],
                             failed_states = [State.FAILED],
                             check_existence = True,
                             execution_date_fn = lambda dt : dt,
                             mode='reschedule',
                             #timeout=3600,
                             dag = dag)


s_task1 >> s_task2