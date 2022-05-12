from pytz import timezone
from dateutil import parser
from datetime import datetime, time
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

default_args = {
    'owner' : 'zaid.ryu',
}

dag = DAG('Test_Branch_Operator',
        description = 'Test_Branch_Operator',
        schedule_interval = '@once',
        default_args = default_args,
        start_date = datetime(2022, 2, 18),
        catchup = False)


def branch_func(**context):
        kst = timezone('Asia/Seoul')
        current_time = parser.parse(context['ts']).astimezone(kst)
        meridiem = current_time.strftime("%p")

        if meridiem == 'PM' :
                return 'PM_TASK'
        else:
                return 'AM_TASK'

branch_task = BranchPythonOperator(task_id = 'Branch_Task',
                                   python_callable=branch_func,
                                   provide_context=True,
                                   dag = dag)

dummy1 = DummyOperator(task_id = 'PM_TASK', dag = dag)
dummy2 = DummyOperator(task_id = 'AM_TASK', dag = dag)

branch_task >> [dummy1, dummy2]