import os
import datetime
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

TAXI_COLOR = Variable.get("taxi_color")

@dag(
    start_date=datetime.datetime(2021, 1, 1), 
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["test"]
)
def sample_dag():
    
    file_name = f"{TAXI_COLOR}_tripdata_{{{{ logical_date.strftime(\'%Y-%m\') }}}}.csv"
    #val = f"some_name_{{{{ logical_date.strftime('%Y-%m') }}}}"
    #Variable.set(key="test_value", value=exec_date)

    @task
    def date_printer(exe_date):
        print(exe_date)
        Variable.set(key="test_value", value=exe_date)
    

    date_printer(file_name)


sample_dag()