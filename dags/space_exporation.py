
from datetime import datetime
import requests
import json
from pprint import pprint
import pandas as pd

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.exceptions import AirflowSkipException


# **context can also be used but dont use it, linting tools will complain
#always have task_instance & the ** indicates the context 
def _is_rocket_launch_today(task_instance, **_): 
    launch_data_today = json.loads(task_instance.xcom_pull(task_ids="get_launch_data", key="return_value"))
    print(launch_data_today)
    if launch_data_today['count'] == 0:
        raise AirflowSkipException('No launches today :(')

def _add_file_to_storage(task_instance, **context):
    launch_data_today = json.loads(task_instance.xcom_pull(task_ids='get_launch_data', key='return_value'))['results']
    launches = []

    for launch in launch_data_today:
        print(launch['id'])
        launches.append({'launch_id': launch['id'], 
            'mission_name': launch['name'], 
            'country': launch['pad']['country']['name'],
            'status': launch['status']['name'],
            'launch_service_provider': launch['launch_service_provider']['name'],
            'launch_service_provider_type': launch['launch_service_provider']['type']['name']
        })
    
    launch_data_df = pd.DataFrame(launches)
    file_name = context["data_interval_start"].strftime('%m-%d-%Y')
    launch_data_df.to_parquet(f"/tmp/{file_name}.parquet")
    print(launch_data_df)
    return launch_data_df

    # launch_data_today = pd.DataFrame(launch_data_today)

    # id
    # name 
    # mission 
    # launch status 
    # country 
    # launch service provider 


    # launch_data_today['mission']

with DAG(
    dag_id="space_exploration",
    start_date=datetime(year=2025, month=3, day=15),
    schedule="@daily",
):
    
    # returns a boolean
    check_space_api_health = HttpSensor(
        task_id="check_space_api_health", 
        http_conn_id='spacedevs', 
        endpoint='',
        method='GET', 
        # response_check=lambda response: response.status_code==200, 
        # mode='poke', 
        # timeout=200,
        # poke_interval=30
    )

    get_launch_data = HttpOperator(
        task_id='get_launch_data',
        http_conn_id='spacedevs',
        method='GET',
        endpoint='',
        data={"window_start__gte":"{{data_interval_start | ds}}T00:00:00Z", "window_end__lte":"{{data_interval_end | ds}}T00:00:00Z"},
        # data={'window_start__gte':'2025-03-24T00:00:00Z', 'window_end__lte':'2025-03-24T00:00:00Z'}
        # data={'window_start__gte':'{{data_interval_start | ds}}T00:00:00Z', 'window_end__lte':'{{data_interval_start | ds}}T00:00:00Z'}
    ) 

    is_rocket_launch_today = PythonOperator(
        task_id='is_rocket_launch_today',
        python_callable=_is_rocket_launch_today #point to the callable
        
    )

    add_file_to_storage = PythonOperator(
        task_id='add_file_to_storage',
        python_callable=_add_file_to_storage
    )

check_space_api_health >> get_launch_data >> is_rocket_launch_today >> add_file_to_storage


    


    


