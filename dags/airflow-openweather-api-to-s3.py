from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import datetime
import json
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
import pandas as pd
import requests
from airflow.operators.dagrun_operator import TriggerDagrunOperator

default_args ={
    'owner' :' airflow',
    'depends_on_past':False,
    'catchup':False,
    'start_date' : datetime.datetime.strptime('20230101','%Y%m%d'),
    'retries':2,
    'retry_delay':datetime.timedelta(minutes=5)
}

api_endpoint = 'https://api.openweathermap.org/data/2.5/forecast'
api_params = {
    "q":"Toronto,Canada",
    "appid": Variable.get("api_key")
}

dag = DAG(
    'open_weather_api_dag',
    default_args = default_args,
    schedule_interval = "@Once"
)

def extract_openweather_api(**kwargs):
    print("Extracting weather api data .....")
    ti = kwargs['ti']
    response = requests.get(api_endpoint,params=api_params)
    data =response.json()
    print(data)
    df = pd.json_normalize(data['list'])
    print(df)
    ti.xcom_push(key='final_data' , value = df.to_csv(index=False))

extract_data_api = PythonOperator(
    task_id = 'extract_data_api',
    python_callable = extract_openweather_api(),
    provide_context = True,
    dag=dag
)

upload_to_s3 = S3CreateObjectOperator(
    task_id= 'upload_to_s3',
    aws_conn_id  = 'aws_default',
    s3_bucket ='assg10-weather-data',
    s3_key='date={{ds}}/weather_api_data.csv',
    data = "{{  ti.xcom_pull(key='final_data')}}",
    dag=dag
)


trigger_transform_redshift_dag = TriggerDagrunOperator(
    task_id='trigger_transform_redshift_dag',
    trigger_dag = 'transform_redshift_dag',
    dag=dag
)


extract_data_api >> upload_to_s3 >> trigger_transform_redshift_dag
