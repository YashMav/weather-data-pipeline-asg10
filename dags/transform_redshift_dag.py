from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import datetime


default_args ={
    'owner' :'airflow',
    'depends_on_past' : False,
    'start_date' : datetime.datetime.strptime('20230101','%Y%m%d'),
    'catchup' :False,
    'retries':1,
    'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(
    'transform_redshift_dag',
    default_args=default_args,
    schedule_interval="@Once"
)

transform_task = GlueJobOperator(
    task_id='transform_task',
    job_name='glue_transform_task',
    script_location='s3://aws-glue-assets-058264222641-ap-south-1/scripts/weather_data_ingestion.py',
    s3_bucket='aws-glue-assets-058264222641-ap-south-1',
    aws_conn_id='aws_default',
    region_name='ap-south-1',
    iam_role='AWS-GLUE-ROLE',
    create_job_kwargs={"GlueVersion":"4.0","NumberOfWorkers":2,"WorkerType":"G.1X","Connections":{"Connections":['Redshift connection 2']}},
    dag=dag
)
