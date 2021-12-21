# References
# https://aws.amazon.com/pt/blogs/aws/introducing-amazon-managed-workflows-for-apache-airflow-mwaa/
# https://docs.aws.amazon.com/mwaa/latest/userguide/sample-code.html
# Redshift Copy - https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
# https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/s3_to_redshift.html
# https://aws.amazon.com/about-aws/whats-new/2021/11/amazon-redshift-sqlalchemy-apache-airflow-open-source-frameworks/
# https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_modules/airflow/providers/amazon/aws/example_dags/example_s3_to_redshift.html


# Requirements
# - Dataset on S3 (csv)
# - Glue Crawler created poiting to S3 bucket where dataset is located
# - Schema created on Redshift compatible with the S3 dataset
# - Role create authorizing Redshift copy from the S3 bucket and associated with cluster - https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html
# - Redshift IAM --> s3 access to the bucket
# - Airflow Role --> S3 access, Glue Crawler permissions, Redshift permissions, appflow permissions


import airflow  
from airflow import DAG  

# Custom Operators deployed as Airflow plugins
# from awsairflowlib.operators.aws_glue_job_operator import AWSGlueJobOperator
from awsairflowlib.operators.aws_glue_crawler_operator import AWSGlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor
import awswrangler as wr

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import boto3

from datetime import datetime
from os import getenv
import boto3


s3_bucket_name = 'your-bucket-name'
# s3_key='files/'
redshift_cluster='redshift-cluster-name'
redshift_db='database-name'
redshift_dbuser='your-user'
redshift_table_name='table-name'
crawler_name = 'glue_crawler_name'

# partitions
year = '2021'
month = '12'
day = '17'

appflow = boto3.client('appflow')

flowname = "test_flow"

execution_id = ''


def start_appflow_flow():

    response = appflow.start_flow(flowName=flowname) 
    execution_id = response["executionId"]
    print("ExecutionId")
    print(execution_id)

    return 'ok'


def s3_to_redshift():    
    s3_location=f's3://{s3_bucket_name}/appflow/orders/test_flow/{year}/{month}/{day}/{execution_id}'
    sqlQuery="copy "+redshift_table_name+" from '"+s3_location+"' iam_role 'arn:aws:iam::ACCOUNTID:role/redshift-role-airflow' CSV IGNOREHEADER 1;"
    print(sqlQuery)
    rsd = boto3.client('redshift-data')
    resp = rsd.execute_statement(
        ClusterIdentifier=redshift_cluster,
        Database=redshift_db,
        DbUser=redshift_dbuser,
        Sql=sqlQuery
    )
    print(resp)
    return "OK"


default_args = {  
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


dag = DAG(  
    's3-redshift-copy',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 1 * * *'
)

app_flow_task = PythonOperator(
        task_id="start_appflow",
        python_callable=start_appflow_flow,
        dag=dag     
    )

s3_sensor = S3PrefixSensor(  
  task_id='s3_sensor',  
  bucket_name=s3_bucket_name,  
  prefix=f"appflow/orders/test_flow/{year}/{month}/{day}/{execution_id}",  
  dag=dag  
)

glue_crawler = AWSGlueCrawlerOperator(
    task_id="glue_crawler",
    crawler_name=crawler_name,
    iam_role_name='AWSGlueServiceRole-AriflowSample',
    dag=dag
)

redshift_copy = PythonOperator(
        task_id="copy_to_redshift",
        python_callable=s3_to_redshift,
        provide_context=True,
        dag=dag     
    )

# construct the DAG by setting the dependencies
app_flow_task >> s3_sensor >> glue_crawler >> redshift_copy

