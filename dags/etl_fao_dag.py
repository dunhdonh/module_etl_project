from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract_data.extract_fao import extract_fao
from transform_data.transform_fao import transform_fao
from load_data.load_fao_supabase import load_fao_supabase

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='etl_fao_pipeline',
    default_args=default_args,
    description='ETL Pipeline for FAO Data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='extract_fao',
        python_callable=extract_fao,
    )

    t2 = PythonOperator(
        task_id='transform_fao',
        python_callable=transform_fao,
    )

    t3 = PythonOperator(
        task_id='load_fao_supabase',
        python_callable=load_fao_supabase,
    )

    t1 >> t2 >> t3
