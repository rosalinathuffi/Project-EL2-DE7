from airflow.decorators import task, dag # type: ignore
from airflow.operators.empty import EmptyOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.utils.trigger_rule import TriggerRule # type: ignore
from datetime import datetime
import os
import pandas as pd
from sqlalchemy import create_engine
import time 

# Impor fungsi dari file eksternal
from tasks.extraction_web import extract_data as extract_web_func
from tasks.extraction_kompas import extract_data as extract_kompas_func  # Ekstraksi dari Kompas

@dag(
    dag_id='ETL',
    description='ETL assignment with external extraction tasks.',
    schedule_interval="@daily",
    start_date=datetime(2024, 9, 29),
    catchup=False,
    params={
        "source": "all",  # Ambil dari semua sumber
        "extension": "parquet"  # Format penyimpanan
    }
)
def etl_process():
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.ONE_SUCCESS)

    def create_directories():
        os.makedirs('dags/data/staging', exist_ok=True)
        os.makedirs('dags/data/db_output', exist_ok=True)
        print("Directories ensured")
    
    def cleanup():
        now = time.time()
        for filename in os.listdir('dags/data/staging'):
            file_path = os.path.join('dags/data/staging', filename)
            if os.stat(file_path).st_mtime < now - 30 * 86400:  # Simpan untuk 30 hari saja
                os.remove(file_path)
                print(f"Deleted old file: {file_path}")
    
    create_directories_task = PythonOperator(
        task_id='create_directories',
        python_callable=create_directories
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_task',
        python_callable=cleanup
    )
    
    @task
    def extract_kompas_task():
        return extract_kompas_func(parquet_file='dags/data/staging/news_kompas.parquet')

    @task
    def extract_web_task():
        return extract_web_func(parquet_file='dags/data/staging/news_data.parquet')

    # Task untuk Load ke SQLite
    @task
    def load_to_sqlite():
        # Load data from Kompas
        kompas_file_path = '/opt/airflow/dags/data/staging/news_kompas.parquet'
        kompas_engine = create_engine('sqlite:////opt/airflow/dags/data/db_output/kompas.db')  # Use absolute path
        kompas_df = pd.read_parquet(kompas_file_path)
        kompas_df.to_sql('kompas_news', kompas_engine, if_exists='replace', index=False)
        print(f"Data loaded to SQLite from {kompas_file_path}")

        # Load data from Detik
        detik_file_path = '/opt/airflow/dags/data/staging/news_data.parquet'
        detik_engine = create_engine('sqlite:////opt/airflow/dags/data/db_output/detik.db')  # Use absolute path
        detik_df = pd.read_parquet(detik_file_path)
        detik_df.to_sql('detik_news', detik_engine, if_exists='replace', index=False)
        print(f"Data loaded to SQLite from {detik_file_path}")

    # Task untuk ekstraksi
    extract_kompas = extract_kompas_task()
    extract_web = extract_web_task()
    load_to_sqlite_task = load_to_sqlite()

    # Rangkaian task dalam DAG
    start >> create_directories_task >> [extract_kompas, extract_web] >> load_to_sqlite_task >> cleanup_task >> end

etl_process()
