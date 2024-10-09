from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3
import sys
from sqlalchemy import create_engine

# Untuk membaca file perintah extract filenya
sys.path.insert(0, 'C:\\Users\\OCHA\\Documents\\Boothcamp\\DE7\\Project_ETL\\airflow\\tasks')
from tasks.extraction_web import extract_data  


# Definisi DAG
@dag(
    description='Extract data from Detik.com and load into SQLite',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)
def ETL():

    # Task untuk ekstraksi
    @task
    def extract_task(**kwargs):
        # Memanggil fungsi extract_data dari extract_web.py
        extract_data()

    # Task untuk memuat data ke SQLite
    @task
    def load_data_to_sqlite(parquet_file='data/news_data.parquet', db_name='news.db'):
        # Membaca data dari file Parquet
        df = pd.read_parquet(parquet_file)
        
        # Buat koneksi ke SQLite
        engine = create_engine(f"sqlite:///{db_name}")
        connection = engine.connect()

        # Buat tabel untuk menampung data
        with connection.begin() as conn:
            conn.execute('DROP TABLE IF EXISTS news')
            conn.execute('''
                CREATE TABLE news(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    date_news DATETIME NOT NULL
                )
            ''')

            # Memasukkan data ke dalam tabel
            df.to_sql('news', con=conn, if_exists='append', index=False)

    # Dummy task untuk menandai awal DAG
    start_task = DummyOperator(task_id='start')

    # Menjalankan ekstraksi dan load ke SQLite
    extract = extract_task()
    load = load_data_to_sqlite()

    # Definisikan urutan task
    start_task >> extract >> load

# Inisialisasi DAG
dag_instance = ETL()
