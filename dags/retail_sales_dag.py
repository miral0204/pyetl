from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the scripts directory to the Python path so our ETL script can be imported
# This assumes 'scripts' is a sibling directory to 'dags' within /opt/airflow
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))

try:
    from etl_script import run_etl
except ImportError as e:
    print(f"Could not import run_etl: {e}")
    print("Ensure 'etl_script.py' is in the 'scripts' directory and 'scripts' is mounted and added to PYTHONPATH.")
    # Define a dummy function to prevent DAG parsing errors if import fails during initial setup
    def run_etl():
        print("ETL script not found or failed to import. Please check file paths and mounts.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='retail_sales_etl_pipeline',
    default_args=default_args,
    description='A DAG to ingest, transform, and load retail sales data into PostgreSQL.',
    schedule_interval=timedelta(days=1), # Run daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['retail', 'etl', 'sales'],
) as dag:
    extract_transform_load = PythonOperator(
        task_id='extract_transform_load_data',
        python_callable=run_etl,
        # Ensure the etl_script.py can find the sales_data.csv
        # The script uses relative paths based on its location within /opt/airflow/scripts
        # and expects 'data/raw/sales_data.csv' relative to /opt/airflow
    )