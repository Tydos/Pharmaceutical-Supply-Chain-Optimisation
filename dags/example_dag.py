from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.split_data import split_data
from src.import_data import import_data
from src.process_data import process_data

def hello_world():
    logging.info("Hello Airflow!")


def fetch_data():
    logging.info("Fetching data...")
    data = import_data("/opt/airflow/data/supply_chain.csv")
    logging.info(f"Loaded {len(data)} rows")
    return data


def preview_data():
    logging.info("Previewing data...")
    data = import_data("/opt/airflow/data/supply_chain.csv")
    logging.info("Data preview:")
    logging.info("\n%s", data.head().to_string())

def process_data_task():
    logging.info("Processing data...")
    data = import_data("/opt/airflow/data/supply_chain.csv")
    processed_data = process_data(data)
    logging.info("Processed data preview:")
    # logging.info("\n%s", processed_data.head().to_string())
    return processed_data

def split_data_task():
    logging.info("Splitting data...")
    data = import_data("/opt/airflow/data/supply_chain.csv")
    processed_data = process_data(data)
    split_paths = split_data(processed_file_path=processed_data, test_size=0.2, random_state=42, balance_classes=True)
    logging.info(f"Data split paths: {split_paths}")
    return split_paths

def done():
    p_data = process_data_task()
    # logging.info("\n%s", p_data.head(10).to_string())
    logging.info("Pipeline finished successfully.")


with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2026, 1, 6),
    schedule="*/1 * * * *",
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="print_hello",
        python_callable=hello_world,
    )

    t2 = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
    )

    t3 = PythonOperator(
        task_id="preview_data",
        python_callable=preview_data,
    )

    t4 = PythonOperator(
        task_id="process_data",
        python_callable=process_data_task,
    )

    t6 = PythonOperator(
        task_id="split_data",
        python_callable=split_data_task,
    )

    end = PythonOperator(
        task_id="done",
        python_callable=done,
    )

    t1 >> t2 >> t3 >> t4 >> t6 >> end
