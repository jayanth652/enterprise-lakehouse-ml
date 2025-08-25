from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess, sys

PROJECT_DATA = "/data"
SPARK_SCRIPT_DIR = "/opt/airflow/spark_jobs"

def run_cmd(cmd):
    print("+", " ".join(cmd))
    subprocess.check_call(cmd)

def gen_data():
    run_cmd([sys.executable, f"{PROJECT_DATA}/make_synthetic_data.py"])

def spark_transform():
    run_cmd([sys.executable, f"{SPARK_SCRIPT_DIR}/transform_orders.py"])

def build_features():
    run_cmd([sys.executable, f"{SPARK_SCRIPT_DIR}/build_clv_features.py"])

def train_models():
    run_cmd([sys.executable, "/ml/train_clv.py"])

with DAG(
    dag_id="lakehouse_end_to_end",
    start_date=datetime(2024,1,1),
    schedule="@daily",
    catchup=False,
    default_args={"owner":"airflow"},
    tags=["lakehouse","spark","ml"],
):
    t1 = PythonOperator(task_id="generate_data", python_callable=gen_data)
    t2 = PythonOperator(task_id="spark_transform", python_callable=spark_transform)
    t3 = PythonOperator(task_id="build_clv_features", python_callable=build_features)
    t4 = PythonOperator(task_id="train_models", python_callable=train_models)

    t1 >> t2 >> t3 >> t4