import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


spark_conf = {
    "spark.hadoop.fs.s3a.endpoint": "http://192.168.2.101:9000",
    "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD"),
}

python_vars = {
    "PYSPARK_PYTHON": "/usr/bin/python3.10",
    "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3.10",
}

with DAG("transactions_flow",
         default_args={
        "owner": "Jakub Grabarczyk",
        "start_date": datetime(2025, 1, 12),
        "provide_context": True,
    },
    schedule_interval="@daily",
) as dag:

    transformation_layer = SparkSubmitOperator(
        task_id="transform_data",
        application="/opt/airflow/jobs/transform.py",
        conn_id="spark_conn",
        executor_memory="4g",
        verbose=True,
        conf=spark_conf,
        env_vars=python_vars,
    )

    presentation_layer = SparkSubmitOperator(
        task_id="presentation",
        application="/opt/airflow/jobs/presentation.py",
        conn_id="spark_conn",
        executor_memory="4g",
        verbose=True,
        conf=spark_conf,
        env_vars=python_vars,
    )

transformation_layer >> presentation_layer
