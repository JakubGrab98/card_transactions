from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


dag = DAG(
    dag_id="transactions_flow",
    default_args={
        "owner": "Jakub Grabarczyk",
        "start_date": datetime(2025, 1, 12),
        "provide_context": True,
    },
    schedule_interval="@daily",
)

transformation_layer = SparkSubmitOperator(
    task_id="transform_data",
    application="/$HOME_SPARK/apps/etl/transform.py",
    conn_id="spark_conn",
    verbose=True,
    dag=dag,
)

presentation_layer = SparkSubmitOperator(
    task_id="presentation",
    application="/$HOME_SPARK/apps/etl/presentation.py",
    conn_id="spark_conn",
    executor_memory="4g",
    verbose=True,
    dag=dag,
)

transformation_layer >> presentation_layer
