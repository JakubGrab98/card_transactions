import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from minio import Minio


load_dotenv()

def minio_upload_data(bucket_name, file_path):
    minio_client = Minio(
        os.getenv("NODE_IP"),  # Replace with any node's IP and port
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )
    found = minio_client.bucket_exists(bucket_name)
    if not found:
        minio_client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    minio_client.fput_object(bucket_name, file_path, file_path)
    print(f"Uploaded {file_path} to MinIO bucket {bucket_name}")

def read_data(spark: SparkSession, source_path: str="data/transform"):
    """
    Reads data from parquet files.
    :param spark: SparkSession
    :param source_path: Path to source folder.
    :return: DataFrame
    """
    df = spark.read.parquet(source_path)
    return df


if __name__ == "__main__":
    minio_upload_data("financials", "data/raw/users_data.csv")
    minio_upload_data("financials", "data/raw/transactions_data.csv")
    minio_upload_data("financials", "data/raw/cards_data.csv")
    minio_upload_data("financials", "data/raw/mcc_codes.json")
    minio_upload_data("financials", "spark-events/test.txt")

