import os
from pyspark.sql import SparkSession


def read_csv_data(spark: SparkSession, path: str):
    """
    Reades csv files stored from configured object store.
    :param spark: SparkSession
    :param path: Path to csv file with data.
    :return: PySpark DataFrame.
    """
    df = (spark.read
          .option("header", "true")
          .csv(path)
    )
    return df

if __name__ == "__main__":
    spark_session = (
        SparkSession.builder
        .appName("Finance Transactions")
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.2.101:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .getOrCreate()
    )

    users_df = read_csv_data(spark_session, "s3a://financials/data/raw/users_data.csv")
    users_df.printSchema()
    users_df.show()
    transaction_df = read_csv_data(spark_session, "s3a://financials/data/raw/transactions_data.csv")
    transaction_df.printSchema()
    transaction_df.show()
    cards_df = read_csv_data(spark_session, "s3a://financials/data/raw/cards_data.csv")
    cards_df.printSchema()
    cards_df.show()