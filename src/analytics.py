import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = (SparkSession.builder
         .appName("analytics")
         .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.2.101:9000")
         .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
         .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
         .getOrCreate()
)


tr_transaction = (
    spark.read
    .parquet("s3a://financials/data/transform/transactions")
)

tr_cards = (
    spark.read
    .parquet("s3a://financials/data/transform/card_data")
)

tr_users = (
    spark.read
    .parquet("s3a://financials/data/transform/users_data")
)

tr_merchant = (
    spark.read
    .parquet("s3a://financials/data/transform/merchants_data")
)

tr_transaction.show()
tr_cards.show()
tr_users.show()
tr_merchant.show()

def filter_transaction_period(df: DataFrame, start_year: int, no_of_years: int):
    end_year = start_year + no_of_years
    filtered_df = df.filter(
        (col("year") >= start_year) & (col("year") <= end_year)
    )
    return filtered_df

filtered_df = filter_transaction_period(tr_transaction, 2010, 2015)

def presentation_client_summary(transaction_df: DataFrame, client_df: DataFrame) -> None:
    """
    Summarizes transaction data by client age segmentation.
    Saves data in presentation layer.
    :param transaction_df: DataFrame with transaction data.
    :param client_df: DataFrame with client data.
    """
    client_summary = (
        transaction_df
        .join(
            client_df,
            [transaction_df["client_id"] == client_df["id"]],
            "left_outer"
        )
        .withColumn(
            "customer_age",
            when(client_df.current_age < 18, "<18")
            .when((client_df.current_age >= 18) & (client_df.current_age <= 24), "18-24")
            .when((client_df.current_age >= 25) & (client_df.current_age <= 34), "25-34")
            .when((client_df.current_age >= 35) & (client_df.current_age <= 44), "35-44")
            .when((client_df.current_age >= 45) & (client_df.current_age <= 54), "45-54")
            .when((client_df.current_age >= 55) & (client_df.current_age <= 64), "55-64")
            .when(client_df.current_age >= 65, ">65")
        )
        .groupby(["year", "month", "gender", "customer_age"])
        .agg(
            count(transaction_df.id).alias("transaction_count"),
            sum("amount").alias("total_spent"),
            avg("amount").alias("avg_transaction"),
            min("date").alias("first_transaction"),
            max("date").alias("last_transaction"),
        )
        .show()
    )
    # client_summary.write.mode("overwrite").parquet("s3a://financials/data/presentation/client_summary")

def presentation_merchant_summary(transaction_df: DataFrame, merchant_df: DataFrame) -> None:
    merchant_summary = (
        transaction_df
        .join(
            merchant_df,
            [transaction_df["merchant_id"] == merchant_df["merchant_id"]],
            "left_outer",
        )
        .groupby(["year", "month", "merchant_city", "merchant_industry"])
        .agg(
            count(transaction_df.id).alias("transaction_count"),
            sum("amount").alias("total_spent"),
            avg("amount").alias("avg_transaction"),
            countDistinct("client_id").alias("unique_customers")
        )
        .show()
    )
    # merchant_summary.write.mode("overwrite").parquet("s3a://financials/data/presentation/merchant_summary")

presentation_client_summary(filtered_df, tr_users)
presentation_merchant_summary(filtered_df, tr_merchant)