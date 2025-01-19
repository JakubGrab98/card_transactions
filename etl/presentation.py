import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *


PRESENTATION_PATH = os.getenv("BUCKET_PRESENTATION_PATH")

def filter_transaction_period(df: DataFrame, start_year: int, no_of_years: int)-> DataFrame:
    """
    Filters transaction by periods.
    :param df: Transformed transaction dataframe.
    :param start_year: Start year of the analysis.
    :param no_of_years: Number of years for analysis.
    :return: Filtered DataFrame
    """
    end_year = start_year + no_of_years
    filtered_df = df.filter(
        (col("year") >= start_year) & (col("year") <= end_year)
    )
    return filtered_df


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
            broadcast(client_df),
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
            count(transaction_df.id).alias("Transaction Count"),
            sum("amount").alias("Total Spent"),
            avg("amount").alias("AVG Transaction"),
            min("date").alias("First Transaction"),
            max("date").alias("Last Transaction"),
        )
        .withColumnRenamed("year", "Year")
        .withColumnRenamed("month", "Month")
        .withColumnRenamed("gender", "Gender")
        .withColumnRenamed("customer_age", "Client Age")

    )
    (
        client_summary.write
        .mode("overwrite")
        .parquet(f"{PRESENTATION_PATH}/client_summary")
    )

def presentation_merchant_summary(transaction_df: DataFrame) -> None:
    """
    Summarizes transaction data by merchant city and industry.
    Saves data in presentation layer.
    :param transaction_df: DataFrame with transaction data.
    """
    merchant_summary = (
        transaction_df
        .groupby(["year", "month", "merchant_city", "merchant_industry"])
        .agg(
            count("id").alias("Transaction Count"),
            sum("amount").alias("Total Spent"),
            avg("amount").alias("AVG Transaction"),
            countDistinct("client_id").alias("Client Count")
        )
        .withColumnRenamed("year", "Year")
        .withColumnRenamed("month", "Month")
        .withColumnRenamed("merchant_city", "Merchant City")
        .withColumnRenamed("merchant_industry", "Merchant Industry")

    )
    (
        merchant_summary.write
        .mode("overwrite")
        .parquet(f"{PRESENTATION_PATH}/merchant_summary")
    )

def presentation_card_summary(transaction_df: DataFrame, card_df: DataFrame) -> None:
    """
    Summarizes transaction data by card type and brand.
    Saves data in presentation layer.
    :param transaction_df: DataFrame with transaction data.
    :param card_df: DataFrame with card data.
    """
    card_summary = (
        transaction_df
        .join(
            broadcast(card_df),
            [transaction_df["card_id"] == card_df["id"]],
            "left_outer",
        )
        .groupby(["year", "month", "card_type", "card_brand"])
        .agg(
            count(transaction_df["id"]).alias("transaction_count"),
            sum("amount").alias("Total Spent"),
            avg("amount").alias("AVG Transaction"),
            max("amount").alias("Max Amount"),
            min("amount").alias("Min Amount")
        )
        .withColumnRenamed("year", "Year")
        .withColumnRenamed("month", "Month")
        .withColumnRenamed("card_brand", "Card Brand")
        .withColumnRenamed("card_type", "Card Type")
    )
    (
        card_summary.write
        .mode("overwrite")
        .parquet(f"{PRESENTATION_PATH}/card_summary")
    )

def year_over_year_growth(transaction_df: DataFrame) -> DataFrame:
    """
    Calculates year-over-year growth in spending.
    :param transaction_df: DataFrame with transaction data.
    :return: DataFrame with YoY growth metrics.
    """
    spending_by_year = (
        transaction_df
        .groupby("year")
        .agg(sum("amount").alias("total_spent"))
        .withColumnRenamed("year", "Year")
    )

    spending_yoy = (
        spending_by_year
        .withColumn("prev_year_spent", lag("total_spent").over(Window.orderBy("Year")))
        .withColumn("YoY Growth",
                    when(col("prev_year_spent").isNotNull(),
                         ((col("total_spent") - col("prev_year_spent")) / col("prev_year_spent")))
                    .otherwise(None))
        .withColumnRenamed("total_spent", "Total Spent")
    )
    (
        spending_yoy.write
        .mode("overwrite")
        .parquet(f"{PRESENTATION_PATH}/yoy_growth")
    )
    return spending_yoy

def customer_frequency_recency(transaction_df: DataFrame) -> DataFrame:
    """
    Calculates frequency and recency metrics for customer transactions.
    :param transaction_df: DataFrame with transaction data.
    :return: DataFrame with frequency and recency metrics.
    """
    customer_metrics = (
        transaction_df
        .groupby("client_id")
        .agg(
            count("id").alias("Transaction Frequency"),
            max("date").alias("Last Transaction Date"),
        )
        .withColumn("Recency", datediff(current_date(), col("Last Transaction Date")))
    )
    (
        customer_metrics.write
        .mode("overwrite")
        .parquet(f"{PRESENTATION_PATH}/client_recency")
    )
    return customer_metrics

def presentation(spark: SparkSession):
    transaction_df = spark.read.parquet("s3a://financials/data/transform/transactions")
    client_df = spark.read.parquet("s3a://financials/data/transform/users_data")
    card_df = spark.read.parquet("s3a://financials/data/transform/card_data")
    presentation_client_summary(transaction_df, client_df)
    presentation_merchant_summary(transaction_df)
    presentation_card_summary(transaction_df, card_df)
    year_over_year_growth(transaction_df)
    customer_frequency_recency(transaction_df)

if __name__ == "__main__":
    spark_session = (
        SparkSession.builder
        .appName("Finance Transactions")
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.2.101:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .getOrCreate()
    )
    presentation(spark_session)
