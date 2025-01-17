import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import utils as ut
import const as ct


RAW_PATH = os.getenv("BUCKET_RAW_PATH")
TRANSFORM_PATH = os.getenv("BUCKET_TRANSFORM_PATH")

def transform_transactions_data(transaction_df: DataFrame, mcc_df: DataFrame) -> DataFrame:
    """
    Transforms dataset with transactions.
    :param transaction_df: Raw transaction dataframe.
    :param mcc_df: DataFrame with mcc codes details.
    :return: Transformed transaction dataset.
    """
    transformed_mcc = (
        mcc_df
        .withColumnRenamed("name", "merchant_industry")
        .withColumnRenamed("id", "mcc_id")
    )
    joined_df = (
        transaction_df
        .join(
            broadcast(transformed_mcc),
            [col("mcc") == col("mcc_id")],
            "left_outer"
        )
    )
    transformed_df = (
        joined_df
        .transform(ut.format_amount_column, column_name="amount")
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .fillna("N/A")
        .select(ct.TRANSACTION_COLUMNS)
    )
    (transformed_df.write
     .partitionBy("year", "month")
     .mode("overwrite")
     .parquet(f"{TRANSFORM_PATH}/transactions")
     )

    return transformed_df

def transform_card_data(df: DataFrame) -> DataFrame:
    """
    Transforms dataset with card data and save data as parquet files.
    :param df: Raw card dataframe.
    :return: Transformed card dataset.
    """
    transformed_df = (
        df
        .transform(ut.format_date_column, date_column_name="expires", new_column_name="expires_date")
        .transform(ut.format_date_column, date_column_name="acct_open_date", new_column_name="acct_open_date")
        .transform(ut.format_amount_column, column_name="credit_limit")
        .select(ct.CARD_COLUMNS)
    )
    transformed_df.write.mode("overwrite").parquet(f"{TRANSFORM_PATH}/card_data")
    return transformed_df

def transform_users_data(df: DataFrame) ->DataFrame:
    """
    Transforms dataset with users data and save data as parquet files.
    :param df: Raw users dataframe.
    :return: Transformed users dataset.
    """
    transformed_df = (
        df
        .transform(ut.format_amount_column, "yearly_income")
        .transform(ut.format_amount_column, "per_capita_income")
        .transform(ut.format_amount_column, "total_debt")
        .withColumn("current_age", col("current_age").cast(IntegerType()))
        .select(ct.USER_COLUMNS)
    )
    transformed_df.write.mode("overwrite").parquet(f"{TRANSFORM_PATH}/users_data")
    return transformed_df

def transform_mmc_codes(df: DataFrame) -> DataFrame:
    """
    Transforms dataset with merchant industry data.
    :param df: Raw mmc_codes dataframe.
    :return: DataFrame with mcc codes.
    """
    transformed_df = (
        df
        .withColumn("id", col("id").cast(IntegerType()))
        .withColumn("name", trim("name"))
    )
    return transformed_df

def transformation(spark: SparkSession):
    raw_transaction = ut.read_csv_data(
        spark, f"{RAW_PATH}/transactions_data.csv", ct.TRANSACTION_SCHEMA
    )
    raw_users = ut.read_csv_data(
        spark, f"{RAW_PATH}/users_data.csv", ct.USERS_SCHEMA
    )
    raw_cards = ut.read_csv_data(
        spark, f"{RAW_PATH}/cards_data.csv", ct.CARDS_SCHEMA
    )

    raw_mcc_codes = ut.read_json_data(
        spark, f"{RAW_PATH}/mcc_codes.json",
    )

    mmc_df = transform_mmc_codes(raw_mcc_codes)
    transform_transactions_data(raw_transaction, mmc_df)
    transform_users_data(raw_users)
    transform_card_data(raw_cards)


if __name__ == "__main__":
    spark_session = (
        SparkSession.builder
        .appName("Finance Transactions")
        .getOrCreate()
    )
    transformation(spark_session)
