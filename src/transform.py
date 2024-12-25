from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName("transform").getOrCreate()

df = (
    spark.read
    .option("header", "true")
    .csv("C:/Users/kubag/PycharmProjects/financial_transactions/data/raw/cards_data.csv"))



def format_amount_column(df: DataFrame, column_name: str) -> DataFrame:
    transform_df = (
        df
        .withColumn("currency", substring(column_name, 1, 1))
        .withColumn(
            column_name, substring(column_name, 2, len(column_name))
            .cast(DoubleType())
        )
    )
    return transform_df

def format_date_column(
        df: DataFrame, date_column_name: str, new_column_name: str, date_format: str="yyyy-MM-dd"
) -> DataFrame:
    transform_df = (
        df
        .withColumn(
            date_column_name,
            to_date(
                concat_ws(
                    "-",
                    col(date_column_name).substr(4, 4),
                    col(date_column_name).substr(1, 2),
                    lit("01")
                ),
                date_format
            )
        )
        .withColumn(new_column_name, last_day(col(date_column_name)).cast(DateType()))
    )
    return transform_df

def transform_transactions_data(df: DataFrame) -> DataFrame:
    transaction_df = (
        df
        .withColumn(
            "date_id",
            regexp_replace(substring("date", 1, 10), "-", "")
            .cast(IntegerType())
        )
        .transform(format_amount_column, column_name="amount")
        .withColumnRenamed("id", "natural_key")
        .select(["natural_key", "date_id", "client_id", "card_id", "amount"])
    )

    return transaction_df

def transform_card_data(df: DataFrame) -> DataFrame:
    transformed_df = (
        df
        .transform(format_date_column, date_column_name="expires", new_column_name="expires_date")
        .transform(format_date_column, date_column_name="acct_open_date", new_column_name="acct_open_date")
        .transform(format_amount_column, column_name="credit_limit")
        .withColumnRenamed("id", "natural_key")
        .select(
            [
                "natural_key", "client_id", "card_brand", "card_type",
                "card_number", "expires_date", "has_chip", "credit_limit",
                "currency", "acct_open_date",
            ]
        )
    )
    return transformed_df
