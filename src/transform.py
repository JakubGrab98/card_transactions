from pyspark.sql.functions import *
from pyspark.sql.types import *
import src.utils as ut
import src.const as ct


def transform_transactions_data(transaction_df: DataFrame, mcc_df: DataFrame) -> DataFrame:
    """
    Transforms dataset with transactions.
    :param transaction_df: Raw transaction dataframe.
    :param mcc_df: DataFrame with mcc codes details.
    :return: Transformed transaction dataset.
    """
    joined_df = (
        transaction_df
        .join(
            broadcast(mcc_df),
            [col("mcc") == col("id")],
            "left_outer"
        )
        .withColumnRenamed("name", "merchant_industry")
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
     .parquet("s3a://financials/data/transform/transactions")
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
    transformed_df.write.mode("overwrite").parquet("s3a://financials/data/transform/card_data")
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
    transformed_df.write.mode("overwrite").parquet("s3a://financials/data/transform/users_data")
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
s