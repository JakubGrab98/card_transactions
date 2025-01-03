from pyspark.sql.functions import *
from pyspark.sql.types import *
import src.utils as ut
import src.const as ct


def transform_transactions_data(df: DataFrame) -> DataFrame:
    """
    Transforms dataset with transactions.
    :param df: Raw transaction dataframe.
    :return: Transformed transaction dataset.
    """
    transaction_df = (
        df
        .transform(ut.format_amount_column, column_name="amount")
        .withColumnRenamed("id", "natural_key")
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .fillna("N/A")
    )

    return transaction_df

def save_transaction_data(transaction_df: DataFrame) -> None:
    """
    Saves transaction DataFrame to parquet files.
    :param transaction_df: Transformed transaction DataFrame
    """
    transaction_df = transaction_df.select(ct.TRANSACTION_COLUMNS)
    (transaction_df.write
     .partitionBy("year", "month")
     .mode("overwrite")
     .parquet("s3a://financials/data/transform/transactions")
     )

def transform_merchant_data(transaction_df: DataFrame, mcc_df: DataFrame) -> None:
    """
    Select merchant distinct data and save as parquet files.
    :param transaction_df: Transformed transaction dataframe.
    :param mcc_df: Transformed mcc codes dataframe.

    """
    merchant_df = transaction_df.select(ct.MERCHANT_COLUMNS).distinct()
    transformed_df = (
        merchant_df
        .join(
            mcc_df,
            [col("mcc") == col("id")],
            "left_outer"
        )
    )
    (transformed_df.write
     .mode("overwrite")
     .parquet("s3a://financials/data/transform/merchants_data")
     )

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
        .withColumnRenamed("id", "natural_key")
        .select(
            [
                "natural_key", "client_id", "card_brand", "card_type",
                "card_number", "expires_date", "has_chip", "credit_limit",
                "currency", "acct_open_date",
            ]
        )
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
        .withColumnRenamed("id", "natural_key")
        .withColumn("current_age", col("current_age").cast(IntegerType()))
        .select(
            [
                "natural_key", "current_age", "retirement_age", "birth_year", "birth_month",
                "gender", "latitude", "longitude", "per_capita_income", "yearly_income",
                "total_debt", "credit_score", "currency",
            ]
        )
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
