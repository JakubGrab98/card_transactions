from pyspark.sql.functions import *
from pyspark.sql.types import *
import utils as ut


def transform_transactions_data(df: DataFrame) -> DataFrame:
    transaction_df = (
        df
        .transform(ut.format_amount_column, column_name="amount")
        .withColumnRenamed("id", "natural_key")
        .select(["natural_key", "date", "client_id", "card_id", "amount"])
    )

    return transaction_df

def transform_card_data(df: DataFrame) -> DataFrame:
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
    return transformed_df

def transform_users_data(df: DataFrame) ->DataFrame:
    transform_df = (
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
    return transform_df
