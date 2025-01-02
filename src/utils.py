from pyspark.sql.types import *
from pyspark.sql.functions import *


def format_amount_column(df: DataFrame, column_name: str) -> DataFrame:
    transform_df = (
        df
        .withColumn(
            "currency",
            regexp_replace(
                substring(column_name, 1, 1),
                r"\$",
                "USD"
            )
        )
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
                concat_ws("-",
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
