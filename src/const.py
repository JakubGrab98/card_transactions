from pyspark.sql.types import *


TRANSACTION_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("date", DateType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("card_id", IntegerType(), True),
    StructField("amount", StringType(), True)
])

USERS_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("current_age", IntegerType(), True),
    StructField("retirement_age", IntegerType(), True),
    StructField("birth_year", IntegerType(), True),
    StructField("birth_month", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("per_capita_income", StringType(), True),
    StructField("yearly_income", StringType(), True),
    StructField("total_debt", StringType(), True),
    StructField("credit_score", IntegerType(), True),
])

CARDS_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("client_id", IntegerType(), True),
    StructField("card_brand", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_number", LongType(), True),
    StructField("card_number", StringType(), True),
    StructField("expires", StringType(), True),
    StructField("has_chip", StringType(), True),
    StructField("credit_limit", StringType(), True),
    StructField("acct_open_date", StringType(), True)
])
