from pyspark.sql.types import *


TRANSACTION_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("date", DateType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("card_id", IntegerType(), True),
    StructField("amount", StringType(), True),
    StructField("use_chip", StringType(), True),
    StructField("merchant_id", IntegerType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("zip", IntegerType(), True),
    StructField("mcc", IntegerType(), True),
])

USERS_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("current_age", IntegerType(), True),
    StructField("retirement_age", IntegerType(), True),
    StructField("birth_year", IntegerType(), True),
    StructField("birth_month", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
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
    StructField("card_number", StringType(), True),
    StructField("expires", StringType(), True),
    StructField("cvv", IntegerType(), True),
    StructField("has_chip", StringType(), True),
    StructField("num_cards_issued", IntegerType(), True),
    StructField("credit_limit", StringType(), True),
    StructField("acct_open_date", StringType(), True)
])

TRANSACTION_COLUMNS = [
    "id",
    "date",
    "client_id",
    "card_id",
    "amount",
    "use_chip",
    "merchant_id",
    "mcc_id",
    "merchant_city",
    "merchant_state",
    "merchant_industry",
    "currency",
    "year",
    "month"
]

CARD_COLUMNS = [
    "id",
    "client_id",
    "card_brand",
    "card_type",
    "card_number",
    "expires_date",
    "has_chip",
    "credit_limit",
    "currency",
    "acct_open_date",
]

USER_COLUMNS = [
    "id",
    "current_age",
    "retirement_age",
    "birth_year",
    "birth_month",
    "gender",
    "latitude",
    "longitude",
    "per_capita_income",
    "yearly_income",
    "total_debt",
    "credit_score",
    "currency",
]