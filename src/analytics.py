from pyspark.sql.functions import *


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
    client_summary.write.mode("overwrite").parquet("s3a://financials/data/presentation/client_summary")

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
    merchant_summary.write.mode("overwrite").parquet("s3a://financials/data/presentation/merchant_summary")

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
    card_summary.write.mode("overwrite").parquet("s3a://financials/data/presentation/card_summary")
