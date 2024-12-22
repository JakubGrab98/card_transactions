from pyspark.sql import SparkSession


def read_csv_data(spark: SparkSession, path: str):
    """
    Reades csv files stored from configured object store.
    :param spark: SparkSession
    :param path: Path to csv file with data.
    :return: PySpark DataFrame.
    """
    df = (spark.read
          .option("header", "true")
          .csv(path)
    )
    return df