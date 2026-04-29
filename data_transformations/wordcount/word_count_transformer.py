import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812


def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    logging.info("Reading text file from: %s", input_path)
    input_df = spark.read.text(input_path)
    input_df = input_df.select(F.explode(F.split(F.col("value"), " ")).alias("word"))
    input_df = input_df.groupBy("word").count()

    logging.info("Writing csv to directory: %s", output_path)

    input_df.coalesce(1).write.csv(output_path, header=True)
