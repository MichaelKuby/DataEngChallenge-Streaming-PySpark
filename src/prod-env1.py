import os

from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType

from src.utils.local.spark_session import get_spark_session


def get_schema():
    schema = StructType([
        StructField("datetime", TimestampType(), True),
        StructField("sales", StructType([
            StructField("quantity", IntegerType(), True),
            StructField("total_price", DoubleType(), True)
        ]), True),
        StructField("analytics", StructType([
            StructField("clicks", IntegerType(), True),
            StructField("impressions", IntegerType(), True)
        ]), True)
    ])
    return schema


def main(spark):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "../data")
    json_dir = os.path.join(data_dir, "json_files")
    csv_dir = os.path.join(data_dir, "csv_files")
    checkpoint_dir = os.path.join(data_dir, "checkpoint_prod-env1")

    json_raw_df = spark.readStream \
        .schema(get_schema()) \
        .json(json_dir)

    json_flattened_df = json_raw_df \
        .select("datetime", "sales.*", "analytics.*")

    result_df = json_flattened_df \
        .writeStream \
        .format("csv") \
        .option("path", csv_dir) \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    result_df.awaitTermination()


if __name__ == "__main__":
    main(get_spark_session())
