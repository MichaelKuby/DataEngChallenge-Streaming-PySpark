import os

from src.utils.local.dir_paths import get_data_dir, get_json_dir, get_csv_dir, get_checkpoint_dir
from src.utils.local.spark_session import get_spark_session
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType


def get_json_schema():
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
    data_dir = get_data_dir(base_dir=base_dir)
    json_dir = get_json_dir(data_dir=data_dir)
    csv_dir = get_csv_dir(data_dir=data_dir)
    checkpoint_dir = get_checkpoint_dir(data_dir=data_dir, checkpoint_dir_name="checkpoint_prod-env1")

    json_raw_df = spark.readStream \
        .format("json") \
        .schema(get_json_schema()) \
        .load(json_dir)

    json_flattened_df = json_raw_df \
        .select("datetime", "sales.*", "analytics.*")

    flattened_result_df = json_flattened_df \
        .writeStream \
        .format("csv") \
        .option("path", csv_dir) \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    flattened_result_df.awaitTermination()


if __name__ == "__main__":
    main(get_spark_session('prod-env1'))
