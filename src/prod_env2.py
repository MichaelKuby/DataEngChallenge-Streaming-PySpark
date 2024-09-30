import os

from pyspark.sql import functions as F

from src.utils.local.dir_paths import get_data_dir, get_csv_dir, get_checkpoint_dir, get_parquet_dir
from src.utils.local.spark_session import get_spark_session
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType


def get_csv_schema():
    schema = StructType([
        StructField("datetime", TimestampType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_price", DoubleType(), True),
        StructField("clicks", IntegerType(), True),
        StructField("impressions", IntegerType(), True)
    ])
    return schema


def main(spark):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = get_data_dir(base_dir=base_dir)
    csv_dir = get_csv_dir(data_dir=data_dir)
    parquet_dir = get_parquet_dir(data_dir=data_dir)
    checkpoint_dir = get_checkpoint_dir(data_dir=data_dir, checkpoint_dir_name="checkpoint_prod-env2")

    flattened_df = spark.readStream \
        .format('csv') \
        .schema(get_csv_schema()) \
        .load(csv_dir)

    grouped_df = flattened_df \
        .withWatermark(eventTime='datetime', delayThreshold='1 minute') \
        .groupBy(F.window(timeColumn='datetime', windowDuration='5 minutes')) \
        .agg(
            F.sum(col='quantity').alias('sum_quantity'),
            F.sum(col='total_price').alias('sum_total_price'),
            F.sum(col='clicks').alias('sum_clicks'),
            F.sum(col='impressions').alias('sum_impressions')
        )

    query = grouped_df \
        .writeStream \
        .format("parquet") \
        .option("path", parquet_dir) \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main(get_spark_session('prod-env2'))
