import os

from pyspark.sql.types import StructType, StructField, TimestampType, LongType, DoubleType

from src.utils.local.dir_paths import get_parquet_dir, get_data_dir
from src.utils.local.spark_session import get_spark_session


def get_parquet_schema():
    schema = StructType([
        StructField("window", StructType([
            StructField("start", TimestampType(), nullable=True),
            StructField("end", TimestampType(), nullable=True)
        ]), nullable=False),
        StructField("sum_quantity", LongType(), nullable=True),
        StructField("sum_total_price", DoubleType(), nullable=True),
        StructField("sum_clicks", LongType(), nullable=True),
        StructField("sum_impressions", LongType(), nullable=True)
    ])
    return schema


def main(spark):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = get_data_dir(base_dir=base_dir)
    parquet_dir = get_parquet_dir(data_dir=data_dir)

    parquet_df = spark.read \
        .format("parquet") \
        .schema(get_parquet_schema()) \
        .load(parquet_dir)

    parquet_df.show()


if __name__ == "__main__":
    main(get_spark_session('check_output'))
