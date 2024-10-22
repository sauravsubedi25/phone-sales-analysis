from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType


def sales_data_collector_api(spark, text_file_path):
    schema = StructType([
        StructField("seller_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("buyer_id", IntegerType(), True),
        StructField("sale_date", DateType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", IntegerType(), True)
    ])

    sales_df = spark.read.format("csv") \
        .option("header", True) \
        .option("delimiter", "|") \
        .schema(schema) \
        .load(text_file_path)

    hive_table = "default.sales_partitioned_table"
    sales_df.write \
        .partitionBy("sale_date") \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(hive_table)

    return hive_table
