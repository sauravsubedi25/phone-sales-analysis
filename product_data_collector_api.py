def product_data_collector_api(spark, parquet_file_path):
    product_df = spark.read.parquet(parquet_file_path)
    hive_table = "default.product_table"
    product_df.write \
        .mode("overwrite") \
        .saveAsTable(hive_table)

    return hive_table