from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def data_preparation_api(spark, product_hive_table, sales_hive_table, target_hive_table):
    # Load data from product and sales Hive tables
    product_df = spark.table(product_hive_table)
    sales_df = spark.table(sales_hive_table)

    # Filter product_id for S8 and iPhone
    s8_id = product_df.filter(col("product_name") == "S8").select("product_id").collect()[0][0]
    iphone_id = product_df.filter(col("product_name") == "iPhone").select("product_id").collect()[0][0]

    # Buyers who bought S8
    s8_buyers = sales_df.filter(col("product_id") == s8_id).select("buyer_id").distinct()

    # Buyers who bought iPhone
    iphone_buyers = sales_df.filter(col("product_id") == iphone_id).select("buyer_id").distinct()

    # Buyers who bought S8 but not iPhone
    s8_not_iphone = s8_buyers.join(iphone_buyers, "buyer_id", "left_anti")

    # Save the result into target Hive table
    s8_not_iphone.write.mode("overwrite").saveAsTable(target_hive_table)