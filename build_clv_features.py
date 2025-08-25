from pyspark.sql import SparkSession, functions as F

def run():
    spark = SparkSession.builder.appName("BuildCLVFeatures").getOrCreate()
    df = spark.read.parquet("/data/bronze/orders")
    feats = (df.groupBy("customer_id")
             .agg(F.countDistinct("order_id").alias("num_orders"),
                  F.sum("amount").alias("total_spent"),
                  F.avg("amount").alias("avg_order_value"),
                  F.countDistinct("product_id").alias("unique_products"))
            )
    feats = feats.withColumn("label", F.col("total_spent") * 0.2 + F.col("num_orders") * 5.0)
    feats.write.mode("overwrite").parquet("/data/gold/clv_features")
    spark.stop()

if __name__ == "__main__":
    run()