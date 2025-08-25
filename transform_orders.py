from pyspark.sql import SparkSession, functions as F

def run():
    spark = SparkSession.builder.appName("TransformOrdersRawToBronze").getOrCreate()

    orders = spark.read.option("header", True).csv("/data/raw_local/orders.csv")
    customers = spark.read.option("header", True).csv("/data/raw_local/customers.csv")
    products = spark.read.option("header", True).csv("/data/raw_local/products.csv")

    orders = orders.withColumn("order_date", F.to_date("order_date"))
    orders = orders.withColumn("quantity", F.col("quantity").cast("int"))
    orders = orders.withColumn("amount", F.col("amount").cast("double"))
    customers = customers.withColumn("signup_date", F.to_date("signup_date"))
    products = products.withColumn("price", F.col("price").cast("double"))

    bronze_orders = (
        orders
        .join(customers, "customer_id", "left")
        .join(products, "product_id", "left")
    )

    bronze_path = "/data/bronze/orders"
    bronze_orders.write.mode("overwrite").parquet(bronze_path)

    gold_daily = (bronze_orders
        .groupBy("order_date")
        .agg(F.sum("amount").alias("daily_revenue"))
        .orderBy("order_date")
    )
    gold_path = "/data/gold/daily_revenue"
    gold_daily.write.mode("overwrite").parquet(gold_path)

    spark.stop()

if __name__ == "__main__":
    run()