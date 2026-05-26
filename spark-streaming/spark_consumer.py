from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# structured streaming application to consume from Kafka topic "processed_orders"
# and perform analytics on the data
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()


schema = StructType([
    StructField("order_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("product", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_time", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "processed_orders") \
    .load()


parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


analytics_df = parsed_df.groupBy("product") \
    .agg(
        count("*").alias("total_orders"),
        sum("amount").alias("total_sales")
    )


query = analytics_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()


query.awaitTermination()