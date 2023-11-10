from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_SOURCE = "invoice"

spark = SparkSession.builder.appName("read_test_stream").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC_SOURCE) \
    .option("startingOffsets", "earliest") \
    .load()

json_schema = StructType([

  StructField("BillNum", StringType()),

  StructField("CreatedTime", TimestampType()),

  StructField("StoreID", StringType()),

  StructField("PaymentMode", StringType()),

  StructField("TotalValue", DoubleType()),

])


# df is our DataFrame reading from Kafka above

json_df = df.select(from_json(col("value").cast("string"), json_schema).alias("value"))

json_df.printSchema()

json_df.select("value.*").createOrReplaceTempView("invoice_view")

totalCash = spark.sql("SELECT StoreID, SUM(TotalValue) AS Total FROM invoice_view WHERE PaymentMode = 'CASH' GROUP BY StoreID")

# Write the streaming result to the console
query = totalCash.writeStream.outputMode("complete").format("console").start()

# Wait for the streaming query to terminate
query.awaitTermination()


