from pyspark.sql import SparkSession
from pyspark.sql import Row
import openai
import re

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "invoice"

spark = SparkSession.builder.appName("write_test_stream").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

# Set your OpenAI API key
api_key = 'sk-PX8etQiUGFDSDTYUIKLJNBVCFGHJK45678UIHJGFJHGFR56789V' # change to your OpenAI API key
openai.api_key = api_key

# Define the JSON template
json_template = {"BillNum": "93647513","CreatedTime": 1595688902254,"StoreID": "STR8510","PaymentMode": "CARD","TotalValue": 9687.09}


# Generate random JSON data with ai
responses = []

for _ in range(50):
    response = openai.Completion.create(
        engine="text-davinci-002",
        prompt=f"Generate a random JSON payload based exactly on this template while respecting the value type: {json_template}",
        max_tokens=100
    )
    jsonResponse=re.sub(r'\s', '', response.choices[0].text.replace("'", '"'))
    responses.append(jsonResponse)

# Create a DataFrame from the generated JSON 
df = spark.createDataFrame([Row(value=x) for x in responses])

# write to kafka topic 
df.selectExpr("CAST(value AS STRING)") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
  .option("topic", KAFKA_TOPIC) \
  .save()

