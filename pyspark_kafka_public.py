from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka_broker_hostname:9093") \
  .option("subscribe", "topic1") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.ssl.truststore.location", "/path/to/keystore.jks") \
  .option("kafka.ssl.truststore.password", "mypassword") \
  .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="skiaie" password="mypassword";') \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


query = df.writeStream \
  .outputMode("append") \
  .format("console") \
  .start() \
  
query.awaitTermination()