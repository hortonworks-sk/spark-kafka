from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


spark = SparkSession \
		.builder \
		.appName("Streaming Example") \
		.getOrCreate()


df = spark \
		.readStream \
		.format("rate") \
		.option("rowsPerSecond", 100) \
		.load()


query = df.writeStream \
			.outputMore("append") \
			.format("console") \
			.start() 

query.awaitTermination()





