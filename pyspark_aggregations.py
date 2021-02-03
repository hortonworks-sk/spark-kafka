from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import *

spark = SparkSession \
		.builder \
		.appName("Streaming Example") \
		.getOrCreate()


df = spark \
		.readStream \
		.format("rate") \
		.option("rowsPerSecond", 100) \
		.load()

windowedDf = df.groupBy(window("timestamp", "5 seconds", "5 seconds")) \
				.sum()


query = windowedDf.writeStream \
			.outputMode("complete") \
			.format("console") \
      		.option("truncate", "false") \
			.start() 

query.awaitTermination()

