from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Smoke-RateConsole").master("local[*]").getOrCreate()
df = (spark.readStream.format("rate").option("rowsPerSecond", 5).load())
q = df.writeStream.format("console").start()
q.awaitTermination()
