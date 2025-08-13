from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("Batch-TopN")
         .master("local[*]")
         .getOrCreate())

schema = "uid STRING, page STRING, ts LONG"

# 读取静态数据（你已经积累了很多 json 文件了）
df = (spark.read.schema(schema).json("data/input_stream")
        .withColumn("event_time", (F.col("ts")/1000).cast("timestamp")))

# 如需只看最近10分钟，可取消下一行注释
# df = df.filter(F.col("event_time") >= F.expr("current_timestamp() - INTERVAL 10 MINUTES"))

topn = (df.groupBy("page").count()
          .orderBy(F.desc("count"))
          .limit(5))

# 1) 直接在终端打印，作为“Batch 运行截图”
topn.show(truncate=False)

# 2) 同时写出结果，便于 DuckDB/复查
(topn.coalesce(1)
     .write.mode("overwrite")
     .parquet("data/batch_topn/"))

spark.stop()
