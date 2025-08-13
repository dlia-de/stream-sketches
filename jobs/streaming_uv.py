from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("UV-Append")
         .master("local[*]")
         .getOrCreate())

schema = "uid STRING, page STRING, ts LONG"

# 1) 源：读取生成器写入的 JSON / JSONL
raw = spark.readStream.schema(schema).json("data/input_stream")

# 2) 事件时间
df = raw.withColumn("event_time", (F.col("ts")/1000).cast("timestamp"))

# 3) 窗口 + 水位线（缩短到 10s，便于快速看到输出）
agg = (df.withWatermark("event_time", "10 seconds")
         .groupBy(F.window("event_time", "10 seconds"))
         .agg(F.approx_count_distinct("uid").alias("uv"))
         .select(F.col("window.start").alias("win_start"),
                 F.col("window.end").alias("win_end"),
                 "uv"))

# 4) Sink：文件只支持 append
q = (agg.writeStream
        .outputMode("append")                  # ← 关键改动
        .format("parquet")
        .option("path", "data/stream_agg/")    # 输出目录
        .option("checkpointLocation", "chk/uv")# checkpoint
        .trigger(processingTime="5 seconds")
        .start())

q.awaitTermination()
