from __future__ import annotations
import json, re
from pathlib import Path
from typing import Dict, Any

from pyspark.sql import SparkSession, functions as F, types as T

def load_config() -> Dict[str, Any]:
    """
    Load config.json
    """
    base = Path("config.json")
    cfg: Dict[str, Any] = json.loads(base.read_text(encoding="utf-8")) if base.exists() else {}

    # defaults
    paths = cfg.setdefault("paths", {})
    paths.setdefault("landing", "landing")
    paths.setdefault("parquet", {"uv": "parquet/uv", "pv": "parquet/pv"})
    paths.setdefault("chk", {"uv": "chk/uv", "pv": "chk/pv"})

    stream = cfg.setdefault("stream", {})
    windowing = stream.setdefault("windowing", {})
    windowing.setdefault("window_size", "5m")
    windowing.setdefault("watermark", "5m")
    windowing.setdefault("trigger", "10s")

    io = stream.setdefault("io", {})
    io.setdefault("output_mode", "append")
    io.setdefault("format", "parquet")
    io.setdefault("compression", "snappy")
    io.setdefault("partitioning", {"type": "time", "by": ["date", "hour"]})
    io.setdefault("maxFilesPerTrigger", 100)
    
    sparks = cfg.setdefault("sparksession", {})
    sparks.setdefault("app_name", "STREAM-SKETCHES")
    sparks.setdefault("partitions", "8")

    return cfg


def to_spark_interval(s: str) -> str:
    """Convert '5m' -> '5 minutes', '10s' -> '10 seconds' for Spark APIs
    Format required: <digits><unit>, unit in {s,m,h,d}
    """
    t = s.strip()
    u = t[-1].lower()
    num = t[:-1]
    if (len(t) < 2) or (u not in ("s", "m", "h", "d")) or (not num.isdigit()):
        raise ValueError("Invalid interval: expected <digits><unit>, e.g., '5m'.")

    n = int(num)
    name_by_unit = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
    return f"{n} {name_by_unit[u]}"


def build_spark(app_name: str = "STREAM-SKETCHES", partitions: int = 8) -> SparkSession:
    """Create a local SparkSession (local[*]) for development with app name and shuffle partitions."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", partitions)
        .getOrCreate()
    )
    
    
SCHEMA = T.StructType([
    T.StructField("user_id", T.StringType(), nullable=False),
    T.StructField("page_id", T.StringType(), nullable=False),
    T.StructField("event_time", T.TimestampType(), nullable=False),  # ISO8601 -> timestamp
])


def read_file_stream(spark: SparkSession, landing_dir: str, max_files_per_trigger: int):
    """
    Read JSONL files incrementally from dir landing
    One line equals to one JSON object
    """
    return (
        spark.readStream.format("json")
        .schema(SCHEMA)
        .option("multiLine", "false")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .load(landing_dir)
    )


def make_uv(df, window_size: str, watermark: str):
    """
    Windowed unique visitors using HLL++ (approx_count_distinct).
    Output columns: window_start, window_end, uv
    """
    wsize = to_spark_interval(window_size)
    wmark = to_spark_interval(watermark)

    return (
        df.withWatermark("event_time", wmark)
          .groupBy(F.window("event_time", wsize))
          .agg(F.approx_count_distinct("user_id").alias("uv"))
          .select(
              F.col("window.start").alias("window_start"),
              F.col("window.end").alias("window_end"),
              F.col("uv")
          )
    )


def make_pv(df, window_size: str, watermark: str):
    """
    Window + page_id counts (page views)
    Output columns: window_start, window_end, page_id, pv
    """
    wsize = to_spark_interval(window_size)
    wmark = to_spark_interval(watermark)

    return (
        df.withWatermark("event_time", wmark)
          .groupBy(F.window("event_time", wsize), F.col("page_id"))
          .count()
          .withColumnRenamed("count", "pv")
          .select(
              F.col("window.start").alias("window_start"),
              F.col("window.end").alias("window_end"),
              F.col("page_id"),
              F.col("pv")
          )
    )


def add_time_partitions(df):
    """
    Add date/hour columns derived from window_start for partitioning.
    """
    return (
        df.withColumn("date", F.to_date("window_start"))
          .withColumn("hour", F.hour("window_start"))
    )
    
    
def write_parquet_stream(df, out_path: str, chk_path: str, trigger_interval: str, output_mode: str = "append"):
    """
    Common writer: append Parquet + checkpoint + time partitions + processingTime trigger.
    """
    trigger = to_spark_interval(trigger_interval)
    return (
        df.writeStream
          .outputMode(output_mode)
          .format("parquet")
          .option("path", out_path)
          .option("checkpointLocation", chk_path)
          .option("compression", "snappy")
          .partitionBy("date", "hour")
          .trigger(processingTime=trigger)
          .start()
    )


def main():
    cfg = load_config()
    paths = cfg["paths"]
    landing = paths["landing"]
    out_uv = paths["parquet"]["uv"]
    out_pv = paths["parquet"]["pv"]
    chk_uv = paths["chk"]["uv"]
    chk_pv = paths["chk"]["pv"]
    
    app_name = cfg["sparksession"]["app_name"]
    partitions = str(cfg["sparksession"]["partitions"])

    windowing = cfg["stream"]["windowing"]
    io = cfg["stream"]["io"]

    # Prepare dirs
    for p in [out_uv, out_pv, chk_uv, chk_pv]:
        Path(p).mkdir(parents=True, exist_ok=True)

    spark = build_spark(app_name=app_name, partitions=partitions)

    # Source
    src = read_file_stream(spark, landing, io.get("maxFilesPerTrigger", 100))

    # Aggregations
    uv_df = make_uv(src, windowing["window_size"], windowing["watermark"])
    pv_df = make_pv(src, windowing["window_size"], windowing["watermark"])

    # Partitions
    uv_part = add_time_partitions(uv_df)
    pv_part = add_time_partitions(pv_df)

    # Sinks
    q_uv = write_parquet_stream(
        uv_part, out_uv, chk_uv, trigger_interval=windowing["trigger"], output_mode=io["output_mode"]
    )
    q_pv = write_parquet_stream(
        pv_part, out_pv, chk_pv, trigger_interval=windowing["trigger"], output_mode=io["output_mode"]
    )

    print(f"[spark_job] reading from {landing}")
    print(f"[spark_job] writing UV -> {out_uv} (chk={chk_uv})")
    print(f"[spark_job] writing PV -> {out_pv} (chk={chk_pv})")
    print(f"[spark_job] window={windowing['window_size']} watermark={windowing['watermark']} trigger={windowing['trigger']}")

    # Block
    q_uv.awaitTermination()
    q_pv.awaitTermination()


if __name__ == "__main__":
    main()