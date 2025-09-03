from __future__ import annotations
import json, sys
from pathlib import Path
from typing import Dict, Any

from pyspark.sql import SparkSession, functions as F, Window



def load_config() -> Dict[str, Any]:
    """
    Load config.json
    """
    base = Path("config.json")
    cfg: Dict[str, Any] = json.loads(base.read_text(encoding="utf-8")) if base.exists() else {}
    
    paths = cfg.setdefault("paths", {})
    paths.setdefault("parquet", {"pv": "parquet/pv", "topn": "parquet/topn"})
    
    cfg.setdefault("topn", {"k": 20})
    
    sparks = cfg.setdefault("sparksession", {})
    sparks.setdefault("app_name", "STREAM-SKETCHES")
    sparks.setdefault("partitions", "8")
    
    return cfg

def build_spark(app_name: str = "STREAM-SKETCHES", partitions: int = 8) -> SparkSession:
    """Create a local SparkSession (local[*]) for development with app name and shuffle partitions."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", partitions)
        .getOrCreate()
    )

def read_pv(spark: SparkSession, pv_path: str):
    """
    Read window x page PV parquet. Expect columns:
    window_start, window_end, page_id, pv
    """
    df = spark.read.parquet(pv_path)
    required = {"window_start", "window_end", "page_id", "pv"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"PV parquet missing columns: {missing}")
    return df


def compute_topn(pv_df, k: int):
    """
    For each window (window_start, window_end), rank pages by pv desc and keep top K.
    """
    w = Window.partitionBy("window_start", "window_end").orderBy(F.desc("pv"), F.asc("page_id"))
    ranked = pv_df.withColumn("rn", F.row_number().over(w))
    topn = ranked.where(F.col("rn") <= k).drop("rn")
    return topn


def add_time_partitions(df):
    """
    Ensure date/hour partition columns exist (derived from window_start).
    Safe if they already exist: we overwrite with same values.
    """
    return df.withColumn("date", F.to_date("window_start")).withColumn("hour", F.hour("window_start"))


# ---------- write & show ----------

def write_parquet(df, out_path: str):
    (
        df.write.mode("overwrite")  # overwrite whole topn dataset; deterministic batch output
          .partitionBy("date", "hour")
          .parquet(out_path)
    )

def show_latest_window(topn_df, k: int):
    latest = (topn_df.groupBy().agg(F.max("window_start").alias("max_ws")).collect()[0]["max_ws"])
    if latest is None:
        print("[topn] no data to show yet")
        return
    latest_df = topn_df.where(F.col("window_start") == F.lit(latest)).orderBy(F.desc("pv"), F.asc("page_id")).limit(k)
    print(f"\n[topn] latest window = {latest}")
    latest_df.select("window_start", "window_end", "page_id", "pv").show(truncate=False)


# ---------- main ----------

def main():
    cfg = load_config()
    pv_path = cfg["paths"]["parquet"]["pv"]
    topn_path = cfg["paths"]["parquet"]["topn"]
    k = int(cfg["topn"]["k"])
    
    app_name = cfg["sparksession"]["app_name"]
    partitions = str(cfg["sparksession"]["partitions"])

    # prepare dirs
    Path(topn_path).mkdir(parents=True, exist_ok=True)

    spark = build_spark(app_name=app_name, partitions=partitions)
    
    pv_df = read_pv(spark, pv_path)

    topn_df = compute_topn(pv_df, k)
    topn_part = add_time_partitions(topn_df)

    write_parquet(topn_part, topn_path)
    show_latest_window(topn_part, k)

    print(f"[topn] written to {topn_path}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
