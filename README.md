# Streaming Clicks — UV & Top-N on Clickstream (PySpark + Parquet + DuckDB)

A laptop-runnable clickstream pipeline that demonstrates production semantics: event-time windowing with watermark, append-only file sinks, HLL++ approximate distinct, checkpointed recovery, and local verification with DuckDB.

## Goal

Process JSONL click events in 5-minute tumbling windows (event time). Output:
• UV per window using HLL++ (approx_count_distinct over user_id)  
• PV per window × page_id (counts)  
Write results to Parquet (append) with time-based partitioning and separate checkpoints. Validate with DuckDB.

## Architecture

Generator (JSONL, rolling 1s) → landing/  
→ spark_job.py (Structured Streaming)  
 • parse event_time (UTC TIMESTAMP)  
 • withWatermark(event_time, 5 minutes)  
 • window = 5 minutes (tumbling)  
 • UV → parquet/uv/ (append, time partitions, checkpoint chk/uv/)  
 • PV → parquet/pv/ (append, time partitions, checkpoint chk/pv/)  
→ DuckDB (queries.sql)  
 • window UV sanity (baseline vs HLL)  
 • window Top-N from parquet/pv/

## Quickstart (3 steps)

1. Start the generator: write one .jsonl per second (multiple lines per file) into landing/. Fields: user_id, page_id, event_time (ISO8601 UTC). The generator supports an optional negative time offset to inject late events.
2. Run the streaming job: read landing/ as a file stream (schema: user_id STRING, page_id STRING, event_time TIMESTAMP). Apply withWatermark(event_time, 5 minutes) and 5-minute tumbling windows. Produce two sinks:  
   • parquet/uv/ (UV per window; append; partition by date/hour or window_start; checkpoint at chk/uv/)  
   • parquet/pv/ (PV per window × page_id; append; partition by date/hour or window_start; checkpoint at chk/pv/)
3. Validate with DuckDB using queries.sql:  
   • Compare window UV baseline vs HLL result (allow small error)  
   • Compute window Top-N pages: order by pv desc limit N

Example commands (adapt to your shell):  
$ python gen_clicks.py  
$ spark-submit spark_job.py  
$ duckdb -c "SELECT _ FROM parquet_scan('parquet/uv/_.parquet') LIMIT 20;"  
$ duckdb -c "SELECT _ FROM parquet_scan('parquet/pv/_.parquet') WHERE window_start='2025-08-27 12:00:00' ORDER BY pv DESC LIMIT 10;"

## Data Model

• user_id STRING — user identifier (demo: random)  
• page_id STRING — page/url id  
• event_time TIMESTAMP (UTC) — parsed from source JSONL; derived from raw timestamps if present

## Design Choices

• File sinks require append with a watermark; results emit only when a window finalizes (sealed).  
• Time-only partitioning (date/hour or window_start) avoids high-cardinality directory explosion; do not partition by page_id.  
• HLL++ trades a few percent error for fixed memory and high throughput; use exact distinct only where cardinality is low or accuracy is critical.  
• Separate checkpoints per stream (chk/uv, chk/pv) ensure restart without re-writing finalized windows.

## Repo Layout

streaming-clicks/  
 gen_clicks.py — JSONL generator (rolling 1s, multi-line per file, optional late offset)  
 spark_job.py — Streaming job: windowed UV and window×page PV → Parquet (append)  
 dq_checks.py — Minimal DQ: nulls, key duplicates, recent-partition completeness  
 landing/ — Source JSONL (git-ignored)  
 parquet/  
 uv/ — UV output (partitioned by time)  
 pv/ — PV output (partitioned by time)  
 chk/  
 uv/ — Checkpoint for UV stream  
 pv/ — Checkpoint for PV stream  
 queries.sql — DuckDB: window UV sanity, window Top-N  
 README.md

## How to Verify (Checklist)

• Files appear under parquet/uv/ and parquet/pv/ after a few micro-batches.  
• DuckDB queries return tables for UV and Top-N.  
• Late-event demo: inject events with event_time = now − 8 minutes.  
 – If watermark has not passed the window: the events are included.  
 – If watermark has passed: the events are dropped.  
• Restart demo: stop and restart the same job; finalized windows are not re-written (thanks to checkpoints).

## Lessons Learned

• Event time vs processing time: watermark defines lateness tolerance and finalization.  
• File sinks do not support complete; append + watermark is the correct pattern.  
• Small-file mitigation: JSONL rolling per second and time partitioning reduce metadata overhead.  
• Parquet + DuckDB enables fast local validation without extra services.

## DQ Minimum (dq_checks.py)

• Null checks for user_id, page_id, event_time.  
• Duplicate key check on (window_start, user_id, page_id) or another sensible composite key.  
• Recent partition completeness (last K windows have output).

## Troubleshooting (Windows)

• If you see UnsatisfiedLinkError: NativeIO$Windows, install matching Hadoop native libs (winutils.exe / hadoop.dll), set HADOOP_HOME, and ensure it is on PATH.  
• If no output appears, confirm outputMode=append, watermark and window are set, and the generator is writing to landing/.

## Roadmap (Optional)

• Streaming Top-N via foreachBatch (batch-local sort and top-k) or keep Top-N in DuckDB.  
• Feature/sample derivation (user/doc/ud features) for ML training.  
• Switch source to Kafka; write to Iceberg/Hudi with exactly-once sinks.
