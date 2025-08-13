# Stream Sketches — UV & Top-N on Clickstream 
*(Spark, HLL/CMS, Bloom, Parquet, DuckDB)*

A small, laptop-runnable clickstream pipeline:
- A generator writes JSON/JSONL events.
- A Structured Streaming job computes **UV** (distinct users) via **HLL++** in 10-second windows and appends **Parquet**.
- A batch job computes **Top-N pages** (prints a table and writes Parquet).

Default source is files; can switch to **Kafka** later. Advanced sketches (**CMS/Bloom**) are planned.

## Architecture
    Generator  -->  data/input_stream/ (JSON/JSONL)
                      |
                      v
    streaming_uv.py  [Structured Streaming]
      - ts -> event_time (timestamp)
      - window = 10s, watermark = 10s
      - approx_count_distinct(uid) -> UV
      - append Parquet + checkpoint -> data/stream_agg/
                      |
                      v
    batch_topn.py  (batch)
      - groupBy(page).count().orderBy desc -> Top-N
      - prints table + writes Parquet -> data/batch_topn/

## Quickstart
    # 1. Generate events
    python scripts/generate_clickstream.py

    # 2. Streaming UV (HLL++ -> Parquet, append mode)
    spark-submit jobs/streaming_uv.py
    # Spark UI: http://localhost:4040

    # 3. Batch Top-N (reads raw events, prints table, writes Parquet)
    spark-submit jobs/batch_topn.py

Screenshots (store under `docs/screenshots/`):
- `day0_stream_ui.png` – Spark UI (Streaming)
- `day0_batch_topn.png` – Console Top-N

DuckDB quick check (optional)
    duckdb -c "SELECT * FROM parquet_scan('data/stream_agg/*.parquet') LIMIT 20;"
    duckdb -c "SELECT * FROM parquet_scan('data/batch_topn/*.parquet') ORDER BY count DESC LIMIT 5;"

## Data Model
- `uid STRING` — user id (demo: random)
- `page STRING` — URL path
- `ts LONG` — event time in milliseconds

## Repo Layout
    scripts/                 # generator
    jobs/                    # streaming & batch
    data/                    # generated (git-ignored)
      input_stream/          # raw events
      stream_agg/            # UV parquet
    chk/                     # checkpoint (git-ignored)
    docs/screenshots/        # images for README
    README.md

## Design Choices (why)
- **File source + Parquet**: simplest on Windows; Parquet pairs well with DuckDB for local validation.
- **HLL++ (`approx_count_distinct`)**: near-exact UV with small memory; no extra jars.
- **Append + watermark**: file sinks don’t support `complete`; emit finalized windows only.
- **JSONL option**: rolling 1-second files reduces small-file storm.

## Status
- ✅ Streaming UV (HLL++) → Parquet (append + checkpoint)  
- ✅ Batch Top-N (print + Parquet)  
- 🟡 Streaming Top-N (WIP)  
- 🟡 CMS / Bloom (Advanced — planned)  
- 🟡 Kafka source (planned; file source by default)

## Lessons Learned
- On Windows, file sinks require **append** with **watermark**; `complete` is unsupported.
- Structured Streaming touches **checkpoints**; keep them out of git.
- Match Hadoop native libs (`winutils.exe`/`hadoop.dll`) or use **WSL2**.
- Short demo windows (10s) surface results quickly.
- Parquet + DuckDB = fast local verification in interviews.

## Roadmap
- Streaming Top-N (windowed `groupBy('page').count()` → append Parquet)
- CMS (Count-Min Sketch) with a tiny UDF + unit test; compare memory/accuracy
- Bloom filter for membership checks (e.g., bot/user lists)
- Partitioned Parquet by `date=`/`hour=` for faster scans
- Optional: switch source to **Kafka**
