# Stream Sketches — 点击流的 UV 与 Top-N（Spark + Parquet + DuckDB）

一个本地即可复现的点击流小系统：
- 生成器持续写入 JSON/JSONL 事件；
- 流式作业在 10 秒窗口内用 **HLL++** 近似去重计算 **UV**，以 **append** 方式写 **Parquet**；
- 批处理脚本计算 **Top-N 页面**（终端打印并写 Parquet）。

默认输入为文件目录；后续可切换到 **Kafka**。高级功能 **CMS/Bloom** 规划中。

## 架构
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
    batch_topn.py  (批处理)
      - groupBy(page).count().orderBy desc -> Top-N
      - 终端打印 + 写 Parquet -> data/batch_topn/

## 快速开始
    python scripts/generate_clickstream.py        # 事件生成器
    spark-submit jobs/streaming_uv.py             # 流式 UV（HLL++）
    spark-submit jobs/batch_topn.py               # 批处理 Top-N
    # Spark UI: http://localhost:4040

截图（放到 `docs/screenshots/`）
- `day0_stream_ui.png` —— 流式 UI 页面
- `day0_batch_topn.png` —— 批处理 Top-N 输出

DuckDB 校验（可选）
    duckdb -c "SELECT * FROM parquet_scan('data/stream_agg/*.parquet') LIMIT 20;"
    duckdb -c "SELECT * FROM parquet_scan('data/batch_topn/*.parquet') ORDER BY count DESC LIMIT 5;"

## 数据模型
- `uid STRING` —— 用户标识（演示为随机）
- `page STRING` —— URL 路径
- `ts LONG` —— 事件时间（毫秒）

## 仓库结构
    scripts/                 # 生成器
    jobs/                    # 流式与批处理
    data/                    # 生成数据（git 忽略）
      input_stream/
      stream_agg/
    chk/                     # checkpoint（git 忽略）
    docs/screenshots/        # README 截图

## 设计取舍
- **文件源 + Parquet**：Windows 上最稳；Parquet 便于 DuckDB 本地核对。
- **HLL++（approx_count_distinct）**：低内存、近似去重，免额外依赖。
- **append + watermark**：文件类 sink 不支持 `complete`，只输出“封口”的窗口。
- **JSONL**：按秒滚动的 .jsonl 可减少小文件数量。

## 当前进度
- ✅ 流式 UV（HLL++）→ Parquet（append + checkpoint）
- ✅ 批处理 Top-N（打印 + Parquet）
- 🟡 流式 Top-N（进行中）
- 🟡 CMS / Bloom（高级计划）
- 🟡 Kafka 源（计划，可替换文件源）

## 学到的要点
- Windows 上文件 sink 必须 **append + watermark**；`complete` 会报错。
- Structured Streaming 一定会读写 **checkpoint**，不要纳入 git。
- 需要匹配版本的 Hadoop 原生库（winutils/hadoop.dll），或改用 WSL2。
- Demo 用短窗口（10s）更容易看到结果。
- Parquet + DuckDB 适合本地快速校验与面试演示。
