from __future__ import annotations
import json, re, sys
from pathlib import Path
from typing import Dict, Any, Tuple, List

import duckdb

def load_config() -> Dict[str, Any]:
    """Load config.json and provide sane defaults used by DQ."""
    cfg = {}
    p = Path("config.json")
    if p.exists():
        cfg = json.loads(p.read_text(encoding="utf-8"))
    paths = cfg.setdefault("paths", {})
    paths.setdefault("parquet", {"uv": "parquet/uv", "pv": "parquet/pv"})
    cfg.setdefault("dq", {"recent_partitions_lookback": "30m"})
    cfg.setdefault("stream", {}).setdefault("windowing", {"window_size": "5m"})
    return cfg

def to_duck_interval(s: str) -> str:
    """'5m' -> '5 minutes', '10s' -> '10 seconds' (DuckDB INTERVAL literal)."""
    m = re.fullmatch(r"\s*(\d+)\s*([smhdSMHD])\s*", s or "")
    if not m:
        return s
    n = int(m.group(1))
    unit = {"s":"seconds","m":"minutes","h":"hours","d":"days"}[m.group(2).lower()]
    return f"{n} {unit}"

# ---------------------- scanners ----------------------

def scan_uv(conn: duckdb.DuckDBPyConnection, path: str):
    """Create a view uv as reading all UV parquet files."""
    conn.execute(f"""
        CREATE OR REPLACE VIEW uv AS
        SELECT window_start, window_end, uv
        FROM parquet_scan('{Path(path)}/**/*.parquet')
    """)

def scan_pv(conn: duckdb.DuckDBPyConnection, path: str):
    """Create a view pv as reading all PV parquet files."""
    conn.execute(f"""
        CREATE OR REPLACE VIEW pv AS
        SELECT window_start, window_end, page_id, pv
        FROM parquet_scan('{Path(path)}/**/*.parquet')
    """)

# ---------------------- checks ----------------------

def check_has_data(conn) -> Tuple[bool, str]:
    """UV/PV should not be empty."""
    uv_cnt = conn.sql("SELECT COUNT(*) FROM uv").fetchone()[0]
    pv_cnt = conn.sql("SELECT COUNT(*) FROM pv").fetchone()[0]
    ok = uv_cnt > 0 and pv_cnt > 0
    return ok, f"has_data uv={uv_cnt} pv={pv_cnt}"

def check_no_nulls(conn) -> Tuple[bool, str]:
    """No NULLs in key/value columns."""
    uv_bad = conn.sql("SELECT COUNT(*) FROM uv WHERE window_start IS NULL OR window_end IS NULL OR uv IS NULL").fetchone()[0]
    pv_bad = conn.sql("SELECT COUNT(*) FROM pv WHERE window_start IS NULL OR window_end IS NULL OR page_id IS NULL OR pv IS NULL").fetchone()[0]
    ok = (uv_bad == 0 and pv_bad == 0)
    return ok, f"no_nulls uv_bad={uv_bad} pv_bad={pv_bad}"

def check_no_duplicates(conn) -> Tuple[bool, str]:
    """Each window has at most one UV row; PV has at most one row per (window,page_id)."""
    uv_dupe = conn.sql("""
        SELECT COUNT(*) FROM (
          SELECT window_start, window_end, COUNT(*) c FROM uv
          GROUP BY 1,2 HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    pv_dupe = conn.sql("""
        SELECT COUNT(*) FROM (
          SELECT window_start, window_end, page_id, COUNT(*) c FROM pv
          GROUP BY 1,2,3 HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    ok = (uv_dupe == 0 and pv_dupe == 0)
    return ok, f"no_duplicates uv_dupe={uv_dupe} pv_dupe={pv_dupe}"

def check_recent_window_coverage(conn, lookback: str) -> Tuple[bool, str]:
    """
    Within recent lookback, every UV window should have PV rows (at least one page).
    This checks cross-sink consistency instead of exact expected count.
    """
    lb = to_duck_interval(lookback)
    # windows in lookback seen in UV
    conn.execute(f"""
        CREATE OR REPLACE VIEW uv_recent AS
        SELECT window_start, window_end
        FROM uv
        WHERE window_end >= now() - INTERVAL '{lb}'
    """)
    missing = conn.sql("""
        SELECT COUNT(*) FROM uv_recent u
        LEFT JOIN (
            SELECT DISTINCT window_start, window_end FROM pv
        ) p USING(window_start, window_end)
        WHERE p.window_start IS NULL
    """).fetchone()[0]
    ok = (missing == 0)
    return ok, f"recent_window_coverage missing_uv_windows_without_pv={missing} lookback={lookback}"

def check_values_non_negative(conn) -> Tuple[bool, str]:
    """UV/PV should be non-negative."""
    uv_bad = conn.sql("SELECT COUNT(*) FROM uv WHERE uv < 0").fetchone()[0]
    pv_bad = conn.sql("SELECT COUNT(*) FROM pv WHERE pv < 0").fetchone()[0]
    ok = (uv_bad == 0 and pv_bad == 0)
    return ok, f"non_negative uv_bad={uv_bad} pv_bad={pv_bad}"

# ---------------------- main ----------------------

def main() -> int:
    cfg = load_config()
    uv_path = cfg["paths"]["parquet"]["uv"]
    pv_path = cfg["paths"]["parquet"]["pv"]
    lookback = cfg["dq"].get("recent_partitions_lookback", "30m")

    # quick existence check
    if not any(Path(uv_path).rglob("*.parquet")) or not any(Path(pv_path).rglob("*.parquet")):
        print("[DQ] No parquet files found yet (uv/pv). Produce some data first.", file=sys.stderr)
        return 1

    conn = duckdb.connect()
    scan_uv(conn, uv_path)
    scan_pv(conn, pv_path)

    checks = [
        ("has_data",                check_has_data),
        ("no_nulls",                check_no_nulls),
        ("no_duplicates",           check_no_duplicates),
        ("recent_window_coverage",  lambda c: check_recent_window_coverage(c, lookback)),
        ("non_negative",            check_values_non_negative),
    ]

    failures: List[str] = []
    for name, fn in checks:
        ok, msg = fn(conn)
        print(f"[DQ] {name}: {'OK' if ok else 'FAIL'} - {msg}")
        if not ok:
            failures.append(name)

    if failures:
        print(f"[DQ] FAILED checks: {', '.join(failures)}", file=sys.stderr)
        return 1

    print("[DQ] All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())