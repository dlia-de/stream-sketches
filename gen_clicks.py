from __future__ import annotations
import json
import random
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any

def load_config() -> Dict[str, Any]:
    """
    Load config.json
    """
    base = Path("config.json")
    cfg: Dict[str, Any] = {}
    if base.exists():
        cfg.update(json.loads(base.read_text(encoding="utf-8")))
        
    # defaults
    cfg.setdefault("paths", {}).setdefault("landing", "landing")
    cfg.setdefault("generator", {})
    
    generator = cfg["generator"]
    generator.setdefault("eps", 40)
    generator.setdefault("pages", 12)
    generator.setdefault("roll_seconds", 1)
    generator.setdefault("jitter_pct", 0.0)
    generator.setdefault("offset_mins", 0)
    generator.setdefault("seed", 114514)
    
    return cfg


def make_event(page_pool, offset: timedelta) -> Dict[str, str]:
    """
    Create one click event. event_time is UTC ISO8601 with milliseconds.
    eg. {"user_id": "u000123", "page_id": "/page/7", "event_time": "2025-08-27T12:00:05.123+00:00"}
    """
    now_utc = datetime.now(timezone.utc) + offset
    return {
        "user_id": f"u{random.randint(1, 999999):06d}",
        "page_id": random.choice(page_pool),
        "event_time": now_utc.isoformat(timespec="milliseconds")
    }
    

def utc_filename_for(ts_epoch: float) -> str:
    """
    Build a Windows-safe filename for a given epoch second in UTC:
    'YYYY-MM-DDTHH-MM-SS.jsonl'
    """
    dt = datetime.fromtimestamp(ts_epoch, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H-%M-%S.jsonl")


def main():
    cfg = load_config()
    
    landing_dir = Path(cfg["paths"]["landing"])  # output dir for JSONL files
    generator_cfg = cfg["generator"]
    
    eps = int(generator_cfg["eps"])  # events per second (fixed to make throughput reproducible)
    pages = int(generator_cfg["pages"])  # total number of distinct pages
    roll_seconds = int(generator_cfg["roll_seconds"])  # seconds per rolling file
    jitter_pct = float(generator_cfg["jitter_pct"]) # jitter percentage to simulate real situation
    offset_mins = int(generator_cfg["offset_mins"])  # global time offset in minutes; negative injects late events for watermark tests
    seed = int(generator_cfg["seed"])  # RNG seed


    # validation
    if eps <= 0 or pages <= 0 or roll_seconds <= 0:
        raise ValueError("eps, pages, roll_seconds must be positive integers")

    landing_dir.mkdir(parents=True, exist_ok=True)
    random.seed(seed)

    page_pool = [f"/page/{i}" for i in range(1, pages + 1)]
    offset = timedelta(minutes=offset_mins)

    total = 0
    print(f"[Click Generator] landing={landing_dir.resolve()} eps={eps} pages={pages} "
          f"roll_seconds={roll_seconds} offset_mins={offset_mins} seed={seed}")

    try:
        while True:
            # current epoch seconds, align to current rolling window start
            now = time.time()
            window_start = now - (now % roll_seconds)          # e.g., 12:00:05 when roll_seconds=5
            window_end = window_start + roll_seconds

            # open file for this window (UTC-based filename)
            file_path = landing_dir / utc_filename_for(window_start)

            
            # write EPS * roll_seconds * (1 + jitter_percentage) events into this file (append mode)
            noise = max(-jitter_pct, min(jitter_pct, random.gauss(0, jitter_pct/2)))
            factor = 1.0 + noise
            lines_to_write = max(1, int(round(eps * roll_seconds * factor)))
            
            with file_path.open("a", encoding="utf-8") as f:
                for _ in range(lines_to_write):
                    evt = make_event(page_pool, offset)
                    f.write(json.dumps(evt, ensure_ascii=False) + "\n")
                    
                    total += 1

            # sleep until the exact next window boundary to keep stable cadence
            remaining = max(0.0, window_end - time.time())
            time.sleep(remaining)

    except KeyboardInterrupt:
        # exit with Ctrl + C
        print(f"\n[Click Generator] stopped. total events written: {total}")


if __name__ == "__main__":
    main()