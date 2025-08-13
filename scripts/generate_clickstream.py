import json, random, time, uuid, pathlib, itertools
out = pathlib.Path("data/input_stream"); out.mkdir(parents=True, exist_ok=True)
pages = ["/", "/home", "/cart", "/prod/1", "/prod/2"]
for _ in itertools.count():
    evt = {"uid": uuid.uuid4().hex[:8], "page": random.choice(pages), "ts": int(time.time()*1000)}
    with open(out / f"{int(time.time()*1000)}.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(evt))
    time.sleep(0.2)
