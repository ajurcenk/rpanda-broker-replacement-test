"""read_check.py record|verify <label> <topic/partition>... : read the first 1,000 records of each
partition from the earliest offset, hash them, and record (or compare with the recorded digest).
Also records the current leader and read latency."""
import hashlib
import json
import os
import sys
import time

from confluent_kafka import Consumer, TopicPartition
from rplib import BOOTSTRAP, RESULTS, any_admin, die, now, write_json

mode, label, tps = sys.argv[1], sys.argv[2], sys.argv[3:]
DIG = f"{RESULTS}/raw/earliest-digests.json"
digs = json.load(open(DIG)) if os.path.exists(DIG) else {}
out, failures = [], []
for tp in tps:
    t, p = tp.rsplit("/", 1)
    p = int(p)
    leader = any_admin(f"/v1/partitions/kafka/{t}/{p}").get("leader_id")
    t0 = time.time()
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "rpbr-readcheck", "enable.auto.commit": False})
    lo, hi = c.get_watermark_offsets(TopicPartition(t, p), timeout=30)
    c.assign([TopicPartition(t, p, lo)])
    h = hashlib.sha256()
    got = 0
    first = None
    while got < 1000 and time.time() - t0 < 120:
        for m in c.consume(500, 1.0):
            if m.error() or got >= 1000:
                continue
            first = m.offset() if first is None else first
            h.update(m.value())
            got += 1
    c.close()
    dt = time.time() - t0
    d = h.hexdigest()
    row = {"tp": tp, "leader": leader, "earliest": lo, "first_offset": first, "records": got, "sha256": d, "read_sec": round(dt, 3)}
    if mode == "record":
        digs[tp] = d
    else:
        row["match"] = digs.get(tp) == d
        if not row["match"]:
            failures.append(f"{tp}: digest mismatch (recorded {digs.get(tp)}, now {d})")
    if got < 1000:
        failures.append(f"{tp}: only {got} records read from earliest")
    out.append(row)
    print(json.dumps(row))
if mode == "record":
    write_json(DIG, digs)
write_json(f"{RESULTS}/read-check-{label}.json", {"time": now(), "label": label, "mode": mode, "reads": out, "failures": failures})
if failures:
    die("; ".join(failures))
