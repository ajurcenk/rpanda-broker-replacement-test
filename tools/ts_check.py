"""ts_check.py <label> <mc-ls.json> : Tiered Storage data check (see check-tiered-storage.sh).

1. uploads exist per partition (segments, manifests)          from the mc listing
2. local start offset > 0 on every replica                      vectorized_cluster_partition_start_offset
3. earliest Kafka offset is 0 and the first 1,000 records of 3 partitions/topic are readable
   (served from MinIO), timed against a read of 1,000 local records
4. upload backlog drained (cloud_log_last_offset close to local last offset)  /v1/cloud_storage/status
5. control topic: no objects, local start offset 0
"""
import json
import os
import re
import sys
import time
from collections import defaultdict

from confluent_kafka import Consumer, TopicPartition
from rplib import BOOTSTRAP, RESULTS, admin, inst_of_node, metrics, now, partitions, running, write_json

label, mc_file = sys.argv[1], sys.argv[2]
CONTROL = os.environ.get("CONTROL_TOPIC", "control.no-ts")
SEGMENT = int(os.environ["SEGMENT_BYTES"])
INTERVAL = int(os.environ.get("UPLOAD_INTERVAL_SEC", "30"))
READS = int(os.environ.get("TS_READ_PARTITIONS", "3"))
SKIP_READS = os.environ.get("TS_SKIP_READS") == "1"
failures = []

# ---- 1. objects in MinIO ----
objs = defaultdict(lambda: {"segments": 0, "manifests": 0, "objects": 0, "bytes": 0})
pat = re.compile(r"kafka/([^/]+)/(\d+)_(\d+)/(.+)$")
with open(mc_file) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        o = json.loads(line)
        if o.get("type") != "file":
            continue
        m = pat.search(o["key"])
        if not m:
            continue
        t, p, name = m.group(1), int(m.group(2)), m.group(4)
        d = objs[(t, p)]
        d["objects"] += 1
        d["bytes"] += o.get("size", 0)
        if ".log" in name:
            d["segments"] += 1
        elif "manifest" in name:
            d["manifests"] += 1

# ---- placement and per-replica local start offsets ----
parts = partitions()
ion = inst_of_node()
local_start = defaultdict(dict)   # (t,p) -> {node: start_offset}
hwm = {}
for inst in running():
    try:
        nid = {v: k for k, v in ion.items()}[inst]
    except KeyError:
        continue
    for name, lb, v in metrics(inst, {"vectorized_cluster_partition_start_offset", "vectorized_cluster_partition_high_watermark"}):
        if lb.get("namespace") != "kafka":
            continue
        key = (lb["topic"], int(lb["partition"]))
        if name.endswith("start_offset"):
            local_start[key][nid] = int(v)
        else:
            hwm[key] = max(hwm.get(key, 0), int(v))

topics = sorted({t for t, *_ in parts if not t.startswith("_")})
ts_topics = [t for t in topics if t != CONTROL]


def consume_n(t, p, off, n=1000, timeout=120):
    t0 = time.time()   # includes connect + metadata, identical for both reads
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"rpbr-tscheck", "enable.auto.commit": False,
                  "fetch.max.bytes": 8 << 20})
    c.assign([TopicPartition(t, p, off)])
    got, first = 0, None
    while got < n and time.time() - t0 < timeout:
        msgs = c.consume(min(500, n - got), 1.0)
        for m in msgs:
            if m.error():
                continue
            if first is None:
                first = m.offset()
            got += 1
    dt = time.time() - t0
    c.close()
    return got, first, dt


# ---- 4. upload backlog (polled, up to 3 upload intervals) ----
def backlog():
    out = {}
    for t, p, reps, leader in parts:
        if t not in ts_topics or leader is None or leader not in ion:
            continue
        try:
            s = admin(ion[leader], f"/v1/cloud_storage/status/{t}/{p}")
        except Exception as e:
            out[(t, p)] = {"error": str(e)}
            continue
        recs = max(1, s["local_log_last_offset"] - s["local_log_start_offset"])
        avg = s["local_log_size_bytes"] / recs
        lag = max(0, s["local_log_last_offset"] - s["cloud_log_last_offset"])
        out[(t, p)] = {"lag_offsets": lag, "lag_bytes_est": int(lag * avg),
                       "ms_since_last_segment_upload": s.get("ms_since_last_segment_upload"),
                       "cloud_log_last_offset": s["cloud_log_last_offset"],
                       "local_log_last_offset": s["local_log_last_offset"]}
    return out


deadline = time.time() + 3 * INTERVAL
while True:
    bl = backlog()
    undrained = {k: v for k, v in bl.items() if "error" in v or v["lag_bytes_est"] > 1.5 * SEGMENT}
    if not undrained or time.time() > deadline:
        break
    time.sleep(5)
for (t, p), v in undrained.items():
    failures.append(f"{t}/{p}: upload backlog not drained: {v}")

# ---- per topic evaluation ----
report = {"label": label, "time": now(), "topics": {}, "failures": failures}
for t in topics:
    tp = sorted(p for tt, p, *_ in parts if tt == t)
    reps = {p: r for tt, p, r, _ in parts if tt == t}
    tr = {"partitions": {}, "objects": 0, "bytes": 0, "tiered": t != CONTROL}
    only_cloud_shares = []
    for p in tp:
        o = objs.get((t, p), {"segments": 0, "manifests": 0, "objects": 0, "bytes": 0})
        tr["objects"] += o["objects"]
        tr["bytes"] += o["bytes"]
        starts = {n: local_start[(t, p)].get(n) for n in reps[p]}
        lo, hi = None, hwm.get((t, p), 0)
        pr = {"replicas": reps[p], "local_start_by_node": starts, "high_watermark": hi, **o}
        if t == CONTROL:
            if o["objects"]:
                failures.append(f"{t}/{p}: control topic has {o['objects']} objects in MinIO")
            if any(v not in (0, None) for v in starts.values()):
                failures.append(f"{t}/{p}: control topic local start offset not 0: {starts}")
        else:
            if o["segments"] == 0:
                failures.append(f"{t}/{p}: no segments in MinIO")
            missing = [n for n, v in starts.items() if v is None]
            if missing:
                failures.append(f"{t}/{p}: no start-offset metric for replicas {missing}")
            zero = [n for n, v in starts.items() if v == 0]
            if zero:
                failures.append(f"{t}/{p}: local start offset is 0 on replicas {zero} (oldest data still local)")
            known = [v for v in starts.values() if v is not None]
            if known and hi:
                only_cloud_shares.append(min(known) / hi)
                pr["share_only_in_minio"] = round(min(known) / hi, 4)
        tr["partitions"][p] = pr
    if t != CONTROL:
        tr["share_only_in_minio_avg"] = round(sum(only_cloud_shares) / len(only_cloud_shares), 4) if only_cloud_shares else 0
        tr["backlog"] = {p: bl.get((t, p)) for p in tp}
    # ---- 3. earliest offset and reads ----
    reads = []
    sample = tp[:READS] if t != CONTROL else tp[:1]
    for p in sample:
        from rplib import watermarks
        lo, hi = watermarks(t, p)
        if t != CONTROL and lo != 0:
            failures.append(f"{t}/{p}: Kafka earliest offset is {lo}, expected 0")
        if SKIP_READS:
            continue
        got, first, dt = consume_n(t, p, lo)
        if got < min(1000, hi - lo):
            failures.append(f"{t}/{p}: read from earliest returned {got} records")
        local_from = max(lo, hi - 1000)
        got2, _, dt2 = consume_n(t, p, local_from)
        reads.append({"partition": p, "earliest": lo, "first_offset_read": first, "records": got,
                      "earliest_read_sec": round(dt, 3), "local_read_sec": round(dt2, 3), "local_read_from": local_from,
                      "local_records": got2})
        if got2 < min(1000, hi - local_from):
            failures.append(f"{t}/{p}: read of local tail returned {got2} records")
    tr["reads"] = reads
    report["topics"][t] = tr

report["failures"] = failures
report["passed"] = not failures
os.makedirs(RESULTS, exist_ok=True)
write_json(f"{RESULTS}/tiered-storage-check-{label}.json", report)
for t, tr in report["topics"].items():
    r = tr["reads"]
    rl = f" read earliest {sum(x['earliest_read_sec'] for x in r)/len(r):.2f}s vs local {sum(x['local_read_sec'] for x in r)/len(r):.2f}s" if r else ""
    print(f"{t:22s} objects={tr['objects']:6d} bytes={tr['bytes']/1048576:8.1f}MiB "
          f"only-in-MinIO={tr.get('share_only_in_minio_avg', '-')}{rl}")
if failures:
    print("\n".join(failures[:40]), file=sys.stderr)
    sys.exit(1)
print(f"tiered storage check '{label}': PASS")
