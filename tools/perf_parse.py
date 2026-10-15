"""perf_parse.py <window_id> <label> <phase> <phase_end> <start_ts> <prod.log> <cons.log> <e2e.log> <target_rate>
Parse the three Kafka perf tool outputs of one window and append one row per tool to results/latency.csv."""
import csv
import os
import re
import sys

wid, label, phase, phase_end, start_ts, plog, clog, elog, target = sys.argv[1:10]
CSV = "/work/results/latency.csv"
COLS = ["timestamp", "window_id", "label", "phase", "phase_end", "tool", "target_rate",
        "rate", "mb_s", "avg_ms", "p50_ms", "p95_ms", "p99_ms", "p999_ms", "max_ms", "errors", "retries",
        "cons_mb_s", "cons_msg_s", "cons_fetch_ms", "cons_rebalance_ms", "cons_msgs", "status"]


def read(p):
    try:
        return open(p, errors="replace").read()
    except FileNotFoundError:
        return ""


rows = []
base = {"timestamp": start_ts, "window_id": wid, "label": label, "phase": phase, "phase_end": phase_end,
        "target_rate": target}

# producer: "N records sent, R records/sec (M MB/sec), A ms avg latency, X ms max latency, a ms 50th, b ms 95th, c ms 99th, d ms 99.9th."
t = read(plog)
m = None
for m in re.finditer(r"(\d+) records sent, ([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency, ([\d.]+) ms max latency, (\d+) ms 50th, (\d+) ms 95th, (\d+) ms 99th, (\d+) ms 99.9th", t):
    pass
err = re.search(r"producer-metrics:record-error-total:\{[^}]*\}\s*:\s*([\d.]+)", t)
ret = re.search(r"producer-metrics:record-retry-total:\{[^}]*\}\s*:\s*([\d.]+)", t)
r = dict(base, tool="produce")
if m:
    r.update(rate=m.group(2), mb_s=m.group(3), avg_ms=m.group(4), max_ms=m.group(5), p50_ms=m.group(6),
             p95_ms=m.group(7), p99_ms=m.group(8), p999_ms=m.group(9),
             errors=int(float(err.group(1))) if err else "", retries=int(float(ret.group(1))) if ret else "",
             status="ok")
else:
    r.update(status="no-summary")
rows.append(r)

# consumer (--show-detailed-stats): time, threadId, MB, MB.sec, nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
t = read(clog)
mb = msgs = fetch = reb = 0.0
n = 0
for line in t.splitlines():
    p = [x.strip() for x in line.split(",")]
    if len(p) >= 10 and re.match(r"\d{4}-\d\d-\d\d", p[0]):
        try:
            n += 1
            mb_s, msg_s = float(p[3]), float(p[5])
            reb = max(reb, float(p[6]))
            fetch += float(p[7])
            mb += mb_s
            msgs += msg_s
            last_msgs = float(p[4])
        except ValueError:
            pass
r = dict(base, tool="consume")
if n:
    r.update(cons_mb_s=round(mb / n, 3), cons_msg_s=round(msgs / n, 1), cons_fetch_ms=round(fetch, 1),
             cons_rebalance_ms=reb, cons_msgs=int(last_msgs), status="ok")
else:
    r.update(status="no-stats")
rows.append(r)

# end-to-end: "Avg latency: X ms" and "Percentiles: 50th = a, 99th = b, 99.9th = c"
t = read(elog)
a = re.search(r"Avg latency: ([\d.]+) ms", t)
pc = re.search(r"Percentiles: 50th = ([\d.]+), 99th = ([\d.]+), 99\.9th = ([\d.]+)", t)
r = dict(base, tool="e2e")
if a and pc:
    r.update(avg_ms=a.group(1), p50_ms=pc.group(1), p99_ms=pc.group(2), p999_ms=pc.group(3), status="ok")
else:
    r.update(status="no-summary")
rows.append(r)

new = not os.path.exists(CSV)
with open(CSV, "a", newline="") as f:
    w = csv.DictWriter(f, fieldnames=COLS, lineterminator="\n")
    if new:
        w.writeheader()
    for r in rows:
        w.writerow({k: r.get(k, "") for k in COLS})
pr = rows[0]
print(f"window {wid} [{label}/{phase}] produce p99={pr.get('p99_ms')} rate={pr.get('rate')} err={pr.get('errors')} | "
      f"consume {rows[1].get('cons_mb_s')} MB/s | e2e p99={rows[2].get('p99_ms')}")
