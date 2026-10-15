"""Fast commission checks.

fc_check.py config <label> <expect:disabled|enabled> <cluster.json>
fc_check.py evidence <label> <new-instance> <du.json> <expect-advantage:yes|no> [timings.json]

Advantage: a replica on the new broker whose local start offset is higher than that of existing
replicas of the same partition, i.e. it started from an uploaded offset instead of copying the full
local window. `vs_lowest` follows the spec literally (higher than the lowest existing start);
`significant` requires it to be above the highest existing start by at least one segment's worth
of records, which filters trimming noise between existing replicas.
"""
import json
import os
import statistics
import sys
from collections import defaultdict

from rplib import (RESULTS, inst_of_node, list_topics, metrics, node_ids, now, partitions, running,
                   topic_configs, write_json)

CONTROL = os.environ.get("CONTROL_TOPIC", "control.no-ts")
SEGMENT = int(os.environ.get("SEGMENT_BYTES", "1048576"))
mode, label = sys.argv[1], sys.argv[2]


def tval(cfg, k):
    v = cfg.get(k)
    return (v[0], v[1]) if v else (None, None)


def is_override(src):
    return src is not None and "DYNAMIC_TOPIC" in src


if mode == "config":
    expect, cl_file = sys.argv[3], sys.argv[4]
    cl = json.load(open(cl_file))
    nul = lambda v: v in (None, "null", "", "-1")
    cl_bytes, cl_ms = cl["initial_retention_local_target_bytes_default"], cl["initial_retention_local_target_ms_default"]
    rows, override_any = {}, False
    for t in sorted(list_topics()):
        cfg = topic_configs(t)
        rw, rr = tval(cfg, "redpanda.remote.write")[0], tval(cfg, "redpanda.remote.read")[0]
        ib, ib_src = tval(cfg, "initial.retention.local.target.bytes")
        im, im_src = tval(cfg, "initial.retention.local.target.ms")
        ts = rw == "true" and rr == "true"
        ov_b = ib if is_override(ib_src) and not nul(ib) else None
        ov_m = im if is_override(im_src) and not nul(im) else None
        override_any |= bool(ov_b or ov_m)
        eff_b = ov_b or (None if nul(cl_bytes) else cl_bytes)
        eff_m = ov_m or (None if nul(cl_ms) else cl_ms)
        covered = ts and (eff_b is not None or eff_m is not None) and cl["cloud_storage_enabled"] == "true"
        if t == CONTROL:
            note = "not covered (no Tiered Storage)"
        elif not ts:
            note = "not covered (no Tiered Storage)"
        else:
            note = "covered" if covered else "not covered"
        rows[t] = {"remote_write": rw, "remote_read": rr, "topic_override_bytes": ov_b, "topic_override_ms": ov_m,
                   "effective_bytes": eff_b, "effective_ms": eff_m, "covered": covered, "note": note,
                   "describe_bytes": [ib, ib_src], "describe_ms": [im, im_src]}
    if nul(cl_bytes) and nul(cl_ms) and not override_any:
        state = "DISABLED"
    elif cl["cloud_storage_enabled"] == "true" and any(r["covered"] for r in rows.values()):
        state = "ENABLED"
    else:
        state = "INCONSISTENT"
    failures = []
    if state != expect.upper():
        failures.append(f"fast commission state is {state}, expected {expect.upper()}")
    if expect == "enabled":
        unc = [t for t, r in rows.items() if t != CONTROL and r["remote_write"] == "true" and not r["covered"]]
        if unc:
            failures.append(f"Tiered Storage topics not covered: {unc}")
    out = {"time": now(), "label": label, "state": state, "expect": expect.upper(), "cluster": cl,
           "topics": rows, "failures": failures, "passed": not failures}
    write_json(f"{RESULTS}/fast-commission-config-{label}.json", out)
    cov = sum(r["covered"] for r in rows.values())
    print(f"fast commission config '{label}': state={state} (expected {expect.upper()}), "
          f"cluster bytes={cl_bytes} ms={cl_ms}, covered topics {cov}/{len(rows)}")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        sys.exit(1)
    sys.exit(0)

# ---- evidence ----
new_inst, du_file, expect_adv = sys.argv[3], sys.argv[4], sys.argv[5]
timings = json.load(open(sys.argv[6])) if len(sys.argv) > 6 and os.path.exists(sys.argv[6]) else {}
du = json.load(open(du_file))
ids = node_ids()
new_id = ids[new_inst]
ion = inst_of_node()
starts = defaultdict(dict)   # (t,p) -> {node: start}
hwm = {}
for inst in running():
    if inst not in ids:
        continue
    nid = ids[inst]
    for name, lb, v in metrics(inst, {"vectorized_cluster_partition_start_offset", "vectorized_cluster_partition_high_watermark"}):
        if lb.get("namespace") != "kafka" or lb["topic"].startswith("_"):
            continue
        key = (lb["topic"], int(lb["partition"]))
        if name.endswith("start_offset"):
            starts[key][nid] = int(v)
        else:
            hwm[key] = max(hwm.get(key, 0), int(v))
parts = partitions()
per_topic = defaultdict(lambda: {"replicas_on_new": 0, "adv_vs_lowest": 0, "adv_significant": 0,
                                 "start_gap_records": [], "local_bytes_on_new": 0})
for t, p, reps, leader in parts:
    if t.startswith("_") or new_id not in reps:
        continue
    s = starts.get((t, p), {})
    ns = s.get(new_id)
    existing = [s[n] for n in reps if n != new_id and n in s]
    if ns is None or not existing:
        continue
    # records in one segment for preloaded ~1 KiB records (conservative for smaller live records)
    seg_records = max(1, SEGMENT // 1100)
    d = per_topic[t]
    d["replicas_on_new"] += 1
    if ns > min(existing):
        d["adv_vs_lowest"] += 1
    if ns >= max(existing) + seg_records:
        d["adv_significant"] += 1
    d["start_gap_records"].append(ns - min(existing))
for t, d in per_topic.items():
    d["local_bytes_on_new"] = du.get(new_inst, {}).get("topics", {}).get(t, 0)
    d["local_bytes_per_partition"] = int(d["local_bytes_on_new"] / d["replicas_on_new"]) if d["replicas_on_new"] else 0
    g = d.pop("start_gap_records")
    d["median_start_gap_records"] = int(statistics.median(g)) if g else 0
    d["tiered"] = t != CONTROL
others = [v["total"] for k, v in du.items() if k != new_inst and v.get("total")]
med = statistics.median(others) if others else 0
ts_rows = {t: d for t, d in per_topic.items() if d["tiered"]}
ts_bytes = sum(d["local_bytes_on_new"] for d in ts_rows.values())
ts_replicas = sum(d["replicas_on_new"] for d in ts_rows.values())
ts_adv = sum(d["adv_significant"] for d in ts_rows.values())
topics_with_adv = sorted(t for t, d in ts_rows.items() if d["adv_significant"] > 0)
ctl = per_topic.get(CONTROL, {})
failures = []
if expect_adv == "yes":
    if not topics_with_adv:
        failures.append("no Tiered Storage topic shows a start-offset advantage on the new broker")
    if ctl.get("adv_significant"):
        failures.append(f"control topic shows an advantage ({ctl['adv_significant']} replicas) but has no Tiered Storage")
elif expect_adv == "no":
    if ts_replicas and ts_adv > 0.1 * ts_replicas:
        failures.append(f"{ts_adv}/{ts_replicas} TS replicas show an advantage although fast commission is disabled")
out = {"time": now(), "label": label, "new_instance": new_inst, "new_node_id": new_id,
       "expect_advantage": expect_adv, "per_topic": dict(per_topic),
       "summary": {"ts_replicas_on_new": ts_replicas, "ts_replicas_advantaged": ts_adv,
                   "ts_replicas_adv_vs_lowest": sum(d["adv_vs_lowest"] for d in ts_rows.values()),
                   "ts_topics_with_advantage": topics_with_adv, "ts_local_bytes_on_new": ts_bytes,
                   "ts_local_bytes_per_replica": int(ts_bytes / ts_replicas) if ts_replicas else 0,
                   "control_replicas_on_new": ctl.get("replicas_on_new", 0),
                   "control_advantaged": ctl.get("adv_significant", 0),
                   "control_local_bytes_on_new": ctl.get("local_bytes_on_new", 0),
                   "new_disk_bytes": du.get(new_inst, {}).get("total"), "existing_median_disk_bytes": med,
                   "new_disk_pct_of_median": round(100 * du.get(new_inst, {}).get("total", 0) / med, 1) if med else None},
       "timings": timings, "disk": du, "failures": failures, "passed": not failures}
write_json(f"{RESULTS}/fast-commission-{label}.json", out)
lines = [f"# Fast commission evidence: {label} (new broker {new_inst}, node {new_id})", "",
         "| topic | TS | replicas on new | advantaged (significant) | advantaged (vs lowest) | median start gap (records) | local bytes on new | bytes/replica |",
         "|---|---|---|---|---|---|---|---|"]
for t in sorted(per_topic):
    d = per_topic[t]
    lines.append(f"| {t} | {'yes' if d['tiered'] else 'no'} | {d['replicas_on_new']} | {d['adv_significant']} | {d['adv_vs_lowest']} | "
                 f"{d['median_start_gap_records']} | {d['local_bytes_on_new']} | {d['local_bytes_per_partition']} |")
s = out["summary"]
lines += ["", f"TS replicas advantaged: {s['ts_replicas_advantaged']}/{s['ts_replicas_on_new']}; "
          f"TS local bytes on new: {s['ts_local_bytes_on_new']} ({s['ts_local_bytes_per_replica']} per replica); "
          f"disk {s['new_disk_pct_of_median']}% of existing median"]
open(f"{RESULTS}/fast-commission-{label}.md", "w").write("\n".join(lines) + "\n")
print(lines[-1])
if failures:
    print("\n".join(failures), file=sys.stderr)
    sys.exit(1)
