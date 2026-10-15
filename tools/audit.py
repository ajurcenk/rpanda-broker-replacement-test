"""audit.py <label> [--informational]: every partition of every topic (internal included) must have
exactly one replica per rack. Racks per node come from /v1/brokers."""
import json
import sys
from collections import Counter

from rplib import RESULTS, any_admin, now

label = sys.argv[1]
info = "--informational" in sys.argv
racks = {b["node_id"]: b.get("rack") for b in any_admin("/v1/brokers")}
all_racks = sorted({r for r in racks.values() if r})
parts = any_admin("/v1/cluster/partitions?with_internal=true")
moving = {(m["ns"], m["topic"], m["partition"]) for m in (any_admin("/v1/partitions/reconfigurations") or [])}
viol = []
for p in parts:
    nodes = [r["node_id"] for r in p["replicas"]]
    rs = [racks.get(n) for n in nodes]
    c = Counter(rs)
    ok = len(nodes) == len(all_racks) and all(c[r] == 1 for r in all_racks)
    if not ok:
        key = (p["ns"], p["topic"], p["partition_id"])
        viol.append({"ntp": f"{p['ns']}/{p['topic']}/{p['partition_id']}", "replicas": nodes, "racks": rs,
                     "moving": key in moving})
res = {"time": now(), "label": label, "partitions": len(parts), "racks": all_racks,
       "violations": viol, "moving": len(moving)}
with open(f"{RESULTS}/audit-placement.jsonl", "a") as f:
    f.write(json.dumps(res) + "\n")
for v in viol[:30]:
    print(f"VIOLATION {v['ntp']} replicas={v['replicas']} racks={v['racks']}{' (moving)' if v['moving'] else ''}")
hard = [v for v in viol if not v["moving"]]
print(f"placement audit '{label}': {len(parts)} partitions, {len(viol)} violations ({len(viol) - len(hard)} in active moves), racks={all_racks}")
if viol and not info:
    sys.exit(1)
