"""verify.py [pre_swaps_offset_file]: read the whole sequencer topic from the beginning and check
every acknowledged sequence number is present, with first occurrences in sequence order.
Duplicates (unacked retries) are allowed and counted. Also checks the `verifier` group offset."""
import json
import os
import sys
import time

from confluent_kafka import Consumer, TopicPartition
from rplib import BOOTSTRAP, RESULTS, now, write_json

TOPIC = os.environ.get("SEQ_TOPIC", "order.wal.btc-try")
pre_file = sys.argv[1] if len(sys.argv) > 1 else None
acked = set()
with open(f"{RESULTS}/acked.log") as f:
    for line in f:
        line = line.strip()
        if line:
            acked.add(int(line))
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "rpbr-verify", "enable.auto.commit": False,
              "fetch.max.bytes": 16 << 20})
tp = TopicPartition(TOPIC, 0)
lo, hi = c.get_watermark_offsets(tp, timeout=30)
c.assign([TopicPartition(TOPIC, 0, lo)])
first_off = {}
dups = 0
total = 0
nxt = lo
t0 = time.time()
while nxt < hi and time.time() - t0 < 1800:
    for m in c.consume(2000, 2.0):
        if m.error():
            continue
        s = json.loads(m.value())["seq"]
        total += 1
        if s in first_off:
            dups += 1
        else:
            first_off[s] = m.offset()
        nxt = m.offset() + 1
committed = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "verifier", "enable.auto.commit": False}) \
    .committed([TopicPartition(TOPIC, 0)], timeout=30)[0].offset
c.close()
missing = sorted(acked - first_off.keys())
reorders = 0
prev = -1
for s in sorted(acked):
    o = first_off.get(s)
    if o is None:
        continue
    if o < prev:
        reorders += 1
    prev = o
pre = int(open(pre_file).read().strip()) if pre_file and os.path.exists(pre_file) else None
res = {"time": now(), "topic": TOPIC, "log_start": lo, "high_watermark": hi, "records_read": total,
       "acked": len(acked), "acked_missing": len(missing), "missing_sample": missing[:20],
       "reorders": reorders, "duplicates": dups, "unacked_present": len(first_off.keys() - acked),
       "group_committed_offset": committed, "group_offset_before_swaps": pre,
       "group_offset_preserved": (pre is None) or (committed >= pre)}
res["passed"] = res["acked_missing"] == 0 and reorders == 0 and res["group_offset_preserved"] and nxt >= hi
write_json(f"{RESULTS}/verify-data.json", res)
print(json.dumps(res, indent=1))
sys.exit(0 if res["passed"] else 1)
