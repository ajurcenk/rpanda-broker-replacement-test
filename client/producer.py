"""Sequencer producer.

Sends strictly increasing sequence numbers to order.wal.btc-try (acks=all, idempotent) at a fixed
rate and appends every *acknowledged* sequence number to results/acked.log. Also produces a
low-rate stream to background topics. Bootstraps through the DNS aliases only.

--preload BYTES: write ~BYTES of 1 KiB sequenced records as fast as possible, then exit.
On start it continues from the last sequence number present in the topic, so restarts never
reuse a number.
"""
import argparse
import json
import os
import random
import signal
import sys
import time

from confluent_kafka import Consumer, Producer, TopicPartition

R = "/work/results"
ap = argparse.ArgumentParser()
ap.add_argument("--topic", default="order.wal.btc-try")
ap.add_argument("--rate", type=float, default=500)
ap.add_argument("--bg-topics", default="")
ap.add_argument("--bg-rate", type=float, default=20)
ap.add_argument("--preload", type=int, default=0)
a = ap.parse_args()
BOOT = os.environ["BOOTSTRAP"]
for h in BOOT.split(","):
    if not h.split(".")[0] in {"b1", "b2", "b3", "b4", "b5"}:
        sys.exit(f"bootstrap must use aliases only, got {h}")


def last_seq_in_topic():
    c = Consumer({"bootstrap.servers": BOOT, "group.id": "rpbr-producer-probe", "enable.auto.commit": False})
    tp = TopicPartition(a.topic, 0)
    lo, hi = c.get_watermark_offsets(tp, timeout=30)
    if hi <= lo:
        c.close()
        return -1
    tp.offset = hi - 1
    c.assign([tp])
    seq = -1
    deadline = time.time() + 60
    while time.time() < deadline:
        m = c.poll(1)
        if m is None or m.error():
            continue
        seq = json.loads(m.value())["seq"]
        if m.offset() >= hi - 1:
            break
    c.close()
    return seq


p = Producer({"bootstrap.servers": BOOT, "acks": "all", "enable.idempotence": True,
              "linger.ms": 3, "batch.size": 4096, "client.id": "rpbr-sequencer"})
start = last_seq_in_topic() + 1
print(f"producer starting at seq {start}", flush=True)

acked = open(f"{R}/acked.log", "a", buffering=1 << 16)
errlog = open(f"{R}/producer-errors.log", "a", buffering=1)
stats = open(f"{R}/producer-stats.csv", "a", buffering=1)
lat = []
n_acked = n_err = 0
stop = False


def on_term(*_):
    global stop
    stop = True


signal.signal(signal.SIGTERM, on_term)
signal.signal(signal.SIGINT, on_term)


def cb(err, msg):
    global n_acked, n_err
    if err:
        n_err += 1
        errlog.write(f"{time.time():.3f} {err}\n")
        return
    d = json.loads(msg.value())
    acked.write(f"{d['seq']}\n")
    lat.append((time.time() - d["ts"]) * 1000)
    n_acked += 1


bg = [t for t in a.bg_topics.split(",") if t]
seq = start
pad = "x" * 960 if a.preload else "x" * 200
interval = 0 if a.preload else 1.0 / a.rate
bg_every = max(1, int(a.rate / a.bg_rate)) if bg else 0
next_t = time.time()
last_flush = last_stat = time.time()
target = start + a.preload // 1024 if a.preload else None
while not stop and (target is None or seq < target):
    if interval:
        now = time.time()
        if now < next_t:
            p.poll(min(next_t - now, 0.01))
            continue
        next_t += interval
    v = json.dumps({"seq": seq, "ts": time.time(), "pad": pad})
    while True:
        try:
            p.produce(a.topic, v.encode(), partition=0, on_delivery=cb)
            break
        except BufferError:
            p.poll(0.05)
    if bg_every and seq % bg_every == 0:
        try:
            p.produce(random.choice(bg), os.urandom(512))
        except BufferError:
            pass
    seq += 1
    p.poll(0)
    now = time.time()
    if now - last_flush > 0.1:
        acked.flush()
        last_flush = now
    if now - last_stat >= 10:
        s = sorted(lat)
        p50 = s[len(s) // 2] if s else ""
        p99 = s[int(len(s) * 0.99)] if s else ""
        stats.write(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())},{seq},{n_acked},{n_err},{p50},{p99}\n")
        lat.clear()
        last_stat = now
left = p.flush(120)
while a.preload and left:          # preload: wait for every queued record to be acknowledged
    left = p.flush(60)
    print(f"preload: {left} records still in flight", flush=True)
acked.flush()
os.fsync(acked.fileno())
print(f"producer stopped at seq {seq}, acked {n_acked}, errors {n_err}, unflushed {left}", flush=True)
