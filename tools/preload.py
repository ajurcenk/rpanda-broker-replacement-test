"""preload.py <bytes_per_partition> <topic>... : fill every partition of each topic explicitly
(not via the sticky partitioner) so each partition gets the same volume. acks=all."""
import os
import sys
import time
from confluent_kafka import Producer
from rplib import BOOTSTRAP, list_topics, die

per_part = int(sys.argv[1])
topics = sys.argv[2:]
REC = 1024
block = os.urandom(1 << 20)
errors = []
p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True,
              "linger.ms": 20, "batch.size": 1 << 20, "queue.buffering.max.kbytes": 262144})


def cb(err, msg):
    if err:
        errors.append(str(err))


parts = list_topics()
for t in topics:
    n = parts.get(t) or die(f"topic {t} not found")
    count = per_part // REC
    t0 = time.time()
    for i in range(count):
        for part in range(n):
            off = ((i * n + part) * 4099) % ((1 << 20) - REC)
            while True:
                try:
                    p.produce(t, block[off:off + REC], partition=part, on_delivery=cb)
                    break
                except BufferError:
                    p.poll(0.05)
        if i % 256 == 0:
            p.poll(0)
    left = p.flush(300)
    if left or errors:
        die(f"{t}: {left} undelivered, errors: {errors[:3]}")
    print(f"{t}: {n} partitions x {count * REC} bytes in {time.time() - t0:.1f}s", flush=True)
