"""Shared helpers for the harness's Python tools (run inside the tools container on rpnet)."""
import json
import os
import re
import sys
import time
import urllib.request

ZONE = "rp.test"
RESULTS = "/work/results"
BOOTSTRAP = os.environ.get("BOOTSTRAP", ",".join(f"b{i}.{ZONE}:9092" for i in range(1, 6)))
_METRIC = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}\s+(\S+)')


def die(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def running():
    """Instances passed from bash (RUNNING="io-b1 io-b2 ...")."""
    return os.environ.get("RUNNING", "").split()


def http(inst, path, timeout=30, method="GET", data=None):
    url = f"http://{inst}.{ZONE}:9644{path}"
    req = urllib.request.Request(url, method=method, data=data)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode()
    return body


def admin(inst, path, **kw):
    body = http(inst, path, **kw)
    return json.loads(body) if body.strip() else None


def any_admin(path):
    last = None
    for inst in running():
        try:
            return admin(inst, path)
        except Exception as e:  # try the next broker
            last = e
    raise RuntimeError(f"no broker answered {path}: {last}")


def metrics(inst, names=None, path="/metrics"):
    """[(name, labels_dict, value)] from a broker's metrics endpoint, optionally filtered by name."""
    out = []
    for line in http(inst, path, timeout=60).splitlines():
        m = _METRIC.match(line)
        if not m:
            continue
        name = m.group(1)
        if names and name not in names:
            continue
        labels = dict(re.findall(r'(\w+)="([^"]*)"', m.group(2)))
        out.append((name, labels, float(m.group(3))))
    return out


def node_ids():
    """{instance: node_id} from results/node_ids.txt (latest wins)."""
    m = {}
    with open(f"{RESULTS}/node_ids.txt") as f:
        for line in f:
            p = line.split()
            if len(p) >= 2:
                m[p[1]] = int(p[0])
    return m


def inst_of_node():
    return {v: k for k, v in node_ids().items()}


def brokers():
    return any_admin("/v1/brokers")


def partitions(ns="kafka"):
    """All partitions with replica node ids: [(topic, partition, [node_ids], leader)]."""
    out = []
    for p in any_admin("/v1/cluster/partitions"):
        if p.get("ns", "kafka") != ns:
            continue
        out.append((p["topic"], p["partition_id"], [r["node_id"] for r in p["replicas"]], p.get("leader_id")))
    return out


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def topic_configs(topic):
    """Topic configs via Kafka DescribeConfigs: {name: (value, source)}."""
    from confluent_kafka.admin import AdminClient, ConfigResource
    a = AdminClient({"bootstrap.servers": BOOTSTRAP})
    res = ConfigResource(ConfigResource.Type.TOPIC, topic)
    fut = a.describe_configs([res])[res]
    cfg = fut.result(timeout=30)
    return {k: (v.value, str(v.source)) for k, v in cfg.items()}


def list_topics():
    from confluent_kafka.admin import AdminClient
    a = AdminClient({"bootstrap.servers": BOOTSTRAP})
    md = a.list_topics(timeout=30)
    return {t: len(md.topics[t].partitions) for t in md.topics if not t.startswith("_")}


def watermarks(topic, partition):
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "rpbr-watermarks", "enable.auto.commit": False})
    try:
        return c.get_watermark_offsets(TopicPartition(topic, partition), timeout=30)
    finally:
        c.close()
