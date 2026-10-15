# Central settings for the broker replacement harness. Sourced by every script.
# Override any value from the environment, e.g. SOAK_SEC=60 scripts/run-migration.sh

# ---- Images (exact pins, never "latest") ----
RP_VERSION="${RP_VERSION:-25.3.17}"
RP_IMAGE="docker.redpanda.com/redpandadata/redpanda:v${RP_VERSION}"
MINIO_TAG="RELEASE.2025-10-15T17-29-55Z"
MC_TAG="RELEASE.2025-08-13T08-35-41Z"
MINIO_IMAGE="rpbr/minio:${MINIO_TAG}"      # built from source, see images/minio/Dockerfile
MC_IMAGE="rpbr/mc:${MC_TAG}"
COREDNS_IMAGE="coredns/coredns:1.12.4"
KAFKA_IMAGE="apache/kafka:3.9.1"
TOOLS_IMAGE="rpbr/tools:1"                 # python:3.12.11-slim + pinned confluent-kafka, see client/

# ---- Network / DNS ----
NET="rpnet"
SUBNET="172.30.0.0/24"
DNS_IP="172.30.0.53"
MINIO_IP="172.30.0.10"
UNUSED_IP="172.30.0.99"
ZONE="rp.test"
DNS_TTL=5

# ---- Instances: name -> "ip rack" ----
declare -A INST_IP=(
  [io-b1]=172.30.0.11 [io-b2]=172.30.0.12 [io-b3]=172.30.0.13 [io-b4]=172.30.0.14 [io-b5]=172.30.0.15
  [nv-n1]=172.30.0.21 [nv-n2]=172.30.0.22 [nv-n3]=172.30.0.23 [nv-n4]=172.30.0.24 [nv-n5]=172.30.0.25
  [tmp-1]=172.30.0.31
  [sp-1]=172.30.0.61 [sp-2]=172.30.0.62 [sp-3]=172.30.0.63 [sp-4]=172.30.0.64
)
declare -A INST_RACK=(
  [io-b1]=eu-central-1a [io-b2]=eu-central-1a [io-b3]=eu-central-1b [io-b4]=eu-central-1b [io-b5]=eu-central-1c
  [nv-n1]=eu-central-1a [nv-n2]=eu-central-1a [nv-n3]=eu-central-1b [nv-n4]=eu-central-1b [nv-n5]=eu-central-1c
  [tmp-1]=eu-central-1a
  [sp-1]=eu-central-1a [sp-2]=eu-central-1b [sp-3]=eu-central-1c [sp-4]=eu-central-1a
)
ALIASES=(b1 b2 b3 b4 b5)
SEED_ALIASES=(b1 b3 b5)
declare -A ALIAS_INITIAL=([b1]=io-b1 [b2]=io-b2 [b3]=io-b3 [b4]=io-b4 [b5]=io-b5)
BOOTSTRAP="b1.${ZONE}:9092,b2.${ZONE}:9092,b3.${ZONE}:9092,b4.${ZONE}:9092,b5.${ZONE}:9092"

# ---- Broker resources (D3) ----
RP_SMP=1
RP_MEMORY=1G
BROKER_MEM_LIMIT=1400m

# ---- Object storage ----
S3_ACCESS_KEY="rpminio"
S3_SECRET_KEY="rpminio-secret-key"
S3_BUCKET="redpanda-ts"
S3_REGION="us-east-1"

# ---- Data sizing (user decision: ~50 MB per partition; spec ratios kept) ----
MiB=1048576
SEGMENT_BYTES=$((1 * MiB))                 # spec 16 MiB
LOCAL_TARGET_BYTES=$((16 * MiB))           # spec 256 MiB
PRELOAD_BYTES_PER_PARTITION=$((50 * MiB))  # >= 3x local target
FC_TARGET_BYTES=$((1 * MiB))               # spec 16 MiB; = 1 segment
BG_TOPICS=20
BG_PARTITIONS=6
SEQ_TOPIC="order.wal.btc-try"
CONTROL_TOPIC="control.no-ts"
PERF_TOPIC="perf.latency"
E2E_TOPIC="perf.e2e"
UPLOAD_INTERVAL_SEC=30
CONCURRENT_MOVES=10
FC_MAX_BYTES_RATIO="0.25"                  # Phase B local bytes must be below this share of Phase A

# ---- Clients ----
PRODUCER_RATE=500
BG_PRODUCER_RATE=20

# ---- Perf windows ----
PERF_RATE="${PERF_RATE:-2000}"
PERF_WINDOW_SEC="${PERF_WINDOW_SEC:-60}"
PERF_E2E_COUNT="${PERF_E2E_COUNT:-2000}"
WARN_RATIO=2
ALERT_RATIO=5
SOAK_RECOVERY_RATIO=1.2

# ---- Recovery rate ----
RAISED_RECOVERY_RATE="${RAISED_RECOVERY_RATE:-209715200}"
ROLLBACK_RATIO=2
ROLLBACK_SAMPLES=3

# ---- Swap timing ----
SOAK_SEC="${SOAK_SEC:-180}"
SETTLE_TIMEOUT_SEC="${SETTLE_TIMEOUT_SEC:-3600}"
DECOM_TIMEOUT_SEC="${DECOM_TIMEOUT_SEC:-3600}"
