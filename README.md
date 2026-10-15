# Rolling broker replacement test harness

A local, reproducible test of the production plan to replace every broker of a 5-broker Redpanda
cluster one at a time (add new, then decommission old). It covers Redpanda v25.3.17, racks in a
2/2/1 layout, Tiered Storage on MinIO, fast commission, and DNS-alias seeds (Path A).
The test specification is kept outside this repository. `scripts/report.sh` writes the results to `REPORT.md`.

## Prerequisites

- Linux with Docker Engine and Docker Compose v2. Tested with Docker 29.8 and Compose 5.5.
- About 12 GiB of RAM available to Docker. Normally 6 brokers run at 1.4 GiB each, plus MinIO,
  CoreDNS, three Kafka perf JVMs and two Python clients. The peak is 7 brokers, during scenario 8.
- About 25 GiB of free disk for Docker volumes. That covers MinIO at about 7.5 GiB, plus broker data.
- Host tools: `bash`, `jq`, `dig`, `python3`. All other tooling runs in containers.
- Network access on the first run, to pull images and to clone MinIO and `mc` from GitHub. MinIO no longer publishes
  images, so `images/minio/Dockerfile` builds them from release tags.
- Licence: the cluster uses its built-in 30-day evaluation licence. `up.sh` stops if
  `rpk cluster license info` reports no valid licence.

## Layout

| Path | Purpose |
|---|---|
| `env.sh` | All settings: image pins, IP plan, data sizes, rates, soak time, thresholds |
| `docker-compose.yml` | Network `rpnet`, CoreDNS, MinIO, bucket init. Brokers run separately (see `scripts/broker.sh`). |
| `coredns/` | `Corefile`, plus the `rp.test.hosts` zone. Aliases are repointed by rewriting this file. |
| `config/redpanda.yaml.tmpl` | Broker config template (`scripts/render-config.sh` produces `config/rendered/<instance>.yaml`) |
| `client/` | Sequencer producer, verifier consumer, and the tools image (Python 3.12 + `confluent-kafka`) |
| `tools/` | Python checks: Tiered Storage, fast commission, placement, data verification, perf parsing, report |
| `scripts/` | Everything you run (see below) |
| `results/` | Logs, CSVs, JSON evidence, charts |

## Run the full migration

```bash
scripts/run-migration.sh      # about 2.5 h: setup, seed, fast commission flow, 5 swaps, verification
scripts/run-scenarios.sh      # about 1 h: scenarios 1-7 and 9 (scenario 8 runs inside swap 3)
scripts/report.sh             # writes REPORT.md and results/charts/*.png
```

`run-migration.sh` can be re-run. It skips stages that already completed: seeded data, a passed fast
commission flow, and swaps marked `done` in `results/swaps.csv`. It runs these stages:

1. `up.sh`: CoreDNS, MinIO and the bucket. The founders io-b1, io-b3 and io-b5 form the cluster, then switch to the alias seeds. io-b2 and io-b4 join.
2. `configure-cluster.sh`: Tiered Storage, rack awareness, continuous balancing, `write_caching_default=false` and `retention_local_strict=true`. Fast commission stays **disabled**.
3. `seed-data.sh`, then `check-tiered-storage.sh --label post-seed`.
4. `run-fast-commission-flow.sh`. Phase A is a `tmp-1` join with fast commission disabled. Phase B enables it, then runs a `tmp-1` join and the read-back checks.
5. The test clients start, then one `cluster-baseline` perf window runs.
6. Five calls to `swap.sh`: io-b2→nv-n2, io-b4→nv-n4 (raised recovery rate), io-b1→nv-n1 (plus scenario 8), io-b3→nv-n3, and io-b5→nv-n5 (raised).
7. Final checks, a restore of the temporary settings, and `verify-data.sh`.

Useful overrides:

```bash
SOAK_SEC=60 PERF_WINDOW_SEC=30 scripts/run-migration.sh    # quicker cycle
EXTRA_SWAP_ARGS=--maintenance scripts/run-migration.sh     # drain the old broker before decommission
```

## Individual pieces

```bash
scripts/swap.sh io-b2 nv-n2 eu-central-1a b2 --n 1 [--raise-recovery-rate [bytes]] [--maintenance] [--soak 180]
scripts/check-seeds.sh
scripts/audit-placement.sh --label manual
scripts/check-tiered-storage.sh --label manual
scripts/check-fast-commission.sh --config --expect enabled --label manual
scripts/check-fast-commission.sh --evidence nv-n2 --label manual
scripts/recovery-rate.sh save | raise [bytes] | restore | assert-setup
scripts/dns-repoint.sh b2 nv-n2
scripts/perf-latency.sh --label manual --phase baseline
scripts/clients.sh start | stop | restart | status
scripts/broker.sh start|stop|kill|restart|rm <instance>
```

## Scenarios

Each scenario requires a healthy 5-broker cluster, and leaves one behind. Run them after the migration.

```bash
scripts/run-scenarios.sh          # 1-7 and 9
scripts/run-scenarios.sh 4 6      # selected scenarios
```

| # | Script | What it shows |
|---|---|---|
| 1 | `01-abort-decommission.sh` | Recommissioning mid-decommission works. A fully decommissioned broker does not rejoin by restarting. |
| 2 | `02-wipe-new-broker.sh` | Wiping a broker's NVMe means a new node ID, no second cluster, the dead ID decommissioned, and the alias repointed. |
| 3 | `03-wipe-seed-broker.sh` | Same as 2, for the broker behind the seed alias `b1` |
| 4 | `04-guardrail.sh` | An empty-disk broker with unreachable seeds never forms a cluster |
| 5 | `05-restart-with-data.sh` | A restart with data intact keeps the same node ID |
| 6 | `06-rack-failure.sh` | With both eu-central-1a brokers down, there are no leaderless partitions and `acks=all` produce works |
| 7 | `07-join-via-repointed-seeds.sh` | A new broker joins via the repointed seeds, even with only one seed reachable |
| 8 | `08-join-after-repoint.sh` | A broker joins within one TTL of a seed repoint. Runs as a hook in swap 3. |
| 9 | `09-auto-decommission.sh` | Skipped: the property does not exist in 25.3.17 |

## Reset

```bash
scripts/down.sh                   # checks the recovery rate is restored, then removes all containers and volumes
scripts/down.sh --purge-results   # also empties results/
```

`down.sh` resets `coredns/rp.test.hosts` to its initial state, with every alias pointing at its io-b* broker.

## Notes

- Clients, seeds and the perf tools only ever use the aliases `b1`–`b5.rp.test`. Brokers advertise
  their instance names (`io-b*`, `nv-n*`, `tmp-1`, `sp-*`).
- Every script uses `set -euo pipefail` and an ERR trap. Gates are recorded in `results/gates.csv`.
- The recovery rate is restored by a trap on every exit path of `swap.sh`.
  `recovery-rate.sh assert-setup` checks that it matches `results/recovery-rate.setup`.
