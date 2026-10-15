# Broker replacement runbook: operator scripts

These scripts carry out the runbook for replacing all five brokers one at a time
(Path A: the seed and client DNS names are repointed; `seed_servers` never changes). Each
`step_NN` script is one runbook step. Run them **by hand, in number order**, read each
script's output, and continue only after the line `All checks passed`.

- Scripts that only read are marked `READ-ONLY` in their header.
- Scripts that change something print the exact command first and ask `y/N`.
- `--dry-run` prints changes without running them. `--yes` skips the prompts (not recommended in production).
- Every run is logged, and every change made is listed in `changes.log` in the state directory.

[How a broker replacement works](#how-a-broker-replacement-works) explains the logic behind the steps.
There are two ways to run them:

| | Where | Settings | Section |
|---|---|---|---|
| **1. Docker rehearsal** | a bastion container on your laptop, against the Docker test harness | `rehearsal/runbook.env` | [Part 1](#part-1-run-the-runbook-on-docker) |
| **2. Production** | the production bastion host, against the real cluster | `runbook.env` | [Part 2](#part-2-run-the-runbook-in-production) |

Rehearse on Docker first. Don't run the steps directly on your laptop with the production
`runbook.env`: its `CHANGE-ME` values (CA file, DNS names) don't exist there, so the steps stop
at the first check.

## How a broker replacement works

Every broker is replaced the same way: **add the new broker first, let it take over its share of the data,
then remove the old one**. The cluster never runs with fewer than five healthy brokers, and every partition
keeps one replica in each AZ the whole time. Nothing is restarted, and no client or broker configuration
changes. At the end, only one DNS alias points somewhere else.

### One swap, step by step

The example is swap 1 of the rehearsal: alias `b2`, old broker `io-b2` (node 3), new broker `nv-n2`, rack
eu-central-1a.

```
          before              add new                 move data              remove old              repoint + retire
   b2 ──▶ io-b2 (node 3)   b2 ──▶ io-b2 + nv-n2     b2 ──▶ io-b2 + nv-n2     b2 ──▶ io-b2 (no data)   b2 ──▶ nv-n2
          5 brokers        6 brokers (nv-n2 empty)  nv-n2 gets replicas      5 members + old process  5 brokers
                           step_12–13               step_14                  step_15–16               step_18–19
```

| # | Step | What happens in the cluster | Why at this point |
|---|---|---|---|
| 1 | `step_10` go / no-go | Nothing changes. Checks health, idle balancer, 0 under-replicated partitions, one replica per rack, original recovery rate, seed rules, and that the previous swap finished its soak | Never start a swap on a cluster that is already moving data or degraded |
| 2 | `step_11` *(optional)* raise recovery rate | `raft_learner_recovery_rate` goes up for this swap; a watcher restores it if client latency stays above 2× baseline | Faster copying for big brokers, at the cost of more load |
| 3 | `step_12` prepare new broker | The new broker's **own** DNS name is created or checked; its `redpanda.yaml` settings are generated (same rack, same seed list, own advertised name, no `node_id`, empty disk) | Clients will connect to it by that name as soon as it leads a partition |
| 4 | *(on the host)* start Redpanda | The new broker contacts the seeds (`b1`/`b3`/`b5`), gets a **new node ID** and joins as a 6th, empty member | Joining needs no seed list change: the seed aliases still point at live brokers |
| 5 | `step_13` confirm join | Nothing changes. Checks the new ID, rack, version, config, and reachability by name | Catch a wrong rack or seed list before any data lands on it |
| 6 | `step_14` settle | The continuous balancer moves replicas **onto** the new broker until it holds its share. With fast commission, Tiered Storage replicas start from data already in object storage, so little is copied | The new broker must be a full member before the old one is drained, so no partition ever drops below 3 replicas |
| 7 | `step_15` decommission old | Redpanda moves every replica and leadership **off** the old broker (mostly to its rack-mates, including the new one), then removes it from the cluster | Only now; until it completes it can be aborted with `recommission` |
| 8 | `step_16` verify | Nothing changes. Checks the old ID is gone, health, placement, and the fast commission evidence | Proof that the cluster is complete without the old broker |
| 9 | `step_17` restore recovery rate | `raft_learner_recovery_rate` back to the original (no-op if never raised) | The soak and the next swap must run at the normal rate |
| 10 | `step_18` repoint DNS | The alias (`b2`) moves from the old broker to the new one | The old broker has left the cluster, so the alias must lead new clients and new brokers to a live member. It happens **before** the old process is stopped, so the alias never points at a dead address |
| 11 | `step_19` retire old | The old instance is stopped and terminated, and removed from inventory, monitoring and security groups | Only after the repoint |
| 12 | `step_20` soak | Nothing changes; watch health and clients (24 h in production) | The next swap only starts after a clean soak |

### Rules that hold for the whole migration

- **Add before remove.** The new broker joins and settles (`step_13`–`14`) before the old one is decommissioned (`step_15`).
- **One replica per AZ for every partition**, before, during and after each swap. The new broker uses the old broker's rack, and the placement audit runs at every gate.
- **New brokers always get a new node ID.** Never set `node_id`, and always start on an empty disk.
- **`seed_servers` never changes.** It lists the seed aliases (`b1`, `b3`, `b5`, one per AZ) on every broker. Seeds change only by repointing DNS.
- **Repoint an alias only after the old broker's decommission has completed, and before its instance is removed.**
- **Brokers advertise their own unique name, never an alias.** Clients bootstrap through the aliases, then connect to brokers by these names. See [Two kinds of DNS names](#two-kinds-of-dns-names).
- **One swap at a time, in plan order:** b2, b4 (the non-seed brokers first), then b1, b3 (seeds), and b5 last (the lone eu-central-1c broker, the largest transfer).
- **The recovery rate is back at its original value** before the repoint, the soak and the next swap.

### DNS and seed server strategy

The migration uses **Path A**. Brokers and clients find the cluster through five **repointable DNS
aliases**, and moving an alias is the only DNS change a swap makes. `seed_servers` and client
configuration are never edited.

#### The DNS records

| Record | Example (production / rehearsal) | Points at | Used by | Changes |
|---|---|---|---|---|
| **Seed alias** (3, one per AZ) | `b1`, `b3`, `b5.kafka.example.com` / `b1`, `b3`, `b5.rp.test` | one live broker in that AZ | `seed_servers` on every broker, and client `bootstrap.servers` | repointed in the seed swaps (3, 4, 5) |
| **Client-only alias** (2) | `b2`, `b4…` | one live broker | client `bootstrap.servers` | repointed in swaps 1 and 2 |
| **Broker's own name** (one per broker) | `n2.kafka.internal.example.com` / `nv-n2.rp.test` | that broker's IP, for its whole life | its `advertised_kafka_api` / `advertised_rpc_api`: what clients and brokers connect to after bootstrap | created before the broker joins (`step_12`), deleted when it is retired |

Aliases have a short TTL: at most 60 s in production (`MAX_DNS_TTL`, checked by `step_01`), and 5 s in the
rehearsal. Lower it in Phase 0, well before the first swap, so the old, longer TTL has expired everywhere.

#### Seed servers

Every broker, old and new, has the **same** seed list. It never changes during the migration:

```yaml
seed_servers:
  - host: {address: b1.kafka.example.com, port: 33145}    # eu-central-1a
  - host: {address: b3.kafka.example.com, port: 33145}    # eu-central-1b
  - host: {address: b5.kafka.example.com, port: 33145}    # eu-central-1c
```

- **Seeds are only used when a broker starts with an empty disk.** It contacts a seed to register and get its node ID. A running broker, or one restarting with its data, already knows the cluster and doesn't need the seeds.
- **One seed per AZ:** losing a whole AZ, or having one seed alias in transition, still leaves two working seeds. A new broker only needs to reach one. The harness migration test (scenario 7) joined a new broker successfully with two of the three seed aliases pointing at an unreachable address.
- **A new broker never lists itself.** Its own name (`NEW_HOST`) is never one of the seed aliases. `step_12` refuses such a plan, and `step_13` checks the running broker.
- **Seeds always point at active members.** `step_01`, `step_10`, `step_13`, `step_18` and `step_30` check this rule. The one accepted exception is the short window described below.
- **25.3.17 behaviour:** with `empty_seed_starts_cluster: false`, a broker decides whether it is a *founding* seed by comparing its own advertised address, as text, with `seed_servers`. Aliases never match an instance name, so **a brand-new cluster cannot be formed** with this alias-only seed list. Joining an existing cluster works, which is all the migration needs. If a cluster ever has to be formed from scratch, the founders must temporarily list their own instance names.

#### Which alias moves in which swap

Each swap moves exactly one alias, in `step_18`, after the old broker's decommission completed:

| After swap | b1 (seed, 1a) | b2 (1a) | b3 (seed, 1b) | b4 (1b) | b5 (seed, 1c) |
|---|---|---|---|---|---|
| start | old b1 | old b2 | old b3 | old b4 | old b5 |
| 1 | old b1 | **new n2** | old b3 | old b4 | old b5 |
| 2 | old b1 | new n2 | old b3 | **new n4** | old b5 |
| 3 | **new n1** | new n2 | old b3 | new n4 | old b5 |
| 4 | new n1 | new n2 | **new n3** | new n4 | old b5 |
| 5 | new n1 | new n2 | new n3 | new n4 | **new n5** |

**How to read the table**

- **Columns** are the five repointable DNS names (aliases). The heading gives each one's role and AZ. `b1`, `b3`, `b5` are the **seed** aliases, listed in every broker's `seed_servers` and used by clients. `b2`, `b4` are used by clients only.
- **Rows** are the state of the cluster after each completed swap, starting from before the migration.
- **Each cell** is the broker the alias resolves to at that moment. "old b2" is the original broker behind `b2` (`io-b2` in the rehearsal, `b2-old…` in production); "new n2" is its replacement (`nv-n2` / `n2…`).

| Row | What happened | Seed aliases now point at |
|---|---|---|
| start | nothing yet: every alias points at its original broker | old b1, old b3, old b5 |
| 1 | broker 2 replaced; **`b2`** now points at new n2 | unchanged: no seed was touched |
| 2 | broker 4 replaced; **`b4`** now points at new n4 | unchanged |
| 3 | broker 1 replaced; **`b1`** (first seed alias) now points at new n1 | new n1, old b3, old b5 |
| 4 | broker 3 replaced; **`b3`** (second seed alias) now points at new n3 | new n1, new n3, old b5 |
| 5 | broker 5 replaced; **`b5`** (third seed alias) now points at new n5 | new n1, new n3, new n5: every seed is a new broker |

**What the table shows**

- **Exactly one cell changes per row.** Each swap repoints one alias, in `step_18`, and only after the old broker's decommission has completed.
- **The seed columns always cover all three AZs.** In every row, `b1` is a live broker in 1a, `b3` in 1b and `b5` in 1c. A broker starting with an empty disk can always find the cluster, even while one seed alias is being moved.
- **No seed ever points at a removed broker.** By the time an alias moves, its old broker is no longer a member, and the old instance is stopped only afterwards (`step_19`).
- **`seed_servers` reads `b1`, `b3`, `b5` in every row.** The table changes, but the broker configuration doesn't, so no broker is edited or restarted for seed reasons.
- **The order is deliberate:**
  - **Client-only aliases first (rows 1–2).** The procedure is proven on brokers that aren't seeds before any seed alias moves.
  - **Then the seeds (rows 3–5).** One at a time, so at most one seed alias is ever in transition.
  - **`b5` last.** It's the only broker in eu-central-1c, so it holds a replica of every partition and is the largest transfer.

**Not shown in the table: the state during a swap.** Each row is the state **after** a swap. During a swap,
from the moment the new broker joins until `step_18`, the alias still points at the old broker. After
row 2, for example, while swap 3 runs, `b1` points at old b1 the whole time, even though new n1 is already in
the cluster. It switches to new n1 only at the end of that swap.

#### The repoint window

Between the end of the decommission (`step_15`) and the repoint (`step_18`), the alias still points at the
old broker. That broker is no longer a member: it holds no data and is not in the metadata. Its process
still runs, so the address answers, but with a stale view. The window is kept short: only the verify step
and the recovery-rate restore run in between, under a minute in an uninterrupted swap. The old broker is
stopped only **after** the repoint (`step_19`), so the alias never points at a dead address.

- **Swaps 1–2 (client-only aliases).** A client that starts during the window and tries `b2` first may get a stale answer from the departing broker. It should then use its other bootstrap names, which is why every client lists all five.
- **Swaps 3–5 (seed aliases).** The other two seeds are still valid, so a new broker can still join. Still, **don't start a new broker during this window.**

#### TTL and client DNS caches

After the repoint, `step_18` waits one TTL, and then checks with `dig` that the alias returns the new IP.
Clients may cache DNS for longer than the TTL. Java clients, for example, cache according to the JVM
setting `networkaddress.cache.ttl`. This matters only for a client that starts or restarts and bootstraps
in that time; running clients use the brokers' own names from the metadata. Check that your client
platforms don't pin DNS answers for long periods.

#### Not used: editing the seed list

The production runbook also describes **Path B1** (convert IP seeds to DNS once) and **Path B2** (edit
`seed_servers` in each seed swap: add the new broker, then remove the old one). Neither is used, because the
cluster already uses repointable DNS names. They were not rehearsed.

See also [Two kinds of DNS names](#two-kinds-of-dns-names) and
[Client bootstrap through the aliases](#client-bootstrap-through-the-aliases) in Part 2.

### Configuration changes and why

The runbook changes only a few settings. **None of them needs a broker restart**: they are cluster
properties, applied live (`rpk cluster config status` confirms "no restart needed" after each change).
Everything temporary is restored, and the original values are saved by `step_02`.

#### Cluster properties the runbook changes

| Property | Default (25.3.17) | Changed to | Step | Restored | Why |
|---|---|---|---|---|---|
| `initial_retention_local_target_ms_default` | `null` (off) | e.g. `3600000` (1 h) | `step_04` | kept on purpose | **Fast commission.** A new replica of a Tiered Storage topic copies only this recent window from the leader. Older data stays in object storage and is read from there on demand. Without it, every new broker copies the full local retention of each replica through Raft, which is more data, more time and more load on the existing brokers. Set it above normal consumer lag, so consumers rarely read below it |
| `initial_retention_local_target_bytes_default` | `null` (off) | optional size cap, e.g. 1 GiB | `step_04` | kept | Same as above, as a size limit per replica instead of, or as well as, the time window |
| `raft_learner_recovery_rate` | `104857600` (100 MiB/s) | `209715200` (200 MiB/s), **optional** | `step_11` | `step_17`, every swap | How fast each broker streams data to new replicas. Raising it shortens the copy for large brokers, at the cost of disk and network load on the existing brokers. It must be back at the original before the repoint, the soak and the next swap, so every swap starts at the same, known rate. The `step_11` watcher restores it early if client latency stays above 2× baseline |
| `partition_autobalancing_concurrent_moves` | `50` | lower, e.g. `10`, **optional** | `step_02` | `step_31` | Limits how many partitions move at the same time, so the copying onto a new broker and off an old one doesn't overload the cluster. Fewer moves at once means a gentler, slower swap |
| leader pinning (`default_leaders_preference`, topic `redpanda.leaders.preference`) | `none` | only if you add it for the migration | – | `step_31` | Not needed by the runbook. `step_31` removes pinning that was added temporarily, and keeps pinning that existed before |

Fast commission stays enabled after the migration, because it helps every future broker replacement.
Turn it off with `rpk cluster config set initial_retention_local_target_ms_default null`, and the same
for `_bytes_default`.

#### Settings that must already be in place (checked, not changed)

`step_01` checks these. If one is wrong, fix it **before** the migration. They are prerequisites, not part
of the swap. In the Docker rehearsal, `scripts/configure-cluster.sh` (Part 1, section 1b) sets them on
the test cluster. Turning on Tiered Storage (`cloud_storage_enabled` and its connection settings) needs a rolling restart;
`configure-cluster.sh` does one restart after setting everything.

| Property | Required | Why it matters for the swap |
|---|---|---|
| `enable_rack_awareness` | `true` | Keeps exactly one replica of every partition in each AZ while replicas move onto the new broker and off the old one. Without it, the balancer could put two replicas in one AZ |
| `partition_autobalancing_mode` | `continuous` | The balancer moves replicas **onto** the new broker by itself after it joins (`step_14`), and keeps rebalancing if something else gets out of balance during the migration (rack constraint violations, disk usage, a broker down). `node_add` also fills a new broker, but does nothing else; with `off`, the new broker would stay empty |
| `cloud_storage_enabled` (+ topic `redpanda.remote.write` / `read`) | `true` | Fast commission only works for topics in Tiered Storage: the data a new replica doesn't copy must exist in object storage. Topics without it are always copied in full |
| `write_caching_default` | `false` (WARN otherwise) | With write caching, `acks=all` can acknowledge data before it is on disk. During replica moves that is a durability risk, so the runbook expects it off |
| broker `rack` | the AZ name | Rack awareness only works if every broker, old and new, has its AZ as rack |
| broker `empty_seed_starts_cluster` | `false` | A broker with an empty disk must **join** the existing cluster, and never form a new one |
| `retention_local_strict` | informational | With `false` (the default), Tiered Storage partitions may keep more local data than `retention.local.target.*` while disk is free. That's not wrong, but existing brokers then hold more local data than the target |

#### The new broker's `redpanda.yaml`

`step_12` generates these node settings for each new host. Everything else (TLS and SASL listeners,
tuning) is copied unchanged from the existing brokers' configuration.

| Setting | Value | Why |
|---|---|---|
| `rack` | the **old** broker's AZ | The new broker takes over the old one's place in its AZ, so one-replica-per-AZ placement holds |
| `empty_seed_starts_cluster` | `false` | It must join the existing cluster, never form a new one |
| `seed_servers` | the **same** three seed aliases as every broker | It finds the cluster through the seeds when it first starts. The list is identical everywhere and never edited, and it never contains the broker's own name |
| `advertised_kafka_api` / `advertised_rpc_api` | the broker's **own** DNS name (`NEW_HOST`) | Clients and other brokers connect to it by this name. It has to be unique and permanent, never an alias that moves (see [Two kinds of DNS names](#two-kinds-of-dns-names)) |
| `kafka_api` / `rpc_server` | `0.0.0.0` (as on the existing brokers) | The address it listens on |
| `node_id` | **not set** | The cluster assigns a new, never-used ID. Reusing an old ID would confuse the cluster's membership and replica history |
| data directory | **empty** | An empty disk makes the broker join as a new member. Leftover data from another broker or cluster would make it start with the wrong identity |

### How the scripts enforce the order

Each per-swap step writes a marker file to `state/swap-N-<alias>/` (`rehearsal/state/…` in the
rehearsal). The next step refuses to run without it:

| Step | Requires | Writes |
|---|---|---|
| `step_10` | `soak.ok` of the **previous** swap | `go.ok`, `old_id` |
| `step_11` | `go.ok` | `raised` (removed by `step_17`) |
| `step_12` | `go.ok` | `prepared.ok` |
| `step_13` | (waits for the new broker to join) | `new_id`, `joined_at` |
| `step_14` | `new_id` | `settled.ok` |
| `step_15` | `settled.ok` | `decommissioned.ok` |
| `step_16` | `decommissioned.ok` | `verified.ok` |
| `step_17` | – (always safe to run) | `rate_restored.ok` |
| `step_18` | `verified.ok`, `rate_restored.ok`, and the old ID no longer a member | `repointed.ok` |
| `step_19` | `repointed.ok` | `retired.ok` |
| `step_20` | `retired.ok` | `soak.ok` |

Every step can be re-run safely. A check that fails changes nothing; fix the cause and run the step again.

### Where you can still go back

| You are… | Going back means | How |
|---|---|---|
| before `step_15` (new broker joined, old untouched) | remove the new broker | decommission the **new** node ID (`rpk redpanda admin brokers decommission <new-id>`), stop it, delete its DNS record. The old broker never stopped serving |
| during `step_15` (decommission still running) | keep the old broker | `rpk redpanda admin brokers recommission <old-id>` |
| after `step_15` completed | not possible | a decommissioned broker cannot rejoin. Go forward: the new broker already holds the data |
| after `step_18` | not needed | the alias points at a live member; re-pointing it back would point at a broker that has left the cluster |

If anything looks wrong before `step_15`, stopping is cheap: the cluster still has all five original
brokers, plus one extra.

---

## Part 1: Run the runbook on Docker

A container plays the bastion host. It joins the test harness network and uses the harness DNS,
so it sees the cluster exactly as a bastion would see production.

```
laptop ── docker exec ──▶ rb-bastion (rpk, jq, curl, dig, fake aws)
                              │  network rpnet, DNS = harness CoreDNS 172.30.0.53
                              ▼
               b1..b5.rp.test ──▶ harness brokers (Redpanda 25.3.17)
```

### Quick start (all commands in order)

Terminal 1 is the laptop shell, in `broker-replaceent-test/`. Terminal 2 is the bastion shell. Each part is
explained in the numbered sections below.

```bash
# --- laptop, broker-replaceent-test/ : test cluster (section 1) ---
scripts/up.sh                         # DNS, MinIO, 5 brokers io-b1..io-b5   (first run: also builds images)
scripts/configure-cluster.sh          # Tiered Storage, racks, balancing; fast commission stays OFF
scripts/seed-data.sh                  # optional: business-like topics, e.g. order.wal.btc-try

# --- laptop, runbook/ : bastion (section 2) ---
cd runbook
rehearsal/bastion.sh start
rehearsal/bastion.sh shell            # -> you are now "on the bastion", in /runbook

# --- bastion: once, before the first swap (section 5) ---
./step_00_setup_test_clients.sh
./step_01_preflight.sh
./step_02_save_baselines.sh
./step_03_fast_commission_check.sh    # must say DISABLED
./step_04_fast_commission_enable.sh
./step_03_fast_commission_check.sh enabled

# --- bastion: one swap, SWAP_PLAN line 1 = b2: io-b2 -> nv-n2 (sections 3-5) ---
./step_10_go_no_go.sh 1
./step_11_raise_recovery_rate.sh 1    # optional
./step_12_prepare_new_broker.sh 1
#     laptop, broker-replaceent-test/:  scripts/broker.sh start nv-n2
./step_13_confirm_join.sh 1
./step_14_wait_settle.sh 1
./step_15_decommission.sh 1
./step_16_verify.sh 1
./step_17_restore_recovery_rate.sh 1  # always
./step_18_dns_repoint.sh 1
#     laptop, broker-replaceent-test/:  scripts/broker.sh rm io-b2
./step_19_retire_old_broker.sh 1
./step_20_soak_check.sh 1

# --- bastion: finish ---
./step_99_stop_test_clients.sh --delete-topics
exit

# --- laptop: stop (section 6) or throw everything away (section 7) ---
rehearsal/bastion.sh stop
```

### Prerequisites (laptop)

- Linux with Docker Engine and Docker Compose v2.
- About 10 GiB of RAM for Docker during a rehearsal: up to 6 brokers at 1.4 GiB each, plus MinIO.
- About 5 GiB of free disk. Add about 8 GiB if you run `scripts/seed-data.sh`.
- Host tools for the harness scripts: `bash`, `jq`, `dig`, `curl`, `python3`. The bastion container brings
  its own `rpk`, `jq`, `curl` and `dig`.
- Internet access on the first run. `scripts/up.sh` builds MinIO and `mc` from their GitHub release tags,
  because MinIO no longer publishes images, and pulls the Redpanda, CoreDNS and Python images.
  `rehearsal/bastion.sh start` builds the bastion image. The first run takes a few minutes; later runs
  reuse the images.

### 1. Set up the test cluster

All commands in this section run on the laptop, in the harness directory (the parent of `runbook/`).

**Is it already set up?** Check before running anything:

```bash
cd ..                                                         # broker-replaceent-test/
docker ps --format '{{.Names}}' | grep -E '^rp-'              # brokers rp-io-b*/rp-nv-n*/..., rp-coredns, rp-minio
docker exec rp-io-b1 rpk cluster config get cloud_storage_enabled     # "true" = configure-cluster already ran
```

| What you see | What to do |
|---|---|
| no `rp-*` containers | run **a**, **b** (and optionally **c**) |
| brokers running, `cloud_storage_enabled` is `false` | run **b** (and optionally **c**) |
| brokers running, `cloud_storage_enabled` is `true` | already set up; go to section 2 |
| the broker in the command doesn't exist (e.g. after earlier swaps) | use any running broker container from `docker ps` instead of `rp-io-b1` |

**a. Start the cluster** (about 1–2 minutes; longer on the first run while images build):

```bash
scripts/up.sh
```

This starts CoreDNS (the `rp.test` DNS zone with the aliases `b1`–`b5`), MinIO with the bucket
`redpanda-ts`, and five brokers `io-b1`–`io-b5` in racks eu-central-1a/1a/1b/1b/1c, on Redpanda 25.3.17.
It ends with `up: 5 brokers, healthy`. Re-running it against a running cluster only verifies it.

**b. Configure the cluster** (about 1 minute; it restarts each broker once):

```bash
scripts/configure-cluster.sh
```

This turns on Tiered Storage on MinIO, rack awareness, continuous partition balancing and
`retention_local_strict`, sets `write_caching_default=false`, and throttles moves to 10 at a time. It
records the original `raft_learner_recovery_rate`. **Fast commission stays off**, so the runbook's
`step_03`/`step_04` can turn it on as in production. It ends with `configure-cluster done`.

Without this step, `step_01` fails on `enable_rack_awareness = false` and `cloud_storage_enabled = false`.

**c. Optional: load business-like data** (about 5 minutes):

```bash
scripts/seed-data.sh
```

This creates 20 background topics, the sequencer topic `order.wal.btc-try` and a control topic. It fills
them with about 50 MB per partition, and waits until the older data exists only in MinIO. Then the
runbook's Tiered Storage and fast commission checks have real data to look at. Without it, `step_01`
shows one warning ("no topics match IMPORTANT_TOPICS_REGEX"), and the rehearsal still works with the
test topics from `step_00`.

```bash
cd runbook
```

### 2. Start the bastion and connect to it

All commands in this section run on the laptop, in `runbook/`.

```bash
rehearsal/bastion.sh start     # builds the image rb-bastion:25.3.17 the first time, then starts the container
rehearsal/bastion.sh status    # should list rpk v25.3.17 and b1..b5.rp.test with IP addresses
rehearsal/bastion.sh shell     # interactive shell inside the bastion, in /runbook
```

Inside the shell, run the steps exactly as in production, starting with `step_00` (section 5). The y/N
prompts work here.

```bash
./step_00_setup_test_clients.sh      # first: creates the test topics and starts the test clients
./step_01_preflight.sh
./tools/test_clients.sh status       # client health at any time
exit                                 # leaves the shell; the container keeps running
```

Or run a single step from the laptop, without opening a shell:

```bash
rehearsal/bastion.sh run step_10_go_no_go.sh 1
```

### 3. Choose a swap that fits the harness as it is now

Edit `SWAP_PLAN` in `rehearsal/runbook.env`. It holds one line per swap:
`DNS_NAME OLD_HOST NEW_HOST RACK NEW_IP`.

- `OLD_HOST`: the instance behind the alias today. Check with `grep alias ../coredns/rp.test.hosts`.
- `NEW_HOST`: a harness instance **in the same rack** that is not running yet. Check with
  `docker ps --format '{{.Names}}' | grep rp-`.

**On a fresh cluster** (after `scripts/up.sh`), the aliases point at `io-b1`–`io-b5`, and each one has a
matching replacement `nv-n1`–`nv-n5` in the same rack:

| Alias | OLD_HOST | NEW_HOST | Rack | NEW_IP |
|---|---|---|---|---|
| b1 | io-b1.rp.test | nv-n1.rp.test | eu-central-1a | 172.30.0.21 |
| b2 | io-b2.rp.test | nv-n2.rp.test | eu-central-1a | 172.30.0.22 |
| b3 | io-b3.rp.test | nv-n3.rp.test | eu-central-1b | 172.30.0.23 |
| b4 | io-b4.rp.test | nv-n4.rp.test | eu-central-1b | 172.30.0.24 |
| b5 | io-b5.rp.test | nv-n5.rp.test | eu-central-1c | 172.30.0.25 |

The default in `rehearsal/runbook.env`, and the example used in the rest of Part 1, is the first swap of
the production order:

```bash
SWAP_PLAN=(
  "b2.rp.test  io-b2.rp.test  nv-n2.rp.test  eu-central-1a  172.30.0.22"
)
```

To rehearse all five swaps, list all five lines in the production order b2, b4, b1, b3, b5.
After earlier rehearsals the aliases point elsewhere. Pick `OLD_HOST` from the alias file, and a
`NEW_HOST` that isn't running, from the spare instances: `sp-1` (172.30.0.61) or `tmp-1` (172.30.0.31) in
eu-central-1a, `sp-2` (172.30.0.62) in eu-central-1b, `sp-3` (172.30.0.63) in eu-central-1c.

### 4. Start and stop brokers ("host" actions)

In production, two steps involve work **on a broker host**: starting Redpanda on the new host, and
stopping and terminating the old one. In the rehearsal, those hosts are harness containers, so you
do this **on the laptop**, in a **second terminal**, with the harness script `scripts/broker.sh`.
The bastion shell stays open in the first terminal.

The scripts print the exact command to use at the right moment.

#### Start the new broker (after `step_12`)

`step_12` ends with:

```
Next: START the new broker -> on the laptop, in broker-replaceent-test/: scripts/broker.sh start nv-n2
      then run: ./step_13_confirm_join.sh b2.rp.test
```

In the second terminal:

```bash
cd <your-checkout>/broker-replaceent-test     # the harness directory (parent of runbook/)
scripts/broker.sh start nv-n2                            # instance name = NEW_HOST without .rp.test
```

Expected output:

```
starting nv-n2 (172.30.0.22, rack eu-central-1a, volume rpbr-nv-n2-data)
```

Make sure it starts **empty**. `docker volume ls | grep rpbr-nv-n2` should print nothing *before* you
run the start command. If a volume is left over from an earlier rehearsal, run `scripts/broker.sh rm nv-n2` first.

This does what an operator does on a real new host. It writes `nv-n2`'s `redpanda.yaml` (rack
`eu-central-1a`, seeds `b1`/`b3`/`b5.rp.test`, advertised name `nv-n2.rp.test`, no `node_id`).
It then starts the container `rp-nv-n2` with a **fresh, empty data volume**, using Redpanda 25.3.17.

Check that it is running, and watch it join (it usually takes 5–15 s):

```bash
docker ps --filter name=rp-nv-n2                                         # STATUS should be "Up ..."
docker logs rp-nv-n2 2>&1 | grep -E 'cluster_discovery|ERROR' | tail     # seed contact / errors
```

Then, back in the bastion shell: `./step_13_confirm_join.sh b2.rp.test`. It waits up to 15 minutes
for the broker to join.

If it does not join:

| Symptom | Cause / fix |
|---|---|
| `docker ps` shows nothing for `rp-nv-n2` | Start failed. Run `docker logs rp-nv-n2`. To start again from empty, run `scripts/broker.sh rm nv-n2`, then `scripts/broker.sh start nv-n2` |
| log repeats `Requesting node ID ... from {host: b1.rp.test ...}` | Can't reach the seeds. Check `rehearsal/bastion.sh status`, and that the seed brokers are up (`docker ps`) |
| `step_13` fails "node id ... was never used before" | `nv-n2` reused a data volume from an earlier rehearsal (`broker.sh start` keeps an existing volume), so it came back with its old ID. Decommission that ID (`docker exec rp-io-b1 rpk redpanda admin brokers decommission <id>`, using any running broker), then `scripts/broker.sh rm nv-n2` (deletes container **and** volume) and `scripts/broker.sh start nv-n2` |

#### Stop and remove the old broker (during `step_19`)

Only after `step_18` has repointed the DNS name. `step_19` shows this checklist item:

```
Done: old broker stopped and its instance terminated: on the laptop, in broker-replaceent-test/: scripts/broker.sh rm io-b2 ?
```

**Before** answering `y`, in the second terminal:

```bash
cd <your-checkout>/broker-replaceent-test
scripts/broker.sh rm io-b2        # instance name = OLD_HOST without .rp.test
```

Expected output:

```
removed io-b2 container and volume rpbr-io-b2-data
```

This is the rehearsal's "terminate the instance": the container and its data volume are deleted.
`step_19` then checks that `io-b2.rp.test:9092` no longer answers.

#### Other harness broker commands

| Command (in `broker-replaceent-test/`) | Effect |
|---|---|
| `scripts/broker.sh start <instance>` | start an instance (empty volume on first start; existing container is just started again) |
| `scripts/broker.sh stop <instance>` | stop the container, keep its data |
| `scripts/broker.sh restart <instance>` | restart with data intact |
| `scripts/broker.sh rm <instance>` | delete container **and** volume (an NVMe wipe / terminated instance) |
| `docker ps --format '{{.Names}} {{.Status}}' \| grep rp-` | which brokers are running |
| `docker exec rp-io-b1 rpk redpanda admin brokers list` | cluster membership as Redpanda sees it (use any running broker container) |

### 5. A complete rehearsal swap

Inside `rehearsal/bastion.sh shell`, with the example `SWAP_PLAN` above:

```bash
# once
./step_00_setup_test_clients.sh            # test topics + data + clients (keep them running)
./step_01_preflight.sh
./step_02_save_baselines.sh
./step_03_fast_commission_check.sh
./step_04_fast_commission_enable.sh
./step_03_fast_commission_check.sh enabled

# the swap (1 = first line of SWAP_PLAN)
./step_10_go_no_go.sh 1
./step_11_raise_recovery_rate.sh 1         # optional
./step_12_prepare_new_broker.sh 1          # creates nv-n2's DNS record if missing
#   SECOND TERMINAL (laptop, broker-replaceent-test/):  scripts/broker.sh start nv-n2   -> see section 4
./step_13_confirm_join.sh 1
./step_14_wait_settle.sh 1
./step_15_decommission.sh 1
./step_16_verify.sh 1
./step_17_restore_recovery_rate.sh 1
./step_18_dns_repoint.sh 1                 # "Route 53" = the harness DNS file
#   SECOND TERMINAL (laptop, broker-replaceent-test/):  scripts/broker.sh rm io-b2     -> see section 4
./step_19_retire_old_broker.sh 1
./step_20_soak_check.sh 1                  # 2-minute soak in the rehearsal

# at the end
./step_99_stop_test_clients.sh --delete-topics
```

**Next swap:** add the next line to `SWAP_PLAN` in `rehearsal/runbook.env` (production order b2, b4, b1, b3,
b5; see the table in section 3), then run `step_10` … `step_20` again with the new swap number, e.g. `2`.
`step_10` refuses until the previous swap's soak is recorded (`step_20`). No bastion restart is needed: the
settings file is read at every step.

**After the last swap:**

```bash
./step_30_final_check.sh                   # only passes when all five brokers were swapped (see below)
./step_31_restore_temp_settings.sh         # restores partition_autobalancing_concurrent_moves, etc.
./step_99_stop_test_clients.sh --delete-topics
```

`step_30_final_check.sh` expects every broker in the cluster to be a new host from `SWAP_PLAN`, so
it only passes in a rehearsal that swaps all five brokers.

### 6. Stop or reset

```bash
rehearsal/bastion.sh stop      # stops the test clients and removes the container (rehearsal state is kept)
rehearsal/bastion.sh reset     # also deletes rehearsal/state and rehearsal/logs, to start a rehearsal from scratch
```

`reset` only clears the bastion side. The test cluster keeps the result of earlier swaps. For a completely
fresh cluster, see section 7.

### 7. Start over from scratch (new test cluster)

To throw away the bastion **and** the test cluster and begin again with a fresh 5-broker cluster:

**a. If a rehearsal swap is in progress, restore the recovery rate first.** `../scripts/down.sh`
stops with an error while `raft_learner_recovery_rate` is still raised (`step_11` without `step_17`):

```bash
rehearsal/bastion.sh run step_17_restore_recovery_rate.sh 1 --yes     # 1 = the swap you raised it for
```

`rehearsal/bastion.sh reset` refuses while a swap still has the rate raised. If the rehearsal state is
already gone and `down.sh` fails with `raft_learner_recovery_rate is 209715200, setup value was 104857600`,
set it back by hand on any running broker, then run `down.sh` again:

```bash
broker="$(docker ps --format '{{.Names}}' | grep -m1 -E '^rp-(io|nv|sp|tmp)')"     # any running broker
docker exec "$broker" rpk cluster config set raft_learner_recovery_rate "$(cat ../results/recovery-rate.setup)"
```

**b. Remove the bastion and the rehearsal state:**

```bash
rehearsal/bastion.sh reset
```

**c. Remove the test cluster:**

```bash
cd ..                                            # broker-replaceent-test/
cp -r results "results-backup-$(date +%Y%m%d)"   # optional: keep the full migration test results (REPORT.md uses results/charts)
scripts/down.sh --purge-results
```

This removes all broker containers and volumes, the clients, CoreDNS, MinIO (with its data) and the
network. It resets `coredns/rp.test.hosts`, so `b1`–`b5` point at `io-b1`–`io-b5` again, and
empties `results/`. Old node IDs and the old cluster ID in `results/` would otherwise clash with the
new cluster.

**d. Set up a fresh cluster.** Follow [section 1](#1-set-up-the-test-cluster), steps a–c
(`scripts/up.sh`, `scripts/configure-cluster.sh`, and optionally `scripts/seed-data.sh`), then `cd runbook`.

**e. Set the swap plan for the fresh cluster** in `rehearsal/runbook.env`. On a fresh cluster the aliases
point at `io-b1`–`io-b5`, and the replacements are `nv-n1`–`nv-n5`. The table is in
[section 3](#3-choose-a-swap-that-fits-the-harness-as-it-is-now). The default plan (`io-b2` → `nv-n2`)
already fits a fresh cluster.

**f. Start the rehearsal again** (sections 2 and 5):

```bash
rehearsal/bastion.sh start
rehearsal/bastion.sh shell
./step_00_setup_test_clients.sh
./step_01_preflight.sh
```

On the fresh cluster `step_03_fast_commission_check.sh` reports **DISABLED**, as in production, so
`step_04` enables it for real.

Run `step_00` **before** `step_01`. On a brand-new cluster with no topics, the partition balancer stays in
status `starting`, so `step_01` fails "balancer idle". The test topics created by `step_00` (or
`scripts/seed-data.sh`) let it complete its first pass and become `ready`. Production clusters always have
topics, so this only affects an empty test cluster.

If `step_01` shows `enable_rack_awareness = false` and `cloud_storage_enabled = false`, then
`scripts/configure-cluster.sh` (section 1b) was not run on the new cluster. Run it, then repeat `step_01`.

### How the rehearsal differs from production

| | Production | Rehearsal (`rehearsal/bastion.sh`) |
|---|---|---|
| Settings | `runbook.env` | `rehearsal/runbook.env`, mounted over `runbook.env` **inside the container only**; the real file is never changed |
| State and logs | `state/`, `logs/` | `rehearsal/state/`, `rehearsal/logs/` |
| Security | TLS + SCRAM | plaintext, no SASL (`RB_ALLOW_NO_AUTH=1`) |
| Route 53 | real `aws` CLI | `rehearsal/aws`: reads and edits `../coredns/rp.test.hosts`, which CoreDNS reloads every 2 s |
| Host actions | `systemctl`, EC2 console | `../scripts/broker.sh start / rm <instance>` |
| DNS TTL, soak | 60 s, 24 h | 5 s, 2 minutes |

A rehearsal can't test TLS and SCRAM, the real Route 53 API and IAM permissions, the interactive
EC2/host checklist items, or reachability from other VPCs. Check those with `--dry-run` steps
on the production bastion before the change window.

These scripts were rehearsed this way, with one full swap from `step_00` to `step_99`. A second rehearsal of
steps 10–14 started with the new broker's own DNS record missing, to check that `step_12` creates it, and
that `step_13`/`step_14` reach the broker by name and read and write through it.

---

## Part 2: Run the runbook in production

### Before you start

1. **Bastion host.** It needs network access to every broker on 9092 (Kafka) and 9644 (Admin API), plus these tools:
   - `rpk` v25.3.17
   - `bash` 5 or later
   - `jq`, `curl`, `dig`
   - `aws` CLI with permission to change the hosted zone(s)
2. **Edit `runbook.env`.** Fill in every `CHANGE-ME`: the DNS names, CA file, swap plan (old host, new host, rack, new IP), Route 53 zone IDs and important topics.
3. **Export the SCRAM credentials** in your shell. They are never written to disk:
   ```bash
   export RB_SASL_USER='admin'
   read -rs RB_SASL_PASSWORD && export RB_SASL_PASSWORD
   ```
4. **Run `./step_01_preflight.sh` once as a dry check** before the change window.

Production state goes to `state/` and logs to `logs/`. Every change made is listed in `state/changes.log`.
The logs record who ran each step and on which host, for the audit trail. They also contain cluster host
names, so keep `logs/` and `state/` out of anything you share; `.gitignore` excludes them.

### Order

| Step | Script | Runbook step | What it does |
|---|---|---|---|
| **Once, at the start** | | | |
| 00 | `step_00_setup_test_clients.sh` | n/a | Creates the test topics `rb.test.*`, preloads data, and starts the test producer, load producer and verifier consumer |
| 01 | `step_01_preflight.sh` | 1–5 | Checks version, racks, `empty_seed_starts_cluster`, seeds per AZ, DNS and TTL, placement, Tiered Storage coverage and backlog, and the licence |
| 02 | `step_02_save_baselines.sh` | 4, 6 | Saves the original recovery rate and concurrent-moves value, records baselines, and can optionally throttle moves |
| 03 | `step_03_fast_commission_check.sh` | 7 | Must report **DISABLED** before step 04 |
| 04 | `step_04_fast_commission_enable.sh` | 8 | Sets the fast commission targets (no restart) |
| 03 | `step_03_fast_commission_check.sh enabled` | 9 | Every Tiered Storage topic must be covered |
| **Per swap** (in `SWAP_PLAN` order; argument = DNS name or swap number) | | | |
| 10 | `step_10_go_no_go.sh <swap>` | 11 | Gate: previous soak passed, health, balancer idle, URP 0, recovery rate at original, seeds, clients |
| 11 | `step_11_raise_recovery_rate.sh <swap> [bytes]` | 12 | *Optional.* Raises the recovery rate and starts the auto-rollback watcher |
| 12 | `step_12_prepare_new_broker.sh <swap>` | 13–14 | Creates or verifies the new broker's **own DNS record**, writes its node settings and walks through the host checklist |
| | *(on the new host)* `systemctl start redpanda` | | |
| 13 | `step_13_confirm_join.sh <swap>` | 15–16 | Confirms a new node ID, rack, version and seed config, and that the new broker answers on 9092/9644 **by its own name** |
| 14 | `step_14_wait_settle.sh <swap>` | 17 | Waits until replicas have moved onto the new broker, then writes and reads a test record through a partition **led by** the new broker |
| 15 | `step_15_decommission.sh <swap> [--maintenance]` | 18 | Decommissions the old broker and shows progress |
| 16 | `step_16_verify.sh <swap>` | 19 | Checks the old ID is gone and placement holds, and collects fast commission evidence |
| 17 | `step_17_restore_recovery_rate.sh <swap>` | 20 | **Always run it**, even if step 11 was skipped (it then does nothing) |
| 18 | `step_18_dns_repoint.sh <swap>` | 21 | Route 53 UPSERT, waits for INSYNC and one TTL, checks `dig`, re-checks seeds |
| 19 | `step_19_retire_old_broker.sh <swap>` | 22 | Clean-up checklist for the old instance, and checks the old host is unreachable |
| 20 | `step_20_soak_check.sh <swap> [minutes]` | 23 | Soak watch. Records the soak as passed only after you confirm 24 h of clean dashboards |
| **Once, at the end** | | | |
| 30 | `step_30_final_check.sh` | 24 | Brokers are exactly the new hosts, placement holds, recovery rate is the original, seeds point at new brokers |
| 31 | `step_31_restore_temp_settings.sh` | 25–26 | Restores concurrent moves and removes any temporary leader pinning |
| 99 | `step_99_stop_test_clients.sh [--delete-topics]` | n/a | Stops the clients and verifies that every acked test message is present, in order |

Each per-swap step refuses to run until the previous step has recorded success in
`state/swap-N-*/`. For example, step 18 will not repoint DNS until step 16 has verified the swap
**and** the old broker is no longer a member.

### Manual alternative: the commands behind each step

Use this when a script can't be used, e.g. a missing tool on the bastion, or to double-check a result by hand.
These are the same checks and changes the scripts make, **without** their safety rails. Nothing refuses to
run out of order, there is no confirmation prompt (except `rpk topic alter-config`), and nothing is recorded
in `state/`. Read each output yourself against the "expect" notes.

`rpk` covers most of it. A few things are only available through the Admin API (`curl`), DNS (`dig`) or
Route 53 (`aws`):
- a broker's own settings: rack, `seed_servers`, advertised name;
- Tiered Storage upload status;
- the metrics behind the fast commission evidence.

#### Shell setup (once per terminal)

On the production bastion, in `runbook/`:

```bash
export RB_SASL_USER='admin'; read -rs RB_SASL_PASSWORD && export RB_SASL_PASSWORD
source runbook.env                    # sets RPK_BROKERS, RPK_ADMIN_HOSTS, TLS, SCRAM for rpk
for v in $(compgen -v RPK_); do [[ -z "${!v}" ]] && unset "$v"; done   # rpk misbehaves on EMPTY RPK_* values

# adm <path> [host:port]  - Admin API call with the same TLS + SCRAM credentials (default: first admin host)
adm() {
  local scheme=http; [[ "${RPK_ADMIN_TLS_ENABLED:-}" == true ]] && scheme=https
  curl -sS --fail ${RPK_ADMIN_TLS_CA:+--cacert "$RPK_ADMIN_TLS_CA"} ${RPK_USER:+-u "$RPK_USER:$RPK_PASS"} \
       "$scheme://${2:-${RPK_ADMIN_HOSTS%%,*}}$1"
}
```

In the Docker rehearsal, use the same lines inside `rehearsal/bastion.sh shell`, but skip the first one
(no SASL there).

In the examples, `OLD`/`NEW` stand for this swap's `OLD_HOST`/`NEW_HOST`, `ALIAS` for its `DNS_NAME`, and
`<old-id>`/`<new-id>` for their node IDs (from `rpk redpanda admin brokers list`).

#### step_00: test topics and clients

```bash
rpk topic create rb.test.sequencer -p 1 -r 3 -c retention.ms=172800000 -c redpanda.remote.write=true -c redpanda.remote.read=true
rpk topic create rb.test.load      -p 6 -r 3 -c retention.ms=172800000 -c redpanda.remote.write=true -c redpanda.remote.read=true
head -c $((200*1024*768)) /dev/urandom | base64 -w 1024 | rpk topic produce rb.test.load -z none -o ''   # ~200 MB preload

# sequencer producer (terminal A): every ACKNOWLEDGED record is written to acked.log
i=0; while :; do echo "$((i++)) ${EPOCHREALTIME/./}"; sleep 0.02; done \
  | rpk topic produce rb.test.sequencer --acks -1 --delivery-timeout 60s -o '%v\n' >> acked.log
# verifier consumer (terminal B): prints offset, sequence number, send time
rpk topic consume rb.test.sequencer -g rb-verifier -f '%o %v\n'
```

#### step_01: preflight (read-only)

```bash
rpk redpanda admin brokers list            # expect: 5 brokers, RACK set, MEMBERSHIP active, IS-ALIVE true, VERSION 25.3.17
rpk cluster health                         # expect: Healthy true, Nodes down [], Leaderless (0), Under-replicated (0)
rpk cluster maintenance status             # expect: ENABLED false for every node
rpk cluster partitions balancer-status     # expect: Status ready, Current Reassignment Count 0
adm /v1/node_config <broker-host>:9644 | jq '{rack, empty_seed_starts_cluster, seeds: [.seed_servers[].host.address]}'
                                           # per broker. expect: rack = AZ, false, the seed DNS names (identical on all)
dig +noall +answer b1.kafka.example.com A  # per seed name. expect: a live broker's IP, TTL <= 60
for p in enable_rack_awareness partition_autobalancing_mode cloud_storage_enabled write_caching_default \
         retention_local_strict raft_learner_recovery_rate partition_autobalancing_concurrent_moves; do
  printf '%-45s %s\n' "$p" "$(rpk cluster config get $p)"; done
                                           # expect: true, continuous, true, false (others informational)
rpk topic describe <topic> -c | grep -E 'redpanda.remote.(write|read)'    # expect: true / true on important topics
adm /v1/cloud_storage/status/<topic>/<partition> | jq '{local_log_last_offset, cloud_log_last_offset}'   # upload backlog
rpk cluster license info                   # expect: License status valid
```

One replica per rack, for every partition (the scripts' placement audit):

```bash
racks="$(adm /v1/brokers | jq 'map({key: (.node_id|tostring), value: .rack}) | from_entries')"
adm '/v1/cluster/partitions?with_internal=true' | jq -r --argjson r "$racks" \
  '.[] | [.replicas[].node_id | tostring | $r[.]] as $k | select(($k|unique|length) != 3 or ($k|length) != 3)
   | "\(.ns)/\(.topic)/\(.partition_id) racks=\($k)"'                     # expect: no output
```

#### step_02: save originals and baselines

```bash
rpk cluster config get raft_learner_recovery_rate                     # write it down (default 104857600)
rpk cluster config get partition_autobalancing_concurrent_moves       # write it down
rpk group describe <group>                                            # TOTAL-LAG per important consumer group
rpk cluster config set partition_autobalancing_concurrent_moves 10    # optional throttle
```

#### step_03 / step_04: fast commission

```bash
rpk cluster config get initial_retention_local_target_ms_default      # step_03: expect null (= disabled)
rpk cluster config get initial_retention_local_target_bytes_default   # step_03: expect null
rpk topic describe <topic> -c | grep -E 'initial.retention.local.target|redpanda.remote.write'   # per-topic override?

rpk cluster config set initial_retention_local_target_ms_default 3600000          # step_04 (no restart)
rpk cluster config set initial_retention_local_target_bytes_default 1073741824    # optional size cap
rpk topic alter-config <topic> --set initial.retention.local.target.ms=<ms>       # optional per-topic override
rpk cluster config status                                             # expect: NEEDS-RESTART false, INVALID [] everywhere
```

Undo: `rpk cluster config set initial_retention_local_target_ms_default null` (same for `_bytes_default`).

#### step_10: go / no-go (read-only)

```bash
rpk cluster health                                     # Healthy true, Under-replicated (0)
rpk cluster partitions balancer-status                 # ready, 0 reassignments
rpk cluster maintenance status                         # nobody in maintenance
rpk redpanda admin brokers list                        # OLD active+alive in the right rack; NEW not listed
rpk cluster config get raft_learner_recovery_rate      # = the original from step_02
rpk cluster config status                              # NEEDS-RESTART false everywhere
dig +short ALIAS; dig +short OLD                       # same IP: the alias still points at OLD
```

#### step_11 / step_17: recovery rate (optional raise, always restore)

```bash
rpk cluster config set raft_learner_recovery_rate 209715200     # step_11: raise to 200 MiB/s
rpk cluster config get raft_learner_recovery_rate               # check
rpk cluster config set raft_learner_recovery_rate 104857600     # step_17: restore the ORIGINAL from step_02
rpk cluster config status                                       # NEEDS-RESTART false everywhere
```

There is no automatic rollback when you do this by hand. Watch produce p99 yourself, and restore at once if
it stays above 2× the baseline.

#### step_12: the new broker's own DNS record

```bash
aws route53 list-resource-record-sets --hosted-zone-id <zone> --start-record-name NEW --max-items 1   # exists?
cat > /tmp/new.json <<'J'
{"Changes":[{"Action":"UPSERT","ResourceRecordSet":{"Name":"NEW.","Type":"A","TTL":300,"ResourceRecords":[{"Value":"<new-ip>"}]}}]}
J
aws route53 change-resource-record-sets --hosted-zone-id <zone> --change-batch file:///tmp/new.json
dig +short NEW                                         # expect: <new-ip> (also from a client network)
```

The `redpanda.yaml` settings for the new host are listed in the header of `step_12_prepare_new_broker.sh`.
Then start Redpanda on the new host.

#### step_13: confirm the join (read-only)

```bash
rpk redpanda admin brokers list                        # NEW listed: new ID, right RACK, active, alive, 25.3.17
adm /v1/node_config NEW:9644 | jq '{node_id, rack, empty_seed_starts_cluster, seeds: [.seed_servers[].host.address], advertised: .advertised_rpc_api.address}'
rpk cluster info -b                                    # NEW appears in the metadata clients receive
nc -zv NEW 9092; nc -zv NEW 9644                       # reachable by name (repeat from a client host)
```

#### step_14: settle, and a client round-trip through the new broker

```bash
rpk cluster partitions balancer-status                 # repeat until: ready, 0 reassignments, and NEW's
                                                       # PARTITION-COUNT close to its rack-mates'
rpk cluster partitions move-status                     # ongoing moves (empty when done)
rpk cluster health                                     # Under-replicated (0)

rpk cluster partitions transfer-leadership --partition rb.test.load/0:<new-id>     # test partition only
echo "roundtrip-$(date +%s)" | rpk topic produce rb.test.load -p 0 --acks -1 -o '%o\n'   # prints the offset
rpk topic consume rb.test.load -p 0 -o <offset> -n 1                                    # must print the same record
```

#### step_15: decommission the old broker

```bash
rpk cluster maintenance enable <old-id> --wait         # optional: drain leadership first
rpk redpanda admin brokers decommission <old-id>       # if the broker is OFFLINE, rpk refuses (and exits 0):
                                                       #   add the hidden --force flag
rpk redpanda admin brokers decommission-status <old-id> -d -H     # progress; repeat
rpk redpanda admin brokers recommission <old-id>       # ABORT, only while it is still running
rpk redpanda admin brokers list                        # done when <old-id> is no longer listed
                                                       #   (--include-decommissioned shows it again)
```

#### step_16: verify, and fast commission evidence (read-only)

```bash
rpk redpanda admin brokers list; rpk cluster health    # old ID gone, healthy, Under-replicated (0)
# local start offset of each replica: on NEW it should be HIGHER than on the other replicas
for h in NEW <other-broker-hosts>; do echo "== $h"
  adm /metrics $h:9644 | grep -E '^vectorized_cluster_partition_start_offset\{.*topic="<topic>"'; done
# local bytes per replica (on NEW much smaller for Tiered Storage topics)
adm /metrics NEW:9644 | grep -E '^vectorized_storage_log_partition_size\{.*topic="<topic>"'
```

`rpk cluster logdirs describe` also reports sizes, but it includes object-storage bytes for Tiered Storage
topics, so it is not a measure of local data.

#### step_18: repoint the DNS name

```bash
cat > /tmp/alias.json <<'J'
{"Changes":[{"Action":"UPSERT","ResourceRecordSet":{"Name":"ALIAS.","Type":"A","TTL":60,"ResourceRecords":[{"Value":"<new-ip>"}]}}]}
J
aws route53 change-resource-record-sets --hosted-zone-id <zone> --change-batch file:///tmp/alias.json   # prints ChangeInfo.Id
aws route53 wait resource-record-sets-changed --id <change-id>
sleep 60; dig +short ALIAS                             # expect: <new-ip>
adm /v1/node_config <any-broker>:9644 | jq '[.seed_servers[].host.address]'     # unchanged seed list
```

Only after `step_15` has completed (the old ID is gone from `rpk redpanda admin brokers list`).

#### step_19 / step_20: retire and soak

```bash
rpk redpanda admin brokers list                        # old ID not listed
nc -zv OLD 9092                                        # after terminating: must FAIL
watch -n 30 'rpk cluster health; rpk cluster partitions balancer-status'   # soak: stays healthy, 0 URP
```

#### step_30 / step_31 / step_99: finish

```bash
rpk redpanda admin brokers list                        # step_30: only the new hosts, all 25.3.17
rpk cluster config get raft_learner_recovery_rate      # = original
dig +short b1.kafka.example.com                        # seed names -> new brokers (b1, b3, b5)
rpk cluster config set partition_autobalancing_concurrent_moves <original>   # step_31
rpk cluster config get default_leaders_preference                            # step_31: none, unless pinned on purpose
rpk topic describe <topic> -c | grep redpanda.leaders.preference             # step_31: per-topic pinning
rpk topic alter-config <topic> --delete redpanda.leaders.preference          # step_31: remove a temporary pin
rpk group delete rb-verifier                                                 # step_99 (group first)
rpk topic delete rb.test.sequencer rb.test.load                              # step_99
```

### Two kinds of DNS names

| Name | Example | Used for | When it changes |
|---|---|---|---|
| **Repointable name** (`DNS_NAME` in `SWAP_PLAN`) | `b2.kafka.example.com` | `seed_servers` and client **bootstrap** only | `step_18` repoints it to the new broker, only after the old broker's decommission completed |
| **Broker's own name** (`NEW_HOST`) | `n2.kafka.internal.example.com` | `advertised_kafka_api` / `advertised_rpc_api`. Clients get it in the metadata and connect **directly** to it for every partition it leads | Must exist **before** the new broker starts (`step_12`) and stay as long as that broker lives |

Kafka clients use the bootstrap names only for the first metadata request. After that they connect
to every broker by its advertised name. So the new broker's own name must resolve, and port 9092 must be
reachable from **every client network** before the broker joins. If not, clients fail as soon as it leads a
partition they use.

`step_12` handles the broker's own name. If the record is missing and `CREATE_INSTANCE_RECORDS=true`, it creates the A
record (`NEW_IP` from `SWAP_PLAN`) in `INSTANCE_HOSTED_ZONE_ID`, after asking. If your provisioning creates it instead,
set `CREATE_INSTANCE_RECORDS=false` and the script only verifies it. Resolution and port access from client
networks (other VPCs, on-premises) can't be checked from the bastion, so the checklist asks you to confirm them.
The TLS certificate must include the broker's own name, and also the repointable name, because clients
bootstrap through it.

#### The new broker's listener and advertised names

| Setting in the new broker's `redpanda.yaml` | Value | What it does |
|---|---|---|
| `kafka_api` (listener) | `0.0.0.0:9092` (keep your TLS/SASL listener settings) | the address the broker **binds** to |
| `advertised_kafka_api` | **`NEW_HOST:9092`**, e.g. `n2.kafka.internal.example.com:9092` | the address **clients are told** to use; it's in the metadata |
| `advertised_rpc_api` | **`NEW_HOST:33145`** | the address **other brokers** use to reach it |

`step_12` writes these values, and `step_13` checks them on the running broker.

- **Advertise the broker's own DNS name, not an IP.** TLS certificates are issued for host names. If the instance's IP ever changes, only one DNS record needs updating instead of the broker config.
- **Never advertise the alias** (`b2…`). Aliases move (`step_18`), so a broker advertising one would send clients to whichever broker it points at that moment. Redpanda also matches `advertised_rpc_api` against `seed_servers` to decide whether a broker is a founding seed. `step_12` refuses a `NEW_HOST` that is an alias name.

#### Client bootstrap through the aliases

Clients list **only the alias names** in `bootstrap.servers`, and that list never changes during the migration:

```properties
bootstrap.servers=b1.kafka.example.com:9092,b2.kafka.example.com:9092,b3.kafka.example.com:9092,b4.kafka.example.com:9092,b5.kafka.example.com:9092
```

A client uses a bootstrap name only for its first request, which returns the cluster metadata. From then on
it connects to each partition leader by that broker's **own** advertised name. A client with the unchanged
list above, through one swap:

| Moment | `b2…` points at | The metadata lists | The client connects to |
|---|---|---|---|
| before the swap | old broker | the old brokers' own names | old brokers, by their names |
| after join and settle (`step_13`–`14`) | old broker | old brokers + **`NEW_HOST`** | also `NEW_HOST`, for partitions it leads |
| after decommission (`step_15`) | old broker (still running, no data) | **without** the old broker | only active brokers |
| after repoint (`step_18`) | **new broker** | same | same; a new bootstrap via `b2` now lands on the new broker |
| after retire (`step_19`) | new broker | same | same; nothing points at the removed instance |

So no client configuration and no `seed_servers` list has to change for the whole migration. Each alias ends
up pointing at the replacement broker.

- **Running clients aren't affected by the repoint.** They already have the broker list, and refresh the metadata through the brokers they're connected to. The alias only matters when a client starts, or restarts, and bootstraps from scratch.
- **Every client should list several bootstrap names** (`b1`–`b5`), so one alias in transition never blocks it.

**Why the repoint comes before removing the old broker.** After `step_15`, the old broker holds no data and
is no longer in the metadata. No client needs it, but its process still answers. Repointing in `step_18`
**before** stopping and terminating it in `step_19` means the alias never points at a dead address. Removing
first, then repointing, would leave clients and new brokers that bootstrap or seed through that alias with
connection failures in between. `step_19` refuses to run until `step_18` has succeeded.

#### Check before the migration: which names do clients use today?

The runbook is Path A: it assumes clients **already** bootstrap through the repointable aliases. Check two
things on the production bastion before the change window:

1. **What the brokers advertise today:**
   ```bash
   rpk cluster info -b        # HOST column = the names clients are given in the metadata
   ```
   - **The HOST column shows names different from the aliases** (e.g. `b2-old.kafka.internal…`): this matches the plan as written. Put that name in `SWAP_PLAN` as `OLD_HOST`.
   - **It shows the alias names themselves** (each old broker advertises `b2.kafka.example.com`, and so on): the procedure still works, because an alias is only repointed after its broker has left the cluster. `OLD_HOST` is then the alias name, and `step_10`'s check "DNS name points at OLD" compares the name with itself. Review the swap plan with that in mind.
2. **What the applications have in `bootstrap.servers`.** If any application lists the old brokers' **own** names instead of the aliases, those names will point at nothing once the old instances are terminated. That application would fail on its next restart. Change it to the aliases **before** the migration (runbook Phase 0, step 3).

### Test clients

`step_00` starts three background processes (`tools/test_clients.sh`). They use `rpk` only, and connect
through the same DNS names as your applications:

- **Sequencer producer.** Writes sequence numbers to `rb.test.sequencer` (1 partition) with `acks=all`. Every acknowledged record is logged.
- **Load producer.** Writes 1 KiB records to `rb.test.load` (6 partitions), so partition moves happen under write load.
- **Verifier consumer.** Checks that the sequence numbers have no gaps, and measures end-to-end latency (send to consume).

Every step prints a line like this one:

```
clients: producer up, consumer up, e2e p99 24 ms, gaps 0, producer errors 0
```

For details at any time, run `tools/test_clients.sh status`. The 2-day retention cleans up the test topics, or
`step_99 --delete-topics` removes them.

The SCRAM user needs create, describe, read and write on `rb.test.*`, and use of the group `rb-verifier`.

### Things learned in the rehearsal (Redpanda 25.3.17)

- **Decommissioning an offline broker needs the hidden `--force` flag.** `rpk` refuses but still exits 0. `step_15` detects this and asks before using `--force`.
- **A decommissioned broker still answers its Admin API** until it is stopped, but with a stale view (no controller). The scripts skip any entry point without an elected controller.
- **Right after a broker joins, the balancer can still be idle.** `step_14` therefore also waits until the new broker holds its share of replicas.
- **Every new broker needs its own DNS name before it joins.** Clients connect to each broker by its advertised name, not through the bootstrap aliases.
- **`rpk cluster logdirs describe` includes object-storage bytes for Tiered Storage topics.** `step_16` measures local data with the `vectorized_storage_log_partition_size` metric instead.
- **A brand-new cluster cannot be formed with alias-only seeds.** Joining an existing cluster through them works, which is what this runbook relies on.
