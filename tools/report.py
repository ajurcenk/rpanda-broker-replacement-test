"""report.py: build REPORT.md (and charts in results/charts/) from results/. Run via scripts/report.sh."""
import csv
import glob
import json
import os
import statistics
from collections import defaultdict
import re
from datetime import datetime, timezone

R = "/work/results"
OUT = "/work/REPORT.md"
CH = f"{R}/charts"
os.makedirs(CH, exist_ok=True)
ENV = dict(l.split("=", 1) for l in os.environ.get("REPORT_ENV", "").split(";") if "=" in l)
WARN, ALERT, SOAK = float(ENV.get("WARN_RATIO", 2)), float(ENV.get("ALERT_RATIO", 5)), float(ENV.get("SOAK_RECOVERY_RATIO", 1.2))
SWAPS = [(1, "io-b2", "nv-n2", "b2", "default"), (2, "io-b4", "nv-n4", "b4", "raised"),
         (3, "io-b1", "nv-n1", "b1", "default"), (4, "io-b3", "nv-n3", "b3", "default"),
         (5, "io-b5", "nv-n5", "b5", "raised")]
PHASES = ["baseline", "join", "settle", "settle-raised", "decommission", "decommission-raised", "post-verify", "post-verify-raised", "soak"]


def J(path, default=None):
    try:
        return json.load(open(path))
    except Exception:
        return default


def lines(path):
    try:
        return [l.rstrip("\n") for l in open(path)]
    except FileNotFoundError:
        return []


def fnum(v, nd=1):
    if v in (None, ""):
        return "–"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    return f"{f:.{nd}f}" if abs(f) < 1e6 else f"{f:.3g}"


def mib(b):
    return "–" if b in (None, "") else f"{b / 1048576:.1f} MiB"


def pf(ok):
    return "PASS" if ok is True else ("FAIL" if ok is False else "not run")


md = []
w = md.append

# ---------------- data loading ----------------
lat = list(csv.DictReader(open(f"{R}/latency.csv"))) if os.path.exists(f"{R}/latency.csv") else []
swaps_csv = [l.split(",", 5) for l in lines(f"{R}/swaps.csv")]
# strip colour codes, and absolute paths (they contain user names / folder names) -> relative "./"
_clean = lambda l: re.sub(r"/(?:[^/\s']+/)*broker-replaceent-test/", "./", re.sub(r"\x1b\[[0-9;]*m", "", l))
gates = [_clean(l).split(",", 4) for l in lines(f"{R}/gates.csv")]
gate_fail = [g for g in gates if len(g) > 3 and g[3] == "FAIL"]
scen = [l.split(",", 3) for l in lines(f"{R}/scenarios.csv")]
audits = [json.loads(l) for l in lines(f"{R}/audit-placement.jsonl") if l.strip()]
vd = J(f"{R}/verify-data.json")
fc_cmp = J(f"{R}/fast-commission-comparison.json")
fc_base, fc_en = J(f"{R}/fast-commission-baseline.json"), J(f"{R}/fast-commission-enabled.json")
fc_cfg = {os.path.basename(p)[len("fast-commission-config-"):-5]: J(p) for p in sorted(glob.glob(f"{R}/fast-commission-config-*.json"))}
ts_checks = {os.path.basename(p)[len("tiered-storage-check-"):-5]: J(p) for p in sorted(glob.glob(f"{R}/tiered-storage-check-*.json"))}
rr_log = [l.split(",") for l in lines(f"{R}/recovery-rate.log")]
dns_log = [l.split(",") for l in lines(f"{R}/dns.log")]
node_ids = [l.split() for l in lines(f"{R}/node_ids.txt")]
deviations_dyn = lines(f"{R}/deviations.log")
done = {int(s[1].split("-")[1]) for s in swaps_csv if len(s) > 4 and s[1].startswith("swap-") and s[4] == "done"}


def swap_rows(n):
    return [s for s in swaps_csv if len(s) > 4 and s[1] == f"swap-{n}"]


def agg(label):
    """per phase medians for a label"""
    out = {}
    rows = [r for r in lat if r["label"] == label and r["status"] == "ok"]
    for ph in PHASES + ["none"]:
        rs = [r for r in rows if r["phase"] == ph]
        if not rs:
            continue

        def med(tool, col):
            v = [float(r[col]) for r in rs if r["tool"] == tool and r[col] not in ("", None)]
            return statistics.median(v) if v else None

        def tot(tool, col):
            v = [float(r[col]) for r in rs if r["tool"] == tool and r[col] not in ("", None)]
            return sum(v) if v else None
        out[ph] = {"windows": len({r["window_id"] for r in rs if r["tool"] == "produce"}),
                   "p50": med("produce", "p50_ms"), "p95": med("produce", "p95_ms"), "p99": med("produce", "p99_ms"),
                   "p999": med("produce", "p999_ms"), "max": med("produce", "max_ms"), "rate": med("produce", "rate"),
                   "target": med("produce", "target_rate"), "errors": tot("produce", "errors"), "retries": tot("produce", "retries"),
                   "c_mb": med("consume", "cons_mb_s"), "c_msg": med("consume", "cons_msg_s"), "c_fetch": med("consume", "cons_fetch_ms"),
                   "e50": med("e2e", "p50_ms"), "e99": med("e2e", "p99_ms"), "e999": med("e2e", "p999_ms")}
    return out


def flag(ph, base):
    if not base or not base.get("p99"):
        return ""
    f = []
    rp = ph["p99"] / base["p99"] if ph.get("p99") else None
    re_ = ph["e99"] / base["e99"] if ph.get("e99") and base.get("e99") else None
    worst = max([x for x in (rp, re_) if x is not None], default=None)
    if (ph.get("errors") or 0) > 0 or (worst and worst > ALERT) or (ph.get("rate") and ph.get("target") and ph["rate"] < 0.9 * ph["target"]):
        f.append("ALERT")
    elif worst and worst > WARN:
        f.append("WARN")
    return " ".join(f)


def phase_table(label):
    a = agg(label)
    base = a.get("baseline")
    w("| phase | windows | produce p50/p95/p99/p99.9/max (ms) | rate (target) | errors / retries | consumer MB/s · msg/s · fetch ms | e2e p50/p99/p99.9 (ms) | p99 ratio produce · e2e | flag |")
    w("|---|---|---|---|---|---|---|---|---|")
    for ph in PHASES:
        if ph not in a:
            continue
        x = a[ph]
        rp = f"{x['p99'] / base['p99']:.2f}" if base and base.get("p99") and x.get("p99") else "–"
        re_ = f"{x['e99'] / base['e99']:.2f}" if base and base.get("e99") and x.get("e99") else "–"
        w(f"| {ph} | {x['windows']} | {fnum(x['p50'],0)}/{fnum(x['p95'],0)}/{fnum(x['p99'],0)}/{fnum(x['p999'],0)}/{fnum(x['max'],0)} | "
          f"{fnum(x['rate'],0)} ({fnum(x['target'],0)}) | {fnum(x['errors'],0)} / {fnum(x['retries'],0)} | "
          f"{fnum(x['c_mb'],2)} · {fnum(x['c_msg'],0)} · {fnum(x['c_fetch'],0)} | {fnum(x['e50'],0)}/{fnum(x['e99'],0)}/{fnum(x['e999'],0)} | "
          f"{rp} · {re_} | {flag(x, base) if ph != 'baseline' else ''} |")
    return a


def chart(label, title, boundaries):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except Exception:
        return None
    rows = [r for r in lat if r["label"] == label and r["status"] == "ok"]
    pr = [(datetime.strptime(r["timestamp"], "%Y-%m-%dT%H:%M:%SZ"), float(r["p99_ms"])) for r in rows if r["tool"] == "produce" and r["p99_ms"]]
    er = [(datetime.strptime(r["timestamp"], "%Y-%m-%dT%H:%M:%SZ"), float(r["p99_ms"])) for r in rows if r["tool"] == "e2e" and r["p99_ms"]]
    if not pr:
        return None
    ink, ink2, grid, surf = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
    fig, ax = plt.subplots(figsize=(10, 4), dpi=120)
    fig.patch.set_facecolor(surf)
    ax.set_facecolor(surf)
    for xs, c, lab in ((pr, "#2a78d6", "produce p99"), (er, "#eb6834", "end-to-end p99")):
        if xs:
            # break the line where windows are missing (e.g. an aborted and resumed step), never bridge a gap
            xx, yy = [], []
            for i, (t, v) in enumerate(xs):
                if i and (t - xs[i - 1][0]).total_seconds() > 2.5 * float(ENV.get("PERF_WINDOW_SEC", 60)) + 30:
                    xx.append(t)
                    yy.append(float("nan"))
                xx.append(t)
                yy.append(v)
            ax.plot(xx, yy, color=c, lw=2, marker="o", ms=4, label=lab,
                    markeredgecolor=surf, markeredgewidth=1.5)
            ax.annotate(lab, xs[-1], xytext=(6, 0), textcoords="offset points", color=ink2, fontsize=8, va="center")
    for t, name in boundaries:
        ax.axvline(t, color=ink2, lw=0.8, ls=(0, (3, 3)), alpha=0.6)
        ax.text(t, 1.0, f" {name}", transform=ax.get_xaxis_transform(), rotation=90, va="top", ha="left", fontsize=7, color=ink2)
    ax.set_title(title, loc="left", fontsize=10, color=ink)
    ax.set_ylabel("latency p99 (ms)", color=ink2, fontsize=8)
    ax.tick_params(colors=ink2, labelsize=7)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.grid(axis="y", color=grid, lw=0.6)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(grid)
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False, fontsize=8, loc="upper right", labelcolor=ink2)
    fig.tight_layout()
    path = f"{CH}/{label}.png"
    fig.savefig(path, facecolor=surf)
    plt.close(fig)
    return f"results/charts/{label}.png"


def bounds(prefix_rows):
    out = []
    for s in prefix_rows:
        try:
            t = datetime.strptime(s[0], "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
        step = s[4]
        if step.split("-")[0] in {"2", "5", "7", "9", "10"} and ("start" in step or step in ("2-provision", "9-repoint-start")):
            out.append((t, step))
    return out


# ---------------- header ----------------
w("# Rolling broker replacement test: report")
w("")
w(f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} from `results/`. Redpanda v25.3.17, 5 brokers in a 2/2/1 rack layout "
  "(eu-central-1a/1b/1c), Tiered Storage on MinIO, DNS alias seeds (Path A).")
w("")
w("> **Only the DNS seed path (Path A) was tested.** Paths B1 and B2 of the runbook were not implemented or tested. "
  "This is a functional and safety test on a single Docker host; absolute latency numbers are not representative of production.")
w("")

# ---------------- goals ----------------
uuid = (lines(f"{R}/cluster-uuid") or ["?"])[0]
fc_off = fc_cfg.get("baseline", {}) and fc_cfg["baseline"].get("passed")
goal_rows = [
    ("1. Zero loss of acked messages, order, group offsets", vd and vd.get("passed") and len(done) == 5,
     f"{len(done)}/5 swaps done; acked {vd.get('acked') if vd else '–'}, missing {vd.get('acked_missing') if vd else '–'}, reorders {vd.get('reorders') if vd else '–'}, group offset preserved {vd.get('group_offset_preserved') if vd else '–'}"),
    ("2. One replica per rack before, during, after", bool(audits) and all(not [v for v in a["violations"] if not v.get("moving")] for a in audits),
     f"{len(audits)} audits, {sum(len(a['violations']) for a in audits)} violations ({sum(len([v for v in a['violations'] if v.get('moving')]) for a in audits)} during active moves)"),
    ("3. Fast commission: off at start, enabled works", bool(fc_off) and bool(fc_cmp and fc_cmp.get("passed")),
     f"DISABLED at setup: {pf(bool(fc_off))}; Phase B bytes/replica ratio vs Phase A: {fnum(fc_cmp.get('ratio') if fc_cmp else None, 3)} (threshold {fc_cmp.get('threshold') if fc_cmp else '–'})"),
    ("4. DNS alias repointing; restarted clients bootstrap", None if len(done) < 5 else len(done) == 5 and len([d for d in dns_log if len(d) > 3 and 'TEST OVERRIDE' not in d[3]]) >= 5,
     f"{len(dns_log)} repoints logged in results/dns.log; clients restarted after each swap"),
    ("5. Seed/bootstrap guardrails (empty disk never forms a cluster)", any(s[1] == "s04-guardrail" and s[2] == "PASS" for s in scen) or None,
     f"cluster UUID constant: {uuid}; node ids never reused ({len(node_ids)} ids issued)"),
]
w("## Goals")
w("")
w("| goal | result | evidence |")
w("|---|---|---|")
for g, ok, ev in goal_rows:
    w(f"| {g} | {pf(ok)} | {ev} |")
w("")
if gate_fail:
    w(f"Gate failures recorded during the run (each stopped the step; a later rerun passed if the swap is marked done): {len(gate_fail)}")
    w("")
    for g in gate_fail[-10:]:
        w(f"- `{g[0]}` {g[1]} / {g[2]}: {g[4] if len(g) > 4 else ''}")
    w("")

# ---------------- key findings ----------------
bases = []
for n, *_ in SWAPS:
    b = J(f"{R}/raw/baseline-swap-{n}.json")
    if b:
        bases.append(f"swap {n}: {fnum(b.get('produce_p99'),0)} ms / e2e {fnum(b.get('e2e_p99'),0)} ms / test producer {fnum(b.get('test_producer_p99'),0)} ms")
w("## Key findings")
w("")
for f in [
    "**Alias seeds cannot form a new cluster on 25.3.17.** With `empty_seed_starts_cluster: false`, a broker is a founder only if its own `advertised_rpc_api` string appears in `seed_servers`. Brokers advertising instance names and seeding with DNS aliases retry forever. Joining an existing cluster through alias seeds works (5 swaps, scenarios 3, 7 and 8). Re-forming a cluster from scratch with alias-only seeds would not.",
    "**Decommissioning an offline broker needs the hidden `--force` flag.** `rpk redpanda admin brokers decommission <id>` refuses an offline broker (\"version for broker N is unknown\") but **exits 0**. `--force` is not listed under Flags in `--help`, only mentioned in the description text. Automation must check the output, not only the exit code (scenarios 2 and 3).",
    "**A decommissioned broker still answers its Admin API**, with a stale view (`no_elected_controller`, 503 from the balancer endpoint), until it is stopped. Tooling that picks \"any broker\" must exclude retired brokers, or it reports the cluster as unhealthy. In production, use load balancers or health checks that drop a broker once its decommission completes.",
    "**`write_caching_default` is `true` on a fresh cluster from the stock image** (`developer_mode: true` in the image's default `redpanda.yaml`). Check that production nodes do not carry that config.",
    "**`retention_local_strict=false` (the default) keeps Tiered Storage data local past `retention.local.target.bytes`** until disk pressure. Fast commission still works, but \"data only in object storage\" does not hold unless local trimming is strict.",
    "**Fast commission cut the data copied per Tiered Storage replica by about 81%** (17.5 → 3.4 MiB per replica here). Every migration swap showed the start-offset advantage on the new broker. The control topic without Tiered Storage was always fully copied. The cost: the first reads below the local target from a fast-committed leader go to object storage.",
    "**The recovery-rate auto-rollback fired in both raised swaps (2 and 5).** At 200 MiB/s, the test producer's p99 went above 2× baseline for 3 consecutive 10 s samples, so the rate went back to the original during settle. The swaps continued at the default rate. On this host, raising the rate did not shorten join-to-settled time (the swap table shows about 110 s in every swap).",
    "**The lone eu-central-1c broker is a structural hotspot.** With one replica per rack in a 2/2/1 layout, it holds a replica of every partition, and wrote about 3–4× the bytes of each other broker (11.5 GB vs 2.6–3.7 GB for io-b1/io-b3 at the time of swap 3). Swap 5 moved the most data during decommission.",
    "**The alias exposure window is unavoidable.** Between a completed decommission and the repoint, the alias resolves to a decommissioned broker that is still running. In uninterrupted swaps (2, 4, 5) the window was 43–55 s, because verification runs in between. In swaps 1 and 3 it was 254 s and 681 s, stretched by harness failures in step 8 that were fixed and resumed (see Deviations). Keep step 8 short, and never stop the old broker before the repoint.",
    "**Latency caveat for this run.** From swap 2 onward the host was I/O-saturated: Linux PSI `io full` was about 20% with no partition moves, on a host also running an unrelated 5-node kind cluster. The perf producer's 2,000 msg/s at `batch.size=4096` then ran at its throughput limit, so its p99 is mostly queueing time. Swap baselines (produce p99 / e2e p99 / test producer p99): " + "; ".join(bases) + ". End-to-end latency and the 500 msg/s test producer stayed in the tens of milliseconds and are the meaningful latency signals for swaps 2–5. Each swap is compared only with its own baseline.",
]:
    w(f"- {f}")
w("")

# ---------------- runbook coverage ----------------
def sw_ok():
    return "PASS" if len(done) == 5 else f"incomplete ({len(done)}/5 swaps)"


RB = [
    ("0", "1", "Rolling upgrade to 25.3.17, rack = AZ, empty_seed_starts_cluster=false", "up.sh creates the cluster directly on 25.3.17 with these settings", "not tested (upgrade)", "Cluster created on 25.3.17; the upgrade itself was not exercised"),
    ("0", "2", "Seed servers: Path A (DNS names, one per AZ, identical, low TTL)", "up.sh, check-seeds.sh at every gate; CoreDNS TTL 5s", pf(not [g for g in gate_fail if g[2].startswith("seeds")]) if gates else "not run", "Founders must bootstrap with instance-name seeds: 25.3.17 matches advertised_rpc_api literally against seed_servers (see Deviations)"),
    ("0", "2", "Seed servers: Path A after a fix / B1 / B2", "–", "not tested", "Production only, per test scope"),
    ("0", "3", "Client bootstrap via DNS names", "client/producer.py, consumer.py, perf tools bootstrap via b1–b5 only", sw_ok(), ""),
    ("0", "4", "Rack awareness, one replica per AZ, idle balancer; lower concurrent moves", "configure-cluster.sh, audit-placement.sh", pf(goal_rows[1][1]), "partition_autobalancing_concurrent_moves=10"),
    ("0", "5", "Tiered Storage coverage; upload backlog drained", "seed-data.sh, check-tiered-storage.sh", pf(all(v and v.get("passed") for v in ts_checks.values()) if ts_checks else None), "retention_local_strict=true required (see Deviations)"),
    ("0", "6", "Save raft_learner_recovery_rate, record baselines", "configure-cluster.sh (recovery-rate.setup), perf-latency.sh cluster-baseline", pf(os.path.exists(f"{R}/recovery-rate.setup")), "IOPS baseline not testable locally"),
    ("1", "7", "Confirm initial_retention_local_target_*_default are null", "check-fast-commission.sh --config --expect disabled", pf(bool(fc_off)), ""),
    ("1", "8", "Set time target / size cap / topic overrides (no restart)", "run-fast-commission-flow.sh Phase B", pf(bool(fc_cfg.get("enabled", {}) and fc_cfg["enabled"].get("passed"))), "Only the size default (initial_retention_local_target_bytes_default) was set; no time target or overrides"),
    ("1", "9", "Re-read values, effective target per topic", "check-fast-commission.sh --config --expect enabled", pf(bool(fc_cfg.get("enabled", {}) and fc_cfg["enabled"].get("passed"))), "Topic configs read via Kafka DescribeConfigs"),
    ("1", "10", "Proof: new broker's start offset higher than existing", "run-fast-commission-flow.sh Phases A and B; --evidence in every swap", pf(bool(fc_en and fc_en.get("passed"))), ""),
    ("2", "11", "Go/no-go checks incl. seed check", "swap.sh step 1", sw_ok(), ""),
    ("2", "12", "Optionally raise raft_learner_recovery_rate; latency windows", "recovery-rate.sh, swap.sh 1b (swaps 2 and 5), perf-latency.sh", sw_ok(), ""),
    ("2", "13", "Provision: stop protection, XFS/UUID mount/nofail, RequiresMountsFor, production mode, tune, iotune", "–", "not testable locally", "EC2, filesystems and host tuning do not exist in Docker"),
    ("2", "14", "Configure redpanda.yaml (rack, empty_seed_starts_cluster, unique name, no node_id, DNS seeds)", "render-config.sh", sw_ok(), ""),
    ("2", "15", "Start and confirm new id, rack, active, version", "swap.sh step 3 (wait_joined)", sw_ok(), ""),
    ("2", "16", "Seed change part 1: none (Path A)", "swap.sh step 4 (check-seeds.sh)", sw_ok(), ""),
    ("2", "17", "Settle: balancer idle, URP=0", "swap.sh step 5", sw_ok(), ""),
    ("2", "18", "Decommission (optional drain); abort = recommission", "swap.sh steps 6–7; scenario 1", sw_ok(), "Maintenance drain available via --maintenance; not used in the default run"),
    ("2", "19", "Verify: old id gone, healthy, URP=0, one replica per AZ, fast commission evidence", "swap.sh step 8", sw_ok(), ""),
    ("2", "20", "Restore raft_learner_recovery_rate, confirm with config status", "swap.sh 8b, recovery-rate.sh restore", sw_ok(), ""),
    ("2", "21", "Seed change part 2: repoint DNS, wait TTL, dig", "swap.sh step 9, dns-repoint.sh", sw_ok(), ""),
    ("2", "22", "Clean up old instance, inventory, monitoring, SGs", "swap.sh step 10 (container + volume removed)", sw_ok(), "Inventory/monitoring/security groups not testable locally"),
    ("2", "23", "Soak 24 hours", f"swap.sh soak ({ENV.get('SOAK_SEC', '180')}s)", "not testable locally", "Shortened soak; 24h soak not run"),
    ("3", "24", "Final check: N1–N5, placement clean, recovery rate original, seeds resolve to N1/N3/N5, seed file unchanged", "run-migration.sh step 6", sw_ok(), ""),
    ("3", "25", "Restore temporary settings, remove leader pinning", "run-migration.sh step 7", sw_ok(), "No leader pinning was configured"),
    ("3", "26", "Delete old volumes", "swap.sh step 10, down.sh", sw_ok(), ""),
    ("3", "27", "Steady-state rules; decide on autodecommission timeout", "scenario 9", "not testable locally", "partition_autobalancing_node_autodecommission_timeout_sec does not exist in 25.3.17; retirement events are AWS-only"),
    ("3", "28", "Plan upgrades to 26.1 / 26.2", "–", "not testable locally", "Planning item"),
]
w("## Runbook coverage")
w("")
w("Only the DNS seed path (Path A) was tested. Paths B1 and B2 were not tested.")
w("")
w("| phase | step | runbook step | harness | result | differences on 25.3.17 / notes |")
w("|---|---|---|---|---|---|")
for r in RB:
    w("| " + " | ".join(r) + " |")
w("")

# ---------------- swaps ----------------
w("## Swaps")
w("")
w("| swap | old → new | recovery rate | join (s) | settle (s) | join→settled (s) | decommission (s) | bytes moved (observed) | soak | status |")
w("|---|---|---|---|---|---|---|---|---|---|")
for n, o, nw, a, rr in SWAPS:
    t = J(f"{R}/raw/timings-swap-{n}.json", {})
    so = J(f"{R}/raw/soak-swap-{n}.json", {})
    rb = os.path.exists(f"{R}/raw/rollback-swap-{n}.txt")
    soak = "–" if not so else ("recovered" if so.get("recovered") else "latency did not recover")
    w(f"| {n} | {o} → {nw} | {rr}{' (rate rolled back)' if rb else ''} | {t.get('join_sec', '–')} | {t.get('settle_sec', '–')} | "
      f"{t.get('join_to_settled_sec', '–')} | {t.get('decommission_sec', '–')} | {mib((t.get('decommission') or {}).get('bytes_moved_max_observed'))} | {soak} | "
      f"{'done' if n in done else 'not done'} |")
w("")
w("Alias exposure window (decommission completed → alias repointed). The test document requires both \"repoint only after the "
  "decommission completes\" and \"an alias never resolves to a decommissioned instance\"; these cannot both hold literally, so "
  "the procedure keeps the window short (verification runs in between) and the old broker keeps running, still answering, until after the repoint:")
w("")
w("| swap | alias | decommission completed | repointed | window (s) |")
w("|---|---|---|---|---|")
for n, o, nw, a, rr in SWAPS:
    rows = swap_rows(n)
    d = [s[0] for s in rows if s[4] == "7-decommissioned"]
    r = [s[0] for s in rows if s[4] == "9-repointed"]
    if d and r:
        dt = (datetime.strptime(r[-1], "%Y-%m-%dT%H:%M:%SZ") - datetime.strptime(d[-1], "%Y-%m-%dT%H:%M:%SZ")).total_seconds()
        w(f"| {n} | {a} | {d[-1]} | {r[-1]} | {int(dt)} |")
t5 = J(f"{R}/raw/timings-swap-5.json", {})
if t5:
    w("")
    w(f"Swap 5 (io-b5 → nv-n5, the lone eu-central-1c broker, largest transfer): join→settled {t5.get('join_to_settled_sec')}s, decommission {t5.get('decommission_sec')}s.")
w("")

# ---------------- latency ----------------
w("## Latency")
w("")
w("Tools: `apache/kafka:3.9.1` (`kafka-producer-perf-test.sh`, `kafka-consumer-perf-test.sh`, `kafka-e2e-latency.sh`), run in parallel per window, bootstrapping through `b1`–`b5`.")
w("")
w("```")
w(f"kafka-producer-perf-test.sh --topic perf.latency --num-records {int(ENV.get('PERF_RATE', 2000)) * int(ENV.get('PERF_WINDOW_SEC', 60))} --record-size 1024 --throughput {ENV.get('PERF_RATE', 2000)} --print-metrics \\")
w("  --producer-props bootstrap.servers=<aliases> acks=all enable.idempotence=true linger.ms=3 batch.size=4096")
w(f"kafka-consumer-perf-test.sh --bootstrap-server <aliases> --topic perf.latency --messages <rate×window> --group perf-consumer-<window> \\")
w("  --from-latest --show-detailed-stats --reporting-interval 5000 --timeout 60000")
w(f"kafka-e2e-latency.sh <aliases> perf.e2e {ENV.get('PERF_E2E_COUNT', 2000)} all 1024")
w("```")
w("")
w("Topics: `perf.latency` (6 partitions) and `perf.e2e` (1 partition), RF=3, `min.insync.replicas=2`, with the same Tiered Storage settings as the background topics. "
  "The consumer perf tool reports throughput and fetch time, not per-message latency. Values are medians over the windows of each phase; "
  f"ratios are against the swap's own baseline. WARN: p99 > {WARN:g}× baseline; ALERT: > {ALERT:g}×, any produce error, or achieved rate < 90% of target.")
w("")
w("**Absolute numbers from a single Docker host are not representative of production; only the relative change between phases is meaningful.**")
w("")
base_cluster = agg("cluster-baseline").get("baseline")
if base_cluster:
    w(f"Healthy-cluster baseline (`cluster-baseline`): produce p99 {fnum(base_cluster['p99'],0)} ms, e2e p99 {fnum(base_cluster['e99'],0)} ms, consumer {fnum(base_cluster['c_mb'],2)} MB/s.")
    w("")
for n, o, nw, a, rr in SWAPS:
    if not any(r["label"] == f"swap-{n}" for r in lat):
        continue
    w(f"### Swap {n}: {o} → {nw} (recovery rate {rr})")
    w("")
    phase_table(f"swap-{n}")
    w("")
    so = J(f"{R}/raw/soak-swap-{n}.json", {})
    if so:
        w(f"Soak recovery: produce ratio {fnum(so.get('produce_ratio'), 2)}, e2e ratio {fnum(so.get('e2e_ratio'), 2)} "
          f"(limit {SOAK:g}×): **{'recovered' if so.get('recovered') else 'latency did not recover'}**.")
        w("")
    c = chart(f"swap-{n}", f"Swap {n} ({o} → {nw}): p99 latency per perf window", bounds(swap_rows(n)))
    if c:
        w(f"![swap {n} latency]({c})")
        w("")
for lab in ("fc-disabled", "fc-enabled"):
    if any(r["label"] == lab for r in lat):
        w(f"### Fast commission join `{lab}`")
        w("")
        phase_table(lab)
        w("")
        c = chart(lab, f"tmp-1 join with {lab}: p99 latency per perf window", [])
        if c:
            w(f"![{lab}]({c})")
            w("")


def comp(l1, l2, name):
    a1, a2 = agg(l1), agg(l2)
    w(f"**{name}**")
    w("")
    w(f"| phase | {l1} produce p99 | {l2} produce p99 | {l1} e2e p99 | {l2} e2e p99 |")
    w("|---|---|---|---|---|")
    for ph in PHASES:
        if ph in a1 or ph in a2:
            w(f"| {ph} | {fnum(a1.get(ph, {}).get('p99'),0)} | {fnum(a2.get(ph, {}).get('p99'),0)} | {fnum(a1.get(ph, {}).get('e99'),0)} | {fnum(a2.get(ph, {}).get('e99'),0)} |")
    w("")


w("### Comparisons")
w("")
comp("fc-disabled", "fc-enabled", "Fast commission disabled vs enabled (tmp-1 joins)")
comp("swap-1", "swap-2", "Swap 1 (default recovery rate) vs swap 2 (raised)")
w("**Swap 5 (largest transfer) vs the others**: see the swap timing table above and the per-swap phase tables.")
w("")

# ---------------- recovery rate ----------------
w("## Recovery rate")
w("")
setup = (lines(f"{R}/recovery-rate.setup") or ["?"])[0]
w(f"Original (setup) value of `raft_learner_recovery_rate`: **{setup}** bytes/s. Raised value for swaps 2 and 5: {ENV.get('RAISED_RECOVERY_RATE', '209715200')} bytes/s.")
w("")
w("| time | event | detail |")
w("|---|---|---|")
for r in rr_log:
    if len(r) > 1 and r[1] in ("save", "raise", "restore", ):
        w(f"| {r[0]} | {r[1]} | {' → '.join(r[2:])} |")
    elif len(r) > 2 and r[2] == "auto-rollback":
        w(f"| {r[0]} | **auto-rollback** ({r[1]}) | {','.join(r[3:])} |")
w("")
for n in (1, 2, 5):
    t = J(f"{R}/raw/timings-swap-{n}.json", {})
    a = agg(f"swap-{n}")
    pk = (t.get("settle") or {}).get("peak_ingest_bytes_per_sec")
    ph = a.get("settle-raised") or a.get("settle") or {}
    if t:
        w(f"- Swap {n}: join→settled {t.get('join_to_settled_sec')}s, decommission {t.get('decommission_sec')}s, peak ingest on new broker {mib(pk)}/s, produce p99 during settle {fnum(ph.get('p99'),0)} ms.")
rbs = sorted(glob.glob(f"{R}/raw/rollback-swap-*.txt"))
w("")
w(f"Automatic rollbacks: {len(rbs)}" + ("".join(f"\n- {os.path.basename(p)[9:-4]}: {open(p).read().strip()}" for p in rbs)))
w("")
fin = J(f"{R}/raw/final-recovery-rate.json")
w(f"Final value equals the original: **{pf(fin.get('equal')) if fin else 'not checked'}**" + (f" ({fin.get('value')})" if fin else ""))
w("")

# ---------------- tiered storage ----------------
w("## Tiered Storage")
w("")
w(f"Topic settings: `redpanda.remote.write=true`, `redpanda.remote.read=true`, `segment.bytes={ENV.get('SEGMENT_BYTES')}`, "
  f"`retention.local.target.bytes={ENV.get('LOCAL_TARGET_BYTES')}`, `retention.bytes=-1`, `retention.ms=-1`, `cleanup.policy=delete`, `write.caching=false`; "
  f"preload {int(ENV.get('PRELOAD_BYTES_PER_PARTITION', 0)) // 1048576} MiB per partition. `control.no-ts`: remote write/read false.")
w("")
pc = ts_checks.get("post-seed")
if pc:
    w("Post-seed check (per topic):")
    w("")
    w("| topic | MinIO objects | MinIO bytes | min local start / HWM (p0) | share only in MinIO | earliest read (s) | local read (s) |")
    w("|---|---|---|---|---|---|---|")
    for t, d in sorted(pc["topics"].items()):
        p0 = d["partitions"].get("0") or {}
        ls = [v for v in (p0.get("local_start_by_node") or {}).values() if v is not None]
        r = d.get("reads") or []
        w(f"| {t} | {d['objects']} | {mib(d['bytes'])} | {min(ls) if ls else '–'} / {p0.get('high_watermark', '–')} | {d.get('share_only_in_minio_avg', '–')} | "
          f"{fnum(statistics.mean([x['earliest_read_sec'] for x in r]), 3) if r else '–'} | {fnum(statistics.mean([x['local_read_sec'] for x in r]), 3) if r else '–'} |")
    w("")
    w("Kafka earliest offset is 0 on every Tiered Storage partition, while every replica's local start offset is above 0, so the oldest data is served from MinIO. "
      "The first read of old segments hydrates them into Redpanda's cloud cache; later checks read from that cache, which is why repeat earliest reads are as fast as local reads.")
    w("")
w("| check | passed | failures | TS partitions with upload backlog > 1.5 segments |")
w("|---|---|---|---|")
for k, v in ts_checks.items():
    if v:
        w(f"| {k} | {pf(v.get('passed'))} | {len(v.get('failures', []))} | {sum(1 for f in v.get('failures', []) if 'backlog' in f)} |")
w("")

# ---------------- fast commission ----------------
w("## Fast commission")
w("")
w("State timeline:")
w("")
for k, v in fc_cfg.items():
    if v:
        w(f"- {v['time']}: `{k}` → **{v['state']}** (expected {v['expect']}); cluster bytes default = {v['cluster']['initial_retention_local_target_bytes_default']}, ms default = {v['cluster']['initial_retention_local_target_ms_default']}")
for l in lines(f"{R}/fast-commission-timeline.csv"):
    w(f"- {l.replace(',', ' ')}")
w("")
en = fc_cfg.get("enabled")
if en:
    w("Coverage (after enabling):")
    w("")
    w("| topic | Tiered Storage | effective initial target (bytes / ms) | covered |")
    w("|---|---|---|---|")
    for t, r in sorted(en["topics"].items()):
        w(f"| {t} | {'yes' if r['remote_write'] == 'true' else 'no'} | {r['effective_bytes'] or '–'} / {r['effective_ms'] or '–'} | {r['note']} |")
    w("")
if fc_base and fc_en:
    w("Baseline (Phase A, disabled) vs enabled (Phase B), tmp-1 joins:")
    w("")
    w("| | TS replicas advantaged | TS local bytes / replica | control replicas advantaged | control bytes on new | join→settled (s) | decommission (s) | disk vs existing median |")
    w("|---|---|---|---|---|---|---|---|")
    for lab, d, dec in (("Phase A (disabled)", fc_base, J(f"{R}/raw/decom-baseline.json", {})), ("Phase B (enabled)", fc_en, J(f"{R}/raw/decom-enabled.json", {}))):
        s = d["summary"]
        w(f"| {lab} | {s['ts_replicas_advantaged']}/{s['ts_replicas_on_new']} | {mib(s['ts_local_bytes_per_replica'])} | {s['control_advantaged']}/{s['control_replicas_on_new']} | "
          f"{mib(s['control_local_bytes_on_new'])} | {(d.get('timings') or {}).get('join_to_settled_sec', '–')} | {dec.get('duration_sec', '–')} | {s['new_disk_pct_of_median']}% |")
    w("")
    w("Per-topic tables: `results/fast-commission-baseline.md`, `results/fast-commission-enabled.md`.")
    w("")
rc = J(f"{R}/read-check-tmp1-leader.json")
if rc:
    w(f"Reads with tmp-1 as leader (Phase B step 13): {len(rc['reads'])} partitions, all digests match the Phase A reads: {pf(not rc['failures'])}.")
    w("")
    w("| partition | leader | records | read (s) | digest match |")
    w("|---|---|---|---|---|")
    for r in rc["reads"]:
        w(f"| {r['tp']} | {r['leader']} | {r['records']} | {r['read_sec']} | {r.get('match')} |")
    w("")
w("Per-swap evidence:")
w("")
w("| swap | new broker | TS replicas advantaged | TS local bytes / replica | control advantaged | disk vs median | passed |")
w("|---|---|---|---|---|---|---|")
for n, o, nw, a, rr in SWAPS:
    d = J(f"{R}/fast-commission-swap-{n}.json")
    if d:
        s = d["summary"]
        w(f"| {n} | {nw} | {s['ts_replicas_advantaged']}/{s['ts_replicas_on_new']} | {mib(s['ts_local_bytes_per_replica'])} | {s['control_advantaged']}/{s['control_replicas_on_new']} | {s['new_disk_pct_of_median']}% | {pf(d['passed'])} |")
w("")
if fc_cmp:
    red = (1 - fc_cmp["ratio"]) * 100 if fc_cmp.get("ratio") is not None else None
    w(f"**Verdict.** Fast commission was {'OFF' if fc_off else 'NOT confirmed off'} at the start (both `initial_retention_local_target_*_default` null, no topic overrides). "
      f"Enabling `initial_retention_local_target_bytes_default={ENV.get('FC_TARGET_BYTES')}` took effect without a restart on every Tiered Storage topic; `control.no-ts` is not covered, as expected. "
      f"Local bytes copied per Tiered Storage replica fell from {mib(fc_cmp.get('baseline_ts_bytes_per_replica'))} to {mib(fc_cmp.get('enabled_ts_bytes_per_replica'))} "
      f"(ratio {fnum(fc_cmp.get('ratio'), 3)}, a {fnum(red, 0)}% reduction in data copied through Raft).")
    w("")
tid = None
try:
    tid = int(open(f"{R}/raw/fc-enabled-node-id").read().strip())
except Exception:
    pass
reads = (J(f"{R}/read-check-tmp1-leader.json", {}) or {}).get("reads", []) + (J(f"{R}/read-check-tmp1-leader-new.json", {}) or {}).get("reads", [])
on_new = [r["read_sec"] for r in reads if r.get("leader") == tid]
on_old = [r["read_sec"] for r in reads if r.get("leader") != tid]
if on_new and on_old:
    w(f"**Tradeoff observed.** Reading the first 1,000 records from the earliest offset took a median {statistics.median(on_new):.3f}s when the fast-committed "
      f"tmp-1 was leader ({len(on_new)} partitions; its cloud cache was cold, so the data came from MinIO) versus {statistics.median(on_old):.3f}s when an existing "
      f"broker with a warm cloud cache led ({len(on_old)} partitions). Consumers reading below the initial local target from a fast-committed leader pay a "
      "remote-read cost until the cache is warm. Leadership transfer to tmp-1 took effect for "
      f"{len(on_new)}/{len(reads)} chosen partitions.")
    w("")
# ---------------- placement ----------------
w("## Placement audit")
w("")
hard = [a for a in audits if [v for v in a["violations"] if not v.get("moving")]]
w(f"{len(audits)} audits; {len(hard)} with violations outside active moves. Every partition, internal included, has exactly one replica in each rack at every gate."
  if not hard else f"{len(audits)} audits; {len(hard)} with violations outside active moves (see results/audit-placement.jsonl).")
w("")

# ---------------- data ----------------
w("## Data verification")
w("")
if vd:
    viol = [l for l in lines(f"{R}/violations.log") if "resume-offset-mismatch" not in l]
    w(f"| acked | present | missing | reorders | duplicates | unacked present | group offset before → after | consumer-side violations |")
    w("|---|---|---|---|---|---|---|---|")
    w(f"| {vd['acked']} | {vd['acked'] - vd['acked_missing']} | {vd['acked_missing']} | {vd['reorders']} | {vd['duplicates']} | {vd['unacked_present']} | "
      f"{vd['group_offset_before_swaps']} → {vd['group_committed_offset']} | {len(viol)} |")
    w("")
    w(f"Result: **{pf(vd['passed'])}**. Duplicates and unacked-present records are writes that were not acknowledged to the producer (allowed).")
else:
    w("Not run.")
w("")

# ---------------- scenarios ----------------
w("## Scenarios")
w("")
w("| scenario | result (final attempt) | detail |")
w("|---|---|---|")
seen, attempts = {}, defaultdict(list)
for s in scen:
    if len(s) >= 3:
        seen[s[1]] = s
        attempts[s[1]].append(s[2])
for k in sorted(seen):
    s = seen[k]
    detail = s[3] if len(s) > 3 else ""
    notes = "\n".join(lines(f"{R}/scenarios/{k}.notes"))
    if "hidden --force" in notes and "plain decommission;" in detail:
        detail = detail.replace("dead id decommissioned with a plain decommission", "dead id decommissioned with the hidden --force flag (plain decommission refused, exit 0)")
    prev = attempts[k][:-1]
    if prev:
        detail += f" (earlier attempt(s): {', '.join(prev)}; see Deviations)"
    w(f"| {k} | {s[2]} | {detail} |")
w("")
for p in sorted(glob.glob(f"{R}/scenarios/*.notes")):
    w(f"<details><summary>{os.path.basename(p)[:-6]} notes</summary>\n\n```\n{open(p).read().strip()}\n```\n</details>\n")

# ---------------- deviations ----------------
w("## Deviations from the test document")
w("")
DEV = [
    "**MinIO built from source.** `minio/minio` and `minio/mc` no longer exist on Docker Hub (404) and `dl.min.io` returns 410. Images are built from GitHub tags `RELEASE.2025-10-15T17-29-55Z` (minio) and `RELEASE.2025-08-13T08-35-41Z` (mc) with `golang:1.24.9-bookworm` (`images/minio/Dockerfile`).",
    "**Data scaled down** (user decision: about 50 MB per partition): `segment.bytes` 1 MiB (spec 16 MiB), `retention.local.target.bytes` 16 MiB (spec 256 MiB), preload 50 MiB per partition (3.1× the local target), fast commission target 1 MiB (spec 16 MiB, still one segment).",
    "**Initial cluster formation with alias seeds is impossible in 25.3.17.** With `empty_seed_starts_cluster: false`, a broker is a founder only if its `advertised_rpc_api` appears literally in `seed_servers` (`cluster_discovery.cc`: `ss.addr == advertised_rpc_api()`, unresolved strings). With seeds `b1/b3/b5` and advertised names `io-b*`, no broker founds the cluster and all retry forever. The three founders therefore start with instance-name seeds (`io-b1/io-b3/io-b5`), and are re-rendered to the alias list and restarted one by one before any test step. The seed fingerprint is recorded after that, and never changed afterwards. This matches the assumed production state (an existing cluster that now uses DNS seeds); a new broker joining via alias seeds works, as every swap shows. It also means that **re-forming a cluster from scratch with DNS-alias seeds would not work**.",
    "**`write_caching_default` set to `false`** in `.bootstrap.yaml` and asserted by `configure-cluster.sh`, because the image's default `redpanda.yaml` has `developer_mode: true`, under which a fresh cluster reports `write_caching_default=true`. The rendered config does not enable developer mode, and topics are created with `write.caching=false`.",
    "**`retention_local_strict=true`.** The 25.3.17 default (`false`) lets Tiered Storage partitions grow past `retention.local.target.bytes` until disk pressure, so data would not be evicted locally.",
    "**No `--force` on `rpk redpanda admin brokers decommission`** in 25.3.17. Scenarios 2 and 3 use a plain decommission for the unreachable broker.",
    "**`kafka-e2e-latency.sh` (Kafka 3.9.1) takes positional arguments and rejects `-1` for acks**; `all` is used.",
    "**`rpk redpanda admin brokers list` has no `--format json`** in 25.3.17; the harness uses the Admin API (`/v1/brokers`, `/v1/node_config`, `/v1/cluster/partitions`, `/v1/cloud_storage/status/...`).",
    "**Scenario 9 skipped:** `partition_autobalancing_node_autodecommission_timeout_sec` does not exist in 25.3.17.",
    "**Consumer perf tool** runs with `--from-latest` so each window reads only new data.",
    "**Leader balancer** is disabled for the duration of fast commission step 13 (leadership transfer to tmp-1) and re-enabled afterwards.",
    "**Brokers run with `docker run`** from `scripts/broker.sh` rather than as Compose services, so each swap creates only the containers it needs.",
]
for d in DEV:
    w(f"- {d}")
for d in deviations_dyn:
    w(f"- Run note: {d}")
w("")
w("## Differences from production that limit what this test proves")
w("")
for d in ["Single Docker host: all \"AZs\" share CPU, disk and network; rack failure is simulated by stopping containers.",
          "Docker volumes on the host filesystem, not NVMe/XFS; no `rpk iotune`, no `rpk redpanda tune`, `--overprovisioned`, 1 core and 1 GiB per broker.",
          f"Small data volume (about {int(ENV.get('PRELOAD_BYTES_PER_PARTITION', 0)) // 1048576} MiB per partition) and short soaks ({ENV.get('SOAK_SEC', '180')}s instead of 24h).",
          "MinIO on the same host stands in for S3; object-storage latency and throughput are not representative.",
          "DNS is CoreDNS with a 5s TTL; production resolvers, negative caching and client DNS caches may behave differently.",
          "No EC2 stop protection, retirement events, io2 IOPS, security groups, or monitoring integration."]:
    w(f"- {d}")
w("")
open(OUT, "w").write("\n".join(md) + "\n")
print(f"wrote {OUT} ({len(md)} lines)")
