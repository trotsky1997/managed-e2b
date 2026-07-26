# managed-e2b (me2b)

A **state-machine + sqlite-tracked lifecycle manager for [E2B](https://e2b.dev) sandboxes** that prevents orphaned sandboxes.

```
queue/limiter → prewarm (build template if needed, dedup) → create → running (heartbeat) → graceful quit/timeout → clean
```

## Why

E2B sandboxes outlive the process that created them. A crash, an unhandled exception, or a forgotten `kill()` leaves a sandbox **running and billable** on your account. me2b tracks every sandbox in sqlite, reaps orphans via heartbeat + periodic scan, and kills-in-flight on exit — so leaks don't accumulate.

Built and hardened through **6 rounds of multi-agent adversarial review** (doc-informed, real-sandbox reproduction). See [Design](#design) for the failure modes it defends against.

## Install

```bash
pip install managed-e2b
# requires e2b + e2b-code-interpreter; set your key:
export E2B_API_KEY=e2b_...
```

## Usage

```python
from managed_e2b import SandboxLifecycle

lc = SandboxLifecycle(db_path="sandboxes.db", max_concurrent=4)

# image path: builds a template once (deduped), reuses after
with lc.acquire(image="python:3.11-slim", timeout=300) as h:
    out = h.sandbox.commands.run("print(2+2)")
    print(out.stdout)

# template path: use a pre-built template directly
with lc.acquire(template="base", timeout=300) as h:
    h.sandbox.commands.run("echo hello")

# periodic orphan reaper (call from a timer/loop)
lc.reap()            # conservative: only reaps own stale/cleaning sandboxes
lc.reap(purge_foreign=True)  # also kills foreign (untracked) sandboxes — careful

# startup reconcile: mark crashed-process orphans as dead
lc.reconcile()
```

`with`-exit auto-kills + confirms dead. `atexit` reaps anything still alive when the process exits.

## Configuration

| param | default | meaning |
|---|---|---|
| `max_concurrent` | 4 | max simultaneously-RUNNING sandboxes (eval concurrency) |
| `create_rate` | 1.0 | sandbox-create rate (token bucket); E2B Hobby=1/s, Pro=5/s |
| `max_build_concurrency` | 4 | concurrent template builds |
| `stale_timeout` | 600 | RUNNING with no heartbeat for this long = crashed → reaped (min 10) |
| `reaper_max_iter` | 5 | cap on reaper pagination loops |
| `e2b_api_url` | env | override endpoint (e.g. Volcengine-hosted E2B mirror) |
| `e2b_key` | env | override API key |

## Design

**State machine** (sqlite-tracked): `RUNNING → CLEANING → CLEANED`. Pre-warm/build states are transient (not persisted); failure raises.

**Three independent concurrency limits** — different resources, not one shared lock:
- `build` — template builds (don't consume sandbox quota)
- `create` — `Sandbox.create` rate (token bucket, matches E2B creation rate limit)
- `run` — simultaneously-held sandboxes (eval concurrency)

**Heartbeat**: `acquire` spawns a daemon thread refreshing `last_heartbeat`. `reap`/`shutdown` only kill RUNNING sandboxes whose heartbeat is stale (process crashed) — active long tasks keep fresh heartbeats and are never reaped.

**Iron rules** (from real-failure diagnosis):
1. `Sandbox.list()` is a *clue*, not truth — the reaper never loops "list until empty" (that caused 15,000 redundant kills in a prior tool). sqlite is the source of truth.
2. Each sandbox_id is killed **once**; on kill it's marked CLEANED, and seen-again ids are skipped.
3. Every reaper loop has a max-iteration cap — no spinning.

## Known limitations

- **Template cleanup**: E2B SDK 2.x exposes no `Template.list`/`Template.remove`. Templates built by me2b accumulate on your account; reuse is deduped by content hash, but distinct images produce distinct templates. Clean up via the E2B console.
- **Crash recovery** requires reusing the same `db_path` across runs; a fresh db_path can't see prior orphans (use `reconcile()` on startup against a stable db).
- **Long tasks**: `timeout=` is a *hard ceiling* — E2B auto-kills at expiry. Pass a value exceeding your worst-case task duration (default 1800s).

## Status

Pre-1.0. The lifecycle core is review-hardened; packaging is minimal. Tests require a live E2B account (`E2B_API_KEY`) and create real sandboxes.
