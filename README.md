# managed-e2b (me2b)

A **state-machine + sqlite-tracked lifecycle manager for [E2B](https://e2b.dev) sandboxes** that prevents orphaned sandboxes.

```
queue/limiter → prewarm (build template if needed, dedup) → create → running (heartbeat) → graceful quit/timeout → clean
```

## Why

E2B sandboxes outlive the process that created them. A crash, an unhandled exception, or a forgotten `kill()` leaves a sandbox **running and billable** on your account. me2b tracks every sandbox in sqlite, reaps orphans via heartbeat + periodic scan, and kills-in-flight on exit — so leaks don't accumulate.

Built and hardened through **7 rounds of multi-agent adversarial review** (doc-informed, real-sandbox reproduction, RPC counting). See [Design](#design) for the failure modes it defends against.

## Install

Not on PyPI — install from GitHub:

```bash
pip install git+https://github.com/trotsky1997/managed-e2b.git
# requires e2b + e2b-code-interpreter; set your key:
export E2B_API_KEY=e2b_...
```

Or clone and develop:

```bash
git clone https://github.com/trotsky1997/managed-e2b.git
cd managed-e2b
pip install -e .
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

Invalid input is rejected **before any E2B RPC** — `acquire` raises `ValidationError` on bad params (timeout out of bounds, multi-source, non-str metadata):

```python
lc.acquire(template="base", timeout=0)        # ValidationError: timeout must be > 0
lc.acquire(image="a", template="b")            # ValidationError: three-way mutual exclusion
lc.acquire(template="base", metadata={"k": 1})  # ValidationError: metadata must be dict[str,str]
```

`with`-exit auto-kills + confirms dead. `atexit` reaps anything still alive when the process exits.

## Use with evalscope

`managed_e2b_evalscope.py` is a drop-in backend that runs evalscope's code
execution (humaneval/mbpp/…) in E2B sandboxes managed by me2b, instead of the
local docker sandbox.

```python
import os
os.environ["E2B_API_KEY"] = "e2b_..."

import managed_e2b_evalscope as m2b_ev
m2b_ev.install_e2b_backend(template="base", e2b_key=os.environ["E2B_API_KEY"])

from evalscope import run_task, TaskConfig
cfg = TaskConfig(
    model="your-model", api_url="https://api.example.com/v1",
    api_key="...", eval_type="openai_api",
    datasets=["humaneval"], limit=10,
    sandbox={"enabled": True, "engine": "docker"},  # engine value ignored once E2B takes over
)
run_task(task_cfg=cfg)  # generated code runs in E2B, results match the local-docker path
```

Verified: glm-5.2 on 10 HumanEval problems scores pass@1 = 1.0 via the E2B
backend, identical to the local-docker path.

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

**Pydantic v2 model layer** (`managed_e2b.models`) — typed, validated, no silent bad data:
- `SandboxConfig` — `__init__` params with bounds (`stale_timeout >= 10`, `create_rate > 0`, `extra='forbid'`).
- `SandboxRecord` — a sandbox row: field constraints, timestamp-consistency validator, `transition_to()` centralizing state side-effects, sqlite round-trip.
- `AcquireRequest` — `acquire()` params: image/dockerfile/template mutual exclusion, `0 < timeout <= 86400`, `dict[str,str]` metadata.
- `State` — enum + transition graph; illegal jumps (e.g. `RUNNING → CLEANED`) are rejected.

**State machine** (sqlite-tracked, **enforced on every write path**): `RUNNING → CLEANING → CLEANED`. The transition guard is encoded into the `try_claim_for_kill` CAS (`WHERE state IN (RUNNING, CLEANING)`) so a `CLEANED → CLEANING` reversal is rejected atomically — you can't bypass the state machine by going around `set_state`. Pre-warm/build states are transient (not persisted); failure raises.

**Three independent concurrency limits** — different resources, not one shared lock:
- `build` — template builds (don't consume sandbox quota)
- `create` — `Sandbox.create` rate (token bucket, matches E2B creation rate limit)
- `run` — simultaneously-held sandboxes (eval concurrency)

**Heartbeat**: `acquire` spawns a daemon thread refreshing `last_heartbeat`. `reap`/`shutdown` only kill RUNNING sandboxes whose heartbeat is stale (process crashed) — active long tasks keep fresh heartbeats and are never reaped. `reconcile()` (call on startup) marks crashed-process orphans dead without racing live `acquire`s.

**Iron rules** (from real-failure diagnosis):
1. `Sandbox.list()` is a *clue*, not truth — the reaper never loops "list until empty" (that caused 15,000 redundant kills in a prior tool). sqlite is the source of truth.
2. Each sandbox_id is killed **once**; on kill it's marked CLEANED, and seen-again ids are skipped.
3. Every reaper loop has a max-iteration cap — no spinning.

## Known limitations

- **Template cleanup**: E2B SDK 2.x exposes no `Template.list`/`Template.remove`. Templates built by me2b accumulate on your account; reuse is deduped by content hash, but distinct images produce distinct templates. Clean up via the E2B console.
- **Crash recovery** requires reusing the same `db_path` across runs; a fresh db_path can't see prior orphans (use `reconcile()` on startup against a stable db).
- **Long tasks**: `timeout=` is a *hard ceiling* — E2B auto-kills at expiry. Pass a value exceeding your worst-case task duration (default 1800s).

## Status

Pre-1.0. The lifecycle core is review-hardened (7 rounds, ~38 issues fixed) with a pydantic v2 model layer and a strict state machine enforced on every write path. 17 test suites / 109 tests. Tests require a live E2B account (`E2B_API_KEY`) and create real sandboxes.
