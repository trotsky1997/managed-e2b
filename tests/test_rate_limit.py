"""#7 修复验证: create 速率限流 (token bucket) 生效"""
import os, time, threading, logging
os.environ.setdefault("E2B_API_KEY", os.environ.get("E2B_API_KEY") or "")
logging.basicConfig(level=logging.WARNING)
from managed_e2b import SandboxLifecycle, RateLimiter

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

print("[1] RateLimiter 限速: rate=2/s, 4 次 create 间隔 >= 0.5s")
rl = RateLimiter(2.0)
times = []
for i in range(4):
    with rl.slot():
        times.append(time.monotonic())
gaps = [times[i+1]-times[i] for i in range(3)]
check("每次间隔 >= 0.5s", all(g >= 0.45 for g in gaps), f"gaps={[f'{g:.2f}' for g in gaps]}")

print("[2] acquire 用 create_rate 限速")
DB = "/root/sb_rate.db"
if os.path.exists(DB): os.remove(DB)
lc = SandboxLifecycle(db_path=DB, max_concurrent=4, create_rate=2.0)
start = time.monotonic()
sids = []
def task(i):
    with lc.acquire(template="base", timeout=60, metadata={"track": f"r{i}"}) as h:
        sids.append(h.sid)
ts = [threading.Thread(target=task, args=(i,)) for i in range(3)]
for t in ts: t.start()
for t in ts: t.join()
elapsed = time.monotonic() - start
# 3 个 create, rate=2/s (0.5s 间隔) → 至少 ~1s
check("3 并发 create 被限速 (>= 0.9s)", elapsed >= 0.9, f"elapsed={elapsed:.2f}s")
check("3 个全 cleaned", lc.db.stats().get("cleaned", 0) == 3)

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
