"""#4 修复验证: 小 stale_timeout 下活跃沙箱不被 reap 误杀"""
import os, time, logging
os.environ["E2B_API_KEY"] = "***REMOVED***"
logging.basicConfig(level=logging.WARNING)
from managed_e2b import SandboxLifecycle

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

DB = "/root/sb_stale_clamp.db"
if os.path.exists(DB): os.remove(DB)
# stale_timeout=20 (interval = max(min(5,10),5)=5 < 20 ✓)
lc = SandboxLifecycle(db_path=DB, max_concurrent=2, stale_timeout=20)

print("[1] 小 stale_timeout=20, 活跃沙箱 sleep 8s 期间 reap 不误杀")
with lc.acquire(template="base", timeout=60, metadata={"track": "clamp"}) as h:
    alive1 = h.sandbox.is_running()
    time.sleep(6)  # 超过 1 个心跳周期(5s), 心跳应已刷新
    r = lc.reap()
    alive2 = h.sandbox.is_running()
    check("reap 前活", alive1)
    check("reap 后仍活(未被误杀)", alive2, f"reap={r}")
    check("reap 没清自己 RUNNING", r["reaped_own"] == 0)
check("退出后 cleaned", lc.db.get(h.sid)["state"] == "cleaned")

print("[2] interval 确实 < stale_timeout")
hb_interval = lc._stale_timeout // 4
clamped = max(min(hb_interval, lc._stale_timeout // 2), 5)
check("interval < stale_timeout", clamped < lc._stale_timeout, f"interval={clamped} stale={lc._stale_timeout}")

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()

print("\n[3] #4 边缘: stale<10 raise, interval<stale")
import os as _os
# stale<10 应 raise
try:
    SandboxLifecycle(db_path="/tmp/sb_bad.db", max_concurrent=1, stale_timeout=5)
    check("stale=5 应 raise", False, "(没raise)")
except ValueError:
    check("stale=5 raise", True)
# stale=10 interval<10
lc10 = SandboxLifecycle(db_path="/tmp/sb10.db", max_concurrent=1, stale_timeout=10)
hb = _Heartbeat(lc10, "x", 10)
check("stale=10 interval<10", hb._interval < 10, f"interval={hb._interval}")
lc10.shutdown()
