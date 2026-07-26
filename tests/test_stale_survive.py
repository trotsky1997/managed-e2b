"""R3-3: acquire 持有超过 stale_timeout 时 reap 不误杀(心跳持续刷新)"""
import os, time, logging
os.environ["E2B_API_KEY"] = "***REMOVED***"
logging.basicConfig(level=logging.WARNING)
from managed_e2b import SandboxLifecycle

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

DB = "/root/sb_stale_survive.db"
if os.path.exists(DB): os.remove(DB)
# stale_timeout=15, 心跳间隔=max(min(3,7),1)=3s, 任务跑 20s(超过 stale)
lc = SandboxLifecycle(db_path=DB, max_concurrent=1, stale_timeout=15)

print("acquire 持有 20s(超过 stale_timeout=15), 期间 reap 两次不误杀")
with lc.acquire(template="base", timeout=120, metadata={"track": "survive"}) as h:
    alive1 = h.sandbox.is_running()
    time.sleep(8); lc.reap()   # 第一次 reap, 沙箱已跑 8s(<15, 不stale)
    time.sleep(8); lc.reap()   # 第二次, 跑 16s(>15!) 但心跳持续刷新, 不该被判 stale
    alive2 = h.sandbox.is_running()
    out = h.sandbox.commands.run("echo survived")
    check("reap 前活", alive1)
    check("超过 stale_timeout 后仍活(心跳保护)", alive2)
    check("仍可执行", (out.stdout or "").strip() == "survived")
check("退出后 cleaned", lc.db.get(h.sid)["state"] == "cleaned")

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
