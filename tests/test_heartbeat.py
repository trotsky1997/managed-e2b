"""心跳机制测试: 崩溃残留(无心跳)被清, 活跃任务(有心跳)不误杀"""
import os, time, threading, logging
os.environ["E2B_API_KEY"] = "***REMOVED***"
logging.basicConfig(level=logging.WARNING)
from managed_e2b import SandboxLifecycle, State

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

DB = "/root/sb_heartbeat.db"
if os.path.exists(DB): os.remove(DB)
# stale_timeout=30, 心跳间隔 30//4=7s —— 测试快
lc = SandboxLifecycle(db_path=DB, max_concurrent=2, stale_timeout=30)

print("[1] 崩溃残留(无心跳)的 RUNNING 被 reap 清掉")
Sandbox = lc._sandbox_cls()
orphan = Sandbox.create(template="base", timeout=300, metadata={"track": "crash-orphan"})
# 模拟崩溃: 进程在 acquire 中途崩, 沙箱留 RUNNING 但无心跳(不进 acquire 就不会有心跳线程)
lc.db.upsert(orphan.sandbox_id, state=State.RUNNING.value, template="base",
             created_at=int(time.time()), running_since=int(time.time()),
             last_heartbeat=int(time.time()) - 200)  # 200s 前的心跳, 远超 stale_timeout=30
check("孤儿制造时活", orphan.is_running())
r = lc.reap()
check("reap 清掉无心跳的崩溃残留", r["reaped_own"] >= 1, f"reaped_own={r['reaped_own']}")
check("孤儿被杀", not orphan.is_running())
check("孤儿 sqlite=cleaned", lc.db.get(orphan.sandbox_id)["state"] == "cleaned")

print("[2] 活跃任务(有心跳)不被 reap 误杀")
with lc.acquire(template="base", timeout=300, metadata={"track": "active"}) as h:
    # acquire 期间心跳线程在刷, 即使超过 stale_timeout 也不该被清
    time.sleep(3)  # 让心跳刷新
    r = lc.reap()
    check("活跃沙箱 reap 后仍活", h.sandbox.is_running())
    check("reap 没杀活跃的", r["reaped_own"] == 0)
    out = h.sandbox.commands.run("echo hb_ok")
    check("活跃沙箱仍可执行", (out.stdout or "").strip() == "hb_ok")

print("[3] 心跳确实在刷新")
with lc.acquire(template="base", timeout=300, metadata={"track": "hb-check"}) as h:
    hb1 = lc.db.get(h.sid)["last_heartbeat"]
    time.sleep(8)  # 心跳间隔 7s, 应刷一次
    hb2 = lc.db.get(h.sid)["last_heartbeat"]
    check("心跳在刷新", hb2 > hb1, f"({hb1} -> {hb2})")

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
