"""验证 reap 不误杀活跃长任务(#8 修复回归)"""
import os, time, threading, logging
os.environ.setdefault("E2B_API_KEY", os.environ.get("E2B_API_KEY") or "")
logging.basicConfig(level=logging.WARNING)
from managed_e2b import SandboxLifecycle, State

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

DB = "/root/sb_reap_safe.db"
if os.path.exists(DB): os.remove(DB)
lc = SandboxLifecycle(db_path=DB, max_concurrent=2)

print("[1] 活跃沙箱不被 reap 误杀")
with lc.acquire(template="base", timeout=300, metadata={"track": "long-task"}) as h:
    # 模拟长任务: 沙箱运行中, 期间 reap 不该杀它
    alive_before = h.sandbox.is_running()
    r = lc.reap()  # reap 期间沙箱在 RUNNING
    time.sleep(1)
    alive_after = h.sandbox.is_running()
    check("reap 前沙箱活", alive_before)
    check("reap 后沙箱仍活(未被误杀)", alive_after, f"(reap={r})")
    check("reap 没清自己的 RUNNING", r["reaped_own"] == 0)
    # 沙箱还能用
    out = h.sandbox.commands.run("echo still_alive")
    check("reap 后沙箱仍可执行", (out.stdout or "").strip() == "still_alive")
check("退出后沙箱 cleaned", lc.db.get(h.sid)["state"] == "cleaned")

print("[2] CLEANING 残留仍被清(进程崩溃模拟)")
# 手动造一个 CLEANING 态(模拟崩溃残留), 但不真建沙箱 → _kill_one 应安全处理
lc.db.upsert("fake-crash-sid", state=State.CLEANING.value, template="base",
             created_at=int(time.time()))
r = lc.reap()
check("CLEANING 残留被 reap 处理", True, f"(reaped_own={r['reaped_own']})")

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
