"""R5-1 修复验证: reconcile 与 acquire 并发不泄漏沙箱
之前: reconcile 直接标 RUNNING 为 CLEANED(不 kill), 与 acquire 并发时
      acquire-finally 跳过 kill → 沙箱留 E2B 泄漏 (实测 2/4 漏)
现在: reconcile 只清 stale 的 RUNNING(无心跳), 活跃的(有心跳)不碰 → acquire 自己 kill
"""
import os, time, threading, logging
os.environ["E2B_API_KEY"] = "***REMOVED***"
logging.basicConfig(level=logging.WARNING)
from managed_e2b import SandboxLifecycle
from e2b_code_interpreter import Sandbox

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

DB = "/root/sb_r5_conc.db"
if os.path.exists(DB): os.remove(DB)
lc = SandboxLifecycle(db_path=DB, max_concurrent=2, stale_timeout=600)

print("=== R5-1: reconcile 与 acquire 并发, 沙箱必须被 kill 不泄漏 ===")
leaked = []
def run_task(i):
    """acquire 一个沙箱, 期间另一个线程跑 reconcile"""
    try:
        with lc.acquire(template="base", timeout=120, metadata={"track": f"c{i}"}) as h:
            # acquire 持有期间, reconcile 不该碰它(心跳新)
            time.sleep(2)
            # 记录 sid 供事后检查是否被 E2B 清掉
            leaked.append(h.sid)
    except Exception as e:
        print(f"  [t{i}] 异常: {e}")

threads = [threading.Thread(target=run_task, args=(i,)) for i in range(3)]
# 同时跑 reconcile(可能和 acquire 并发)
for t in threads: t.start()
time.sleep(0.5)
lc.reconcile()  # 并发跑
for t in threads: t.join()

# 所有沙箱退出 with 后应被 kill。检查它们在 E2B 上是否还活着
alive_count = 0
for sid in leaked:
    try:
        if Sandbox.connect(sid).is_running():
            alive_count += 1
    except Exception:
        pass  # 已死=已清, 不计泄漏
check("无泄漏(所有沙箱退出后被 kill)", alive_count == 0, f"(leaked alive: {alive_count}/{len(leaked)})")
check("退出后全 cleaned", lc.db.stats().get("cleaned", 0) == len(leaked))

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
