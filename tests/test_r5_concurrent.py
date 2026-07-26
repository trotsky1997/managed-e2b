"""R3-1/R3-2: 真正触发 acquire-vs-reconcile 竞争 + 钉住 fresh-heartbeat 不被碰
R3-1 教训: 上版用 time.sleep(0.5) 发 reconcile, 但首行 ~1.3s 才出现 → 碰 0 行 = 假验证。
本版用事件同步: 等 RUNNING 行存在且心跳新, 再发 reconcile。
"""
import os, time, threading, logging
os.environ["E2B_API_KEY"] = "***REMOVED***"
logging.basicConfig(level=logging.WARNING)
from managed_e2b import SandboxLifecycle, State
from e2b_code_interpreter import Sandbox

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

DB = "/root/sb_r7_conc.db"
if os.path.exists(DB): os.remove(DB)
lc = SandboxLifecycle(db_path=DB, max_concurrent=2, stale_timeout=600)

# R3-2: 钉住 fresh-heartbeat RUNNING 行不被 reconcile 碰
print("[1] R3-2: reconcile 不碰 fresh-heartbeat 的 RUNNING 行")
held = {}
def hold_task():
    with lc.acquire(template="base", timeout=120, metadata={"track": "hold"}) as h:
        held["sid"] = h.sid
        held["event"].set()      # 通知主线程: RUNNING 行已存在
        time.sleep(3)            # 持有期间让 reconcile 跑
held["event"] = threading.Event()
t = threading.Thread(target=hold_task, daemon=True)
t.start()
held["event"].wait(30)          # 等 acquire 真正进入 RUNNING
sid = held["sid"]
# 此时行是 RUNNING + 心跳新, reconcile 不该碰它
row_before = lc.db.get(sid)
r = lc.reconcile()
row_after = lc.db.get(sid)
check("reconcile 前状态=running", row_before["state"] == "running")
check("reconcile 没碰 fresh-heartbeat 行", row_after["state"] == "running",
      f"(reconciled={r['reconciled']})")
check("reconcile 没 kill 持有的沙箱", Sandbox.connect(sid).is_running())
t.join()

# R3-1: 真竞争 — reconcile 在 acquire 持有期间跑, 退出后沙箱必须被 kill(不泄漏)
print("\n[2] R3-1: acquire-vs-reconcile 真竞争, 退出后 0 泄漏")
sids = []
def task(i, ev):
    with lc.acquire(template="base", timeout=120, metadata={"track": f"c{i}"}) as h:
        sids.append(h.sid)
        ev.set()
        time.sleep(2)

ev = threading.Event()
ts = [threading.Thread(target=task, args=(i, ev), daemon=True) for i in range(2)]
for t in ts: t.start()
ev.wait(30)  # 等至少一个进入 RUNNING
lc.reconcile()  # 持有期间并发 reconcile
for t in ts: t.join()

leaked = 0
for sid in sids:
    try:
        if Sandbox.connect(sid).is_running():
            leaked += 1
    except Exception:
        pass
check("退出后 0 泄漏(全被 kill)", leaked == 0, f"(leaked {leaked}/{len(sids)})")

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
