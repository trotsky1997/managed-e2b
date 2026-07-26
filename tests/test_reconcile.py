"""#5 reconcile 测试: sqlite 有 RUNNING 残留但 E2B 无对应 → 标 CLEANED"""
import os, logging
os.environ["E2B_API_KEY"] = "***REMOVED***"
logging.basicConfig(level=logging.WARNING)
from managed_e2b import SandboxLifecycle, State

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

DB = "/root/sb_reconcile.db"
if os.path.exists(DB): os.remove(DB)
lc = SandboxLifecycle(db_path=DB, max_concurrent=2)

print("[1] sqlite 有 RUNNING 残留(模拟崩溃), E2B 无对应 → reconcile 标 CLEANED")
# 造 2 个假的 RUNNING 行(E2B 不会有这些 id)
lc.db.upsert("fake-dead-1", state=State.RUNNING.value, template="base",
             created_at=100, last_heartbeat=100, metadata="{}")
lc.db.upsert("fake-dead-2", state=State.RUNNING.value, template="base",
             created_at=100, last_heartbeat=100, metadata="{}")
check("造了 2 个 RUNNING 残留", len(lc.db.list_state(State.RUNNING)) == 2)

# reconcile: E2B 列表里没有这俩 → 标 CLEANED
r = lc.reconcile()
check("reconcile 清掉 2 个残留", r["reconciled"] == 2, f"reconciled={r['reconciled']}")
check("残留行变 CLEANED", lc.db.get("fake-dead-1")["state"] == "cleaned")
check("无 RUNNING 残留", len(lc.db.list_state(State.RUNNING)) == 0)

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
