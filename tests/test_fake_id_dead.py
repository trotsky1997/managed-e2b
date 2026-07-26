"""R2-2: fake-id(格式无效)的 CLEANING 行能收敛到 CLEANED, 不卡死"""
import os, logging
os.environ["E2B_API_KEY"] = "***REMOVED***"
logging.basicConfig(level=logging.WARNING)
from managed_e2b import SandboxLifecycle, State

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

DB = "/root/sb_fakeid.db"
if os.path.exists(DB): os.remove(DB)
lc = SandboxLifecycle(db_path=DB, max_concurrent=1)

# 造一个格式无效的 sid 的 CLEANING 行(E2B 会报 400: Invalid sandbox ID)
lc.db.upsert("fake-crash-sid-xyz", state=State.CLEANING.value, template="base",
             created_at=100, killed_at=100, metadata="{}")
check("造了 CLEANING 行", len(lc.db.list_state(State.CLEANING)) == 1)

# reap 应清掉它(不再卡 CLEANING)
r = lc.reap()
row = lc.db.get("fake-crash-sid-xyz")
check("fake-id CLEANING 行收敛到 cleaned", row["state"] == "cleaned",
      f"(reaped_own={r['reaped_own']})")
check("无 CLEANING 残留", len(lc.db.list_state(State.CLEANING)) == 0)

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
