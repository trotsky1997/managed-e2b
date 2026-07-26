"""#2 修复验证: purge_foreign=True 真正 kill 外部沙箱(不在 DB)"""
import os, time, logging
os.environ["E2B_API_KEY"] = "***REMOVED***"
logging.basicConfig(level=logging.WARNING)
from managed_e2b import SandboxLifecycle
from e2b_code_interpreter import Sandbox

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

DB = "/root/sb_purge.db"
if os.path.exists(DB): os.remove(DB)
lc = SandboxLifecycle(db_path=DB, max_concurrent=2, stale_timeout=600)

print("[1] 外部沙箱(不在 DB) + purge_foreign=True → 真被 kill")
# 建一个不被 lifecycle 追踪的外部沙箱
foreign = Sandbox.create(template="base", timeout=300, metadata={"track": "foreign-purge-test"})
check("外部沙箱建起", foreign.is_running())
check("外部沙箱不在 sqlite", lc.db.get(foreign.sandbox_id) is None)

# purge_foreign=True 应该杀掉它
r = lc.reap(purge_foreign=True)
check("purge_foreign 后沙箱已死", not foreign.is_running(), f"reap={r}")

print("[2] purge_foreign=False → 不杀外部沙箱(保守)")
foreign2 = Sandbox.create(template="base", timeout=300, metadata={"track": "foreign-keep"})
r2 = lc.reap(purge_foreign=False)
check("保守模式不杀外部沙箱", foreign2.is_running(), f"reap={r2}")
foreign2.kill()  # 手动清理

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
