"""验证 resume_sandbox 纳入状态机: 进 db + _inflight, 可被清理"""
import os
os.environ["E2B_API_KEY"] = "***REMOVED***"
from managed_e2b import SandboxLifecycle, State

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

lc = SandboxLifecycle(db_path="/root/sb_resume.db", max_concurrent=2,
                      e2b_key="***REMOVED***")

# 先 acquire 一个沙箱, pause 它
with lc.acquire(template="base", timeout=300) as h:
    h.pause(keep_memory=True)
    sid = h.sid
# 沙箱已 pause, 用 resume_sandbox 恢复 (非 acquire 路径)
h2 = lc.resume_sandbox(sid)
check("resume_sandbox 返回 handle", h2 is not None)
check("进 sqlite", lc.db.get(sid) is not None)
check("状态=running", lc.db.get(sid)["state"] == "running")
check("进 _inflight", sid in lc._inflight)
check("有 lifecycle 引用", h2.lifecycle is lc)
# 可执行
r = h2.run("echo resumed_ok")
check("可执行", r["stdout"].strip() == "resumed_ok")
# 能被清理 (走 _kill_one)
ok = lc._kill_one(sid)
check("能被 _kill_one 清理", ok)
check("清后=cleaned", lc.db.get(sid)["state"] == "cleaned")

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
