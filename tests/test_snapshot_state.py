"""验证 save/fork/pause/resume 纳入状态机: fork 副本进 db, pause→PAUSED, resume→RUNNING"""
import os
os.environ["E2B_API_KEY"] = "***REMOVED***"
from managed_e2b import SandboxLifecycle, State

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

lc = SandboxLifecycle(db_path="/root/sb_snapstate.db", max_concurrent=2,
                      e2b_key="***REMOVED***")

print("[1] fork 副本进状态机 (db + _inflight)")
with lc.acquire(template="base", timeout=300) as h:
    clones = h.fork(count=2, timeout=60)
    check("fork 2 个", len(clones) == 2)
    if clones:
        c0 = clones[0]
        row = lc.db.get(c0.sid)
        check("副本在 sqlite", row is not None)
        check("副本状态=running", row and row["state"] == "running", f"({row['state'] if row else '?'})")
        check("副本在 _inflight", c0.sid in lc._inflight)
        # atexit/shutdown 能清: 直接调 _kill_one 验证副本可清
        ok = lc._kill_one(c0.sid)
        check("副本能被 _kill_one 清理", ok)
        check("副本清后=cleaned", lc.db.get(c0.sid)["state"] == "cleaned")
        # 清理另一个
        lc._kill_one(clones[1].sid)

print("\n[2] pause → PAUSED 状态转移")
with lc.acquire(template="base", timeout=300) as h:
    r = h.pause(keep_memory=True)
    check("pause 成功", r is True)
    check("sqlite 状态=paused", lc.db.get(h.sid)["state"] == "paused", f"({lc.db.get(h.sid)['state']})")
    # resume → RUNNING
    h2 = h.resume()
    check("resume 后 sqlite=running", lc.db.get(h.sid)["state"] == "running")
    check("resume 后 is_running", h2.sandbox.is_running())

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
