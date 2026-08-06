"""测试 save/fork/pause/resume/restore_from_snapshot"""
import os
os.environ.setdefault("E2B_API_KEY", os.environ.get("E2B_API_KEY") or "")  # 官方(pause 火山禁用)
from managed_e2b import SandboxLifecycle

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

lc = SandboxLifecycle(db_path="/root/sb_snap.db", max_concurrent=2,
                      e2b_key=os.environ["E2B_API_KEY"])

print("[1] save + restore_from_snapshot: 快照状态能恢复")
with lc.acquire(template="base", timeout=300) as h:
    h.stage_in({"data.txt": "snapshot-state-42"})
    snap_id = h.save(name="me2b-test-snap")
check("save 返回 snapshot_id", snap_id is not None and "/" in snap_id, f"({snap_id})")

# 从快照恢复
with lc.restore_from_snapshot(snap_id, timeout=120) as h2:
    out = h2.stage_out(["data.txt"])
    check("快照恢复后 state 保留", out.get("data.txt") == b"snapshot-state-42", f"({out.get('data.txt')})")
check("恢复沙箱 cleaned", lc.db.get(h2.sid)["state"] == "cleaned")

print("\n[2] fork: 复制带状态的沙箱")
with lc.acquire(template="base", timeout=300) as h:
    h.stage_in({"f.txt": "forked"})
    clones = h.fork(count=2, timeout=60)
    check("fork 返回 2 个", len(clones) == 2, f"({len(clones)})")
    if clones:
        out = clones[0].stage_out(["f.txt"])
        check("fork 副本有 state", out.get("f.txt") == b"forked")
        for c in clones:
            try: c.sandbox.kill()
            except: pass

print("\n[3] pause + resume (官方端点)")
with lc.acquire(template="base", timeout=300) as h:
    h.stage_in({"p.txt": "paused-state"})
    paused = h.pause(keep_memory=True)
    check("pause 返回 True", paused is True)
    resumed = h.resume()
    out = resumed.stage_out(["p.txt"])
    check("resume 后 state 保留", out.get("p.txt") == b"paused-state", f"({out.get('p.txt')})")

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
