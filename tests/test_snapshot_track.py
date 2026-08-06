"""验证 save 追踪快照 + cleanup_snapshots 清理"""
import os
os.environ.setdefault("E2B_API_KEY", os.environ.get("E2B_API_KEY") or "")
from managed_e2b import SandboxLifecycle

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

lc = SandboxLifecycle(db_path="/root/sb_snaptrack.db", max_concurrent=2,
                      e2b_key=os.environ["E2B_API_KEY"])

snap_ids = []
with lc.acquire(template="base", timeout=300) as h:
    h.stage_in({"x.txt": "snap1"})
    s1 = h.save(name="me2b-track-1")
    s2 = h.save(name="me2b-track-2")
    snap_ids = [s1, s2]
check("save 返回 2 个快照", len(snap_ids) == 2, f"({snap_ids})")

# 追踪到 sqlite
snaps = lc.db.list_snapshots()
check("sqlite 记录 2 个快照", len(snaps) == 2, f"({len(snaps)})")
check("快照有 source_sid", all(r["source_sid"] for r in snaps))

# cleanup: 删 s1, 保留 s2
r = lc.cleanup_snapshots(keep={s2})
check("cleanup 删了 1 个", r["deleted"] == 1, f"({r})")
remaining = lc.db.list_snapshots()
check("剩 1 个 (s2)", len(remaining) == 1 and remaining[0]["snapshot_id"] == s2)

# 清理 s2
lc.cleanup_snapshots()
check("全清后 0 个", len(lc.db.list_snapshots()) == 0)

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
