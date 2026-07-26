"""验证状态机强制: 所有写 state 的路径都受 transition_to 约束, 跳跃被拒"""
import os
os.environ["E2B_API_KEY"] = "***REMOVED***"
from managed_e2b import SandboxLifecycle, State

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

lc = SandboxLifecycle(db_path="/root/sb_enforce.db", max_concurrent=1)
db = lc.db

print("[1] set_state 拒绝跳跃: RUNNING→CLEANED 必须经 CLEANING")
db.upsert("s1", state=State.RUNNING.value, created_at=100, last_heartbeat=100)
try:
    db.set_state("s1", State.CLEANED)
    check("RUNNING→CLEANED 被拒", False, "(没抛)")
except ValueError as e:
    check("RUNNING→CLEANED 抛 ValueError", True, str(e)[:50])
# 合法路径: RUNNING→CLEANING→CLEANED
db.set_state("s1", State.CLEANING)
db.set_state("s1", State.CLEANED)
check("RUNNING→CLEANING→CLEANED 合法", db.get("s1")["state"] == "cleaned")

print("\n[2] try_claim_for_kill 只允许 RUNNING/CLEANING→CLEANING")
db.upsert("s2", state=State.RUNNING.value, created_at=100, last_heartbeat=100)
check("RUNNING 抢占成功", db.try_claim_for_kill("s2") is True)
check("s2 现在 CLEANING", db.get("s2")["state"] == "cleaning")
db.upsert("s3", state=State.CLEANED.value, created_at=100)  # 已 CLEANED
check("CLEANED 抢占被拒(防倒退)", db.try_claim_for_kill("s3") is False)
check("s3 仍 CLEANED(没倒退)", db.get("s3")["state"] == "cleaned")

print("\n[3] _create 用 SandboxRecord(字段校验)")
# _create 插入会经 SandboxRecord 校验; 不直接测, 但确认 record 拒绝非法
from managed_e2b.models import SandboxRecord
try:
    SandboxRecord(sandbox_id="x", state=State.RUNNING, created_at=100, running_since=50)
    check("running_since<created_at 被拒", False)
except Exception:
    check("running_since<created_at 被拒", True)

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
