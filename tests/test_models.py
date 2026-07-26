"""pydantic 模型层测试: SandboxRecord 状态转移 + SandboxConfig 校验"""
import time
from managed_e2b.models import State, SandboxRecord, SandboxConfig

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

print("[1] State 转移校验")
check("RUNNING→CLEANING 合法", State.RUNNING.can_transition_to(State.CLEANING))
check("RUNNING→CLEANED 非法(须经 CLEANING)", not State.RUNNING.can_transition_to(State.CLEANED))
check("CLEANED→RUNNING 非法(终态)", not State.CLEANED.can_transition_to(State.RUNNING))
check("CLEANED→CLEANED 自转合法", State.CLEANED.can_transition_to(State.CLEANED))
check("QUEUED→PREWARMING 合法", State.QUEUED.can_transition_to(State.PREWARMING))
check("QUEUED→RUNNING 非法(跳态)", not State.QUEUED.can_transition_to(State.RUNNING))
check("terminal: CLEANED", State.CLEANED.terminal)
check("terminal: RUNNING 非", not State.RUNNING.terminal)

print("\n[2] SandboxRecord 转移 + 副作用")
r = SandboxRecord(sandbox_id="s1", state=State.RUNNING, created_at=100, last_heartbeat=100)
r2 = r.transition_to(State.CLEANING)
check("转移后 state=cleaning", r2.state == State.CLEANING)
check("转移后 killed_at 已设", r2.killed_at is not None)
check("原记录不可变(仍 running)", r.state == State.RUNNING)
# RUNNING 转移设 running_since + last_heartbeat
r3 = SandboxRecord(sandbox_id="s2", state=State.CREATING, created_at=100).transition_to(State.RUNNING)
check("CREATING→RUNNING 设 running_since", r3.running_since is not None)
check("CREATING→RUNNING 设 last_heartbeat", r3.last_heartbeat is not None)

print("\n[3] 非法转移抛 ValueError")
try:
    r.transition_to(State.QUEUED); check("RUNNING→QUEUED 抛", False)
except ValueError: check("RUNNING→QUEUED 抛 ValueError", True)
try:
    SandboxRecord(sandbox_id="s", state=State.CLEANED, created_at=1).transition_to(State.RUNNING)
    check("终态转出抛", False)
except ValueError: check("终态转出抛 ValueError", True)

print("\n[4] 字段校验")
try:
    SandboxRecord(sandbox_id="", state=State.RUNNING, created_at=1); check("空 id 抛", False)
except Exception: check("空 sandbox_id 抛", True)
try:
    SandboxRecord(sandbox_id="s", state=State.RUNNING, created_at=100, running_since=50)
    check("running_since<created_at 抛", False)
except Exception: check("running_since<created_at 抛", True)

print("\n[5] 往返序列化")
d = r2.to_db_row()
check("to_db_row 有 state", "state" in d and d["state"] == "cleaning")
check("to_db_row 去掉 None", all(v is not None for v in d.values()))
back = SandboxRecord.from_db_row(d)
check("from_db_row 还原 state", back.state == State.CLEANING)
check("from_db_row 还原 sid", back.sandbox_id == "s1")

print("\n[6] SandboxConfig 校验")
try:
    SandboxConfig(db_path="/x", stale_timeout=5); check("stale<10 抛", False)
except Exception: check("stale<10 抛", True)
try:
    SandboxConfig(db_path="/x", max_concurrent=0); check("max_concurrent=0 抛", False)
except Exception: check("max_concurrent>=1", True)
try:
    SandboxConfig(db_path="/x", create_rate=0); check("create_rate=0 抛", False)
except Exception: check("create_rate>0", True)
try:
    SandboxConfig(db_path="/x", bogus=1); check("extra 字段抛", False)
except Exception: check("extra='forbid' 拒绝未知字段", True)
c = SandboxConfig(db_path="/x", create_rate=2.0, max_concurrent=8)
check("正常配置 OK", c.max_concurrent == 8 and c.create_rate == 2.0)

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
