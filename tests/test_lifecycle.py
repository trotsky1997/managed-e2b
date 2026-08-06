"""沙箱生命周期管理器 —— 关键场景测试"""
import os, time, threading, logging
os.environ.setdefault("E2B_API_KEY", os.environ.get("E2B_API_KEY") or "")
logging.basicConfig(level=logging.WARNING)
from managed_e2b import SandboxLifecycle, State

passed, failed = [], []

def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

DB = "/root/sb_test.db"
if os.path.exists(DB):
    os.remove(DB)

# ---- 测试1: 并发 limiter 生效 (max_concurrent=2, 起5个, 同时在跑应<=2) ----
print("\n[1] 并发 limiter")
lc = SandboxLifecycle(db_path=DB, max_concurrent=2)
peak = {"n": 0, "max": 0}
lock = threading.Lock()

def task(i):
    with lc.acquire(template="base", timeout=60, metadata={"track": f"t{i}"}) as h:
        with lock:
            peak["n"] += 1
            peak["max"] = max(peak["max"], peak["n"])
        time.sleep(1.5)
        with lock:
            peak["n"] -= 1

ts = [threading.Thread(target=task, args=(i,)) for i in range(5)]
for t in ts: t.start()
for t in ts: t.join()
check("并发峰值<=2", peak["max"] <= 2, f"(实际峰值 {peak['max']})")
check("5个任务全 CLEANED", lc.db.stats().get("cleaned", 0) >= 5, f"stats={lc.db.stats()}")

# ---- 测试2: 重复 reap 不重复杀 (铁律2) ----
print("\n[2] 重复 reap 去重")
r1 = lc.reap()
killed_before = sum(r1.values())  # 不含 already_dead
# 再 reap 一次: 自己的都 CLEANED 了, 不应再杀任何 own
own_cleaned_before = lc.db.stats().get("cleaned", 0)
r2 = lc.reap()
check("二次 reap 不重复杀自己的", r2["reaped_own"] == 0, f"reaped_own={r2['reaped_own']}")
check("二次 reap cleaned 数不变", lc.db.stats().get("cleaned", 0) == own_cleaned_before)

# ---- 测试3: 孤儿检测 (模拟进程崩溃: 沙箱在 RUNNING, 进程崩了没 kill,
#         被某机制(如 E2B timeout 或手动)弄到 CLEANING 态, reap 清掉残留) ----
print("\n[3] 孤儿检测+清理 (CLEANING 残留)")
Sandbox = lc._sandbox_cls()
orphan = Sandbox.create(template="base", timeout=300, metadata={"track": "orphan"})
# 模拟崩溃残留: 标成 CLEANING(进程在 kill 中途崩了), 沙箱其实还活着
lc.db.upsert(orphan.sandbox_id, state=State.CLEANING.value, template="base",
             created_at=int(time.time()), metadata='{"track":"orphan"}')
check("孤儿制造时 is_running=True", orphan.is_running())
r = lc.reap()
check("reap 清掉 CLEANING 残留孤儿", r["reaped_own"] >= 1, f"reaped_own={r['reaped_own']}")
check("孤儿最终 is_running=False", not orphan.is_running())
check("孤儿 sqlite 状态=cleaned", lc.db.get(orphan.sandbox_id)["state"] == "cleaned")

# ---- 测试4: atexit 清理 (进程退出杀掉 RUNNING 的) ----
print("\n[4] atexit 清理")
# 这个进程结束时 shutdown 会被调, 这里显式调验证
lc.shutdown()
check("shutdown 后无 RUNNING 残留", lc.db.stats().get("running", 0) == 0, f"stats={lc.db.stats()}")

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed:
    print("FAILED:", failed)
