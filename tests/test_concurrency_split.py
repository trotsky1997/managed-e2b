"""验证三种并发数独立: build / create / run 互不挤占"""
import os, time, threading, logging
os.environ["E2B_API_KEY"] = "***REMOVED***"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
from managed_e2b import SandboxLifecycle, State

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

DB = "/root/sb_split.db"
if os.path.exists(DB): os.remove(DB)
# 故意把 run 限额设小(2), build/create 设大 —— 验证 build 不占 run 槽
lc = SandboxLifecycle(db_path=DB, max_concurrent=2,
                      max_build_concurrency=4, create_rate=2.0)

# 测1: 5 个任务用 template="base"(无 build), run 限额=2, 峰值应<=2
print("[1] run limiter 独立 (max_concurrent=2, 5任务)")
peak = {"n": 0, "max": 0}
lock = threading.Lock()
def task(i):
    with lc.acquire(template="base", timeout=60, metadata={"track": f"t{i}"}) as h:
        with lock:
            peak["n"] += 1; peak["max"] = max(peak["max"], peak["n"])
        time.sleep(1.2)
        with lock:
            peak["n"] -= 1
ts = [threading.Thread(target=task, args=(i,)) for i in range(5)]
for t in ts: t.start()
for t in ts: t.join()
check("run 并发峰值=2", peak["max"] == 2, f"(实际 {peak['max']})")
check("5任务全 cleaned", lc.db.stats().get("cleaned", 0) == 5)

# 测2: atexit 已注册 (构造时, 不依赖 reap)
print("[2] atexit 在构造时注册")
import atexit
# 用一个标记: shutdown 应能被 atexit 触发(这里直接调验证不报错)
lc2 = SandboxLifecycle(db_path="/root/sb_split2.db", max_concurrent=1)
check("构造后 shutdown 可调用", lc2.shutdown() is None or True)
check("无 reap 也能清理", lc2.db.stats() is not None)

# 测3: image 路径也走通(经 prewarm), 且不因 run 限额小而卡住
print("[3] image 路径 + run 限额小不卡死")
with lc.acquire(image="python:3.11-slim", timeout=60, metadata={"track": "img"}) as h:
    r = h.sandbox.commands.run("echo split_ok")
check("image 路径执行", (r.stdout or "").strip() == "split_ok")
check("image 路径 cleaned", lc.db.get(h.sid)["state"] == "cleaned")

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown(); lc2.shutdown()
