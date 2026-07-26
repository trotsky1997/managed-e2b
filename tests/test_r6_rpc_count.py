"""#2 v2 RPC 计数验证: prewarm 真的走到 READY 缓存分支了吗?(r4 死代码教训)
数每次 prewarm 实际调 build_in_background 的次数:
  - 全新 image → 期望 1 次 build
  - READY 复用 → 期望 0 次(走 exists+轮询, 命中缓存)
  - 同 READY 再来 → 期望 0 次
"""
import os, time, logging
os.environ["E2B_API_KEY"] = "***REMOVED***"
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

from managed_e2b import SandboxLifecycle
from e2b import Template

# 计数 build_in_background 调用
_orig_build_bg = Template.build_in_background.__func__ if hasattr(Template.build_in_background, "__func__") else Template.build_in_background
calls = {"n": 0, "names": []}

def _counting_build_bg(*args, **kwargs):
    calls["n"] += 1
    name = kwargs.get("name") or (args[1] if len(args) > 1 else "?")
    calls["names"].append(name)
    logging.info(f"  >>> build_in_background 调用 #{calls['n']}: name={name}")
    return _orig_build_bg(*args, **kwargs)

# 替换静态方法
Template.build_in_background = staticmethod(_counting_build_bg)

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

DB = "/root/sb_r6_rpc.db"
if os.path.exists(DB): os.remove(DB)
lc = SandboxLifecycle(db_path=DB, max_concurrent=2)

IMG = "python:3.11-slim"
# 用一个全新 name(加时间戳), 确保首次 build
import hashlib
uniq = "img-r6-" + str(int(time.time()))[-6:]

print(f"\n[1] 全新 image(从未 build) → 期望 1 次 build_in_background")
calls["n"] = 0; calls["names"] = []
# monkeypatch template_name_for 让它返回我们的 uniq 名
lc.template_name_for = lambda image=None, dockerfile=None: uniq
t0 = time.time()
n1 = lc.prewarm(image=IMG)
dt = time.time() - t0
check(f"返回名={uniq}", n1 == uniq, f"(got {n1})")
check("build 调用 1 次", calls["n"] == 1, f"(实际 {calls['n']} 次, names={calls['names']})")

print(f"\n[2] READY 复用(已 build 好) → 期望 0 次 build_in_background")
calls["n"] = 0; calls["names"] = []
t0 = time.time()
n2 = lc.prewarm(image=IMG)
dt = time.time() - t0
check("返回同名(复用)", n2 == n1)
check("build 调用 0 次(走 exists+轮询缓存)", calls["n"] == 0, f"(实际 {calls['n']} 次)")
check("复用快(无 build)", dt < 5, f"({dt:.1f}s)")

print(f"\n[3] 同 READY 再来一次 → 期望 0 次(内存缓存命中)")
calls["n"] = 0; calls["names"] = []
n3 = lc.prewarm(image=IMG)
check("build 调用 0 次(内存快路径)", calls["n"] == 0, f"(实际 {calls['n']} 次)")

print(f"\n[4] acquire(image=) 端到端 + 无冗余 build")
calls["n"] = 0
with lc.acquire(image=IMG, timeout=60, metadata={"track": "r6"}) as h:
    r = h.sandbox.commands.run("echo r6_ok")
check("acquire 执行成功", (r.stdout or "").strip() == "r6_ok")
check("acquire 没触发额外 build", calls["n"] == 0, f"(实际 {calls['n']} 次)")

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
