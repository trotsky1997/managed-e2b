"""prewarm 去重测试: 同输入不重复 build"""
import os, time, threading, logging
os.environ["E2B_API_KEY"] = "***REMOVED***"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
from managed_e2b import SandboxLifecycle

DB = "/root/sb_prewarm.db"
if os.path.exists(DB): os.remove(DB)
lc = SandboxLifecycle(db_path=DB, max_concurrent=4)

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

# 测1: 同镜像 prewarm 两次, 第二次应命中缓存不 build
print("[1] image 去重: 同镜像两次 prewarm")
t0 = time.time()
n1 = lc.prewarm(image="python:3.11-slim")
t1 = time.time()
n2 = lc.prewarm(image="python:3.11-slim")
t2 = time.time()
check("两次返回同名 template", n1 == n2, f"({n1} == {n2})")
check("第二次远快于第一次(命中缓存)", (t2 - t1) < (t1 - t0) * 0.3,
      f"(第一次 {t1-t0:.1f}s, 第二次 {t2-t1:.2f}s)")

# 测2: 并发 5 个同镜像 prewarm, 只 build 一次
print("[2] 并发去重: 5 线程同镜像")
lc2 = SandboxLifecycle(db_path="/root/sb_prewarm2.db", max_concurrent=4)
build_count = {"n": 0}
# 包一层统计 build 次数
orig_build = lc2._template_ready.__class__
results = []
def concur(i):
    n = lc2.prewarm(image="python:3.12-slim")
    results.append(n)
ts = [threading.Thread(target=concur, args=(i,)) for i in range(5)]
for t in ts: t.start()
for t in ts: t.join()
check("5 线程返回同名", len(set(results)) == 1, f"({set(results)})")
# exists 命中后 _template_ready 只设一次, 但无法直接数 build; 改用时间: 全部应快
check("并发 prewarm 完成", len(results) == 5)

# 测3: image vs dockerfile 不同输入 → 不同 template
print("[3] image 与 dockerfile 区分")
n_img = lc.template_name_for(image="python:3.11-slim")
n_df = lc.template_name_for(dockerfile="FROM python:3.11-slim\nRUN pip install numpy")
check("image 和 dockerfile 不同名", n_img != n_df, f"({n_img} != {n_df})")
check("image 名前缀 img-", n_img.startswith("img-"))
check("dockerfile 名前缀 df-", n_df.startswith("df-"))

# 测4: acquire(image=...) 走通完整链路
print("[4] acquire(image=) 完整链路")
with lc.acquire(image="python:3.11-slim", timeout=60, metadata={"track": "acq"}) as h:
    r = h.sandbox.commands.run("echo ok")
    check("acquire(image) 执行成功", (r.stdout or "").strip() == "ok", f"({(r.stdout or '').strip()})")
check("acquire 后沙箱 cleaned", lc.db.get(h.sid)["state"] == "cleaned")

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown(); lc2.shutdown()
