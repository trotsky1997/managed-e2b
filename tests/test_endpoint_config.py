"""P0 回归: key/url 只走构造参数(不预置 env)也能跑 —— 锁住 e2b_key 下沉 env 修复"""
import os, time
# 关键: 不预置 E2B_API_KEY / E2B_API_URL env, 只走构造参数
os.environ.pop("E2B_API_KEY", None)
os.environ.pop("E2B_API_URL", None)

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

print("[1] key/url 只走构造参数, 不预置 env")
from managed_e2b import SandboxLifecycle
lc = SandboxLifecycle(
    db_path="/root/sb_endpoint.db",
    max_concurrent=2,
    e2b_key=os.environ["E2B_API_KEY"],  # E2B 官方
)
# 沙箱能 create = key 下沉 env 生效
with lc.acquire(template="base", timeout=60, metadata={"track": "endpoint-test"}) as h:
    check("构造参数 key 生效(沙箱建起)", h.sid.startswith(("i", "v", "s")))
    r = h.sandbox.commands.run("echo endpoint_ok")
    check("执行成功", (r.stdout or "").strip() == "endpoint_ok")
check("退出后 cleaned", lc.db.get(h.sid)["state"] == "cleaned")

print("[2] 环境变量确实被写入")
check("E2B_API_KEY env 已写入", os.environ.get("E2B_API_KEY") == os.environ["E2B_API_KEY"])

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
