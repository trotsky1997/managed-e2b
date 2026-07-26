"""测试 mount_tos (s3fs 挂 TOS 桶) + 挂载后读写"""
import os
os.environ.setdefault("E2B_API_KEY", os.environ.get("E2B_API_KEY", ""))
os.environ.setdefault("E2B_TOS_AK", os.environ.get("E2B_TOS_AK", ""))
os.environ.setdefault("E2B_TOS_SK", os.environ.get("E2B_TOS_SK", ""))
from managed_e2b import SandboxLifecycle

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

lc = SandboxLifecycle(db_path="/root/sb_mount.db", max_concurrent=1,
                      e2b_key="***REMOVED***")
with lc.acquire(template="base", timeout=600) as h:
    print("挂载 TOS 桶 (装 s3fs 可能慢)...")
    h.mount_tos("tos-mindverse")
    r = h.run("ls /mnt/tos/ | head -3", timeout=30)
    check("挂载后能 ls 桶内容", len(r["stdout"].strip()) > 0, f"({r['stdout'].strip()[:40]})")
    # 挂载点写入(= 上传到 TOS)
    r = h.run("echo me2b_mount_test > /mnt/tos/me2b-fuse-test.txt && cat /mnt/tos/me2b-fuse-test.txt", timeout=30)
    check("挂载点写入+读取", "me2b_mount_test" in r["stdout"])
    h.run("rm /mnt/tos/me2b-fuse-test.txt", timeout=15)
print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
