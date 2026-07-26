"""示例: 挂载火山 TOS 桶到沙箱, 直接读写大文件 (不走 API 上传)。

    export E2B_API_KEY=e2b_...
    export E2B_TOS_AK=AKLT...   # 火山 TOS AK (无尾部 =)
    export E2B_TOS_SK=...       # 火山 TOS SK (base64 直接用)
    python examples/mount_tos.py
"""
from managed_e2b import SandboxLifecycle, load_env

load_env()

lc = SandboxLifecycle(db_path="me2b.db", max_concurrent=1)

with lc.acquire(template="base", timeout=600) as h:
    h.mount_tos("tos-mindverse")          # 挂载到 /mnt/tos
    # 当本地盘读写 = 直接操作 TOS 桶
    r = h.run("echo 'hello from me2b' > /mnt/tos/me2b-example.txt")
    r = h.run("cat /mnt/tos/me2b-example.txt")
    print("读到:", r["stdout"].strip())
    h.run("rm /mnt/tos/me2b-example.txt")
