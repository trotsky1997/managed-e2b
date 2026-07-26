"""示例: 用 me2b 在 E2B 沙箱执行代码 (stage in + run_script + stage out)。

    pip install -e .  (或 git+https://github.com/trotsky1997/managed-e2b.git)
    export E2B_API_KEY=e2b_...
    python examples/run_code.py
"""
from managed_e2b import SandboxLifecycle, load_env

load_env()  # 读 .env (如有)

lc = SandboxLifecycle(db_path="me2b.db", max_concurrent=2)

# 一个会算斐波那契的脚本
fib_script = """
def fib(n):
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a
import json
print(fib(20))
open('result.json', 'w').write(json.dumps({'fib20': fib(20)}))
"""

with lc.acquire(template="base", timeout=120) as h:
    h.stage_in({"fib.py": fib_script})
    r = h.run_script("fib.py", timeout=30)
    print("stdout:", r["stdout"].strip())          # 6765
    out = h.stage_out(["result.json"])
    print("result:", out["result.json"])            # b'{"fib20": 6765}'

# 退出 with 自动 kill; 进程退出 atexit 兜底清理
