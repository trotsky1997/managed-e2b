"""测试 stage_in + run_script + stage_out 端到端"""
import os
os.environ.setdefault("E2B_API_KEY", os.environ.get("E2B_API_KEY") or "")
from managed_e2b import SandboxLifecycle

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

lc = SandboxLifecycle(db_path="/root/sb_stage.db", max_concurrent=2,
                      e2b_key=os.environ["E2B_API_KEY"])

with lc.acquire(template="base", timeout=180) as h:
    # stage in: 推一个计算脚本 + 数据
    code = "import json\nwith open('input.json') as f: d=json.load(f)\nprint(d['x']+d['y'])\nopen('result.txt','w').write(str(d['x']*d['y']))"
    paths = h.stage_in({"solve.py": code, "input.json": '{"x":3,"y":5}'})
    check("stage_in 返回路径", paths["solve.py"] == "/task/solve.py", paths)

    # run_script: 执行推入的脚本
    r = h.run_script("solve.py", timeout=30)
    check("run_script exit 0", r["exit_code"] == 0, f"(exit={r['exit_code']})")
    check("stdout=8", r["stdout"].strip() == "8", f"(got {r['stdout'].strip()})")

    # stage_out: 取产物
    out = h.stage_out(["result.txt"])
    check("stage_out 取到产物", out["result.txt"] == b"15", f"(got {out['result.txt']})")

    # run raw
    r2 = h.run("echo raw_ok")
    check("run raw", r2["stdout"].strip() == "raw_ok")

check("沙箱 cleaned", lc.db.get(h.sid)["state"] == "cleaned")
print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown()
