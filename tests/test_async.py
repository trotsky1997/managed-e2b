"""async 核心测试: acquire + stage in/out + run_script + 并发"""
import os, asyncio
os.environ.setdefault("E2B_API_KEY", os.environ.get("E2B_API_KEY") or "")
from managed_e2b import AsyncSandboxLifecycle

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

async def main():
    lc = AsyncSandboxLifecycle(db_path="/root/sb_async.db", max_concurrent=2,
                               e2b_key=os.environ["E2B_API_KEY"])

    print("[1] async acquire + stage in/out + run_script")
    async with lc.acquire(template="base", timeout=180) as h:
        await h.stage_in({"solve.py": "print(6*7)", "data.json": '{"x":1}'})
        r = await h.run_script("solve.py", timeout=30)
        check("run_script exit 0", r["exit_code"] == 0, f"(exit={r['exit_code']})")
        check("stdout=42", r["stdout"].strip() == "42", f"({r['stdout'].strip()})")
        out = await h.stage_out(["data.json"])
        check("stage_out", out["data.json"] == b'{"x":1}')
    check("沙箱 cleaned", lc.db.get(h.sid)["state"] == "cleaned")

    print("\n[2] 并发 acquire (asyncio.gather)")
    async def task(i):
        async with lc.acquire(template="base", timeout=120) as h:
            r = await h.run(f"echo task{i}")
            return r["stdout"].strip()
    results = await asyncio.gather(*[task(i) for i in range(3)])
    check("3 个并发任务", results == ["task0", "task1", "task2"], f"({results})")

    await lc.shutdown()
    print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
    if failed: print("FAILED:", failed)

asyncio.run(main())
