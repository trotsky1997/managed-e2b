"""验证 async reap/reconcile 不再崩 (await Sandbox.list 修复) + acquire 正常"""
import os, asyncio
os.environ["E2B_API_KEY"] = "***REMOVED***"
from managed_e2b import AsyncSandboxLifecycle

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

async def main():
    lc = AsyncSandboxLifecycle(db_path="/root/sb_async_reap.db", max_concurrent=2,
                               e2b_key="***REMOVED***")

    print("[1] acquire 正常 (修复没破坏)")
    async with lc.acquire(template="base", timeout=120) as h:
        r = await h.run("echo ok")
        check("acquire+run", r["stdout"].strip() == "ok")
    check("cleaned", lc.db.get(h.sid)["state"] == "cleaned")

    print("\n[2] reap 不崩 (之前 await Sandbox.list TypeError)")
    r = await lc.reap()
    check("reap 返回 dict", isinstance(r, dict), f"({r})")

    print("\n[3] reconcile 不崩")
    r = await lc.reconcile()
    check("reconcile 返回 dict", isinstance(r, dict), f"({r})")

    print("\n[4] 并发 acquire + 期间 reap")
    async def task(i):
        async with lc.acquire(template="base", timeout=120) as h:
            await h.run(f"echo t{i}")
            return h.sid
    sids = await asyncio.gather(*[task(i) for i in range(2)])
    await lc.reap()  # 期间并发
    check("2 个并发完成", len(sids) == 2)

    await lc.shutdown()
    print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
    if failed: print("FAILED:", failed)

asyncio.run(main())
