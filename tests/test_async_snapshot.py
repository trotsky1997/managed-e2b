"""async 版 save/fork/pause/resume/restore_from_snapshot 测试"""
import os, asyncio
os.environ["E2B_API_KEY"] = "***REMOVED***"
from managed_e2b import AsyncSandboxLifecycle

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

async def main():
    lc = AsyncSandboxLifecycle(db_path="/root/sb_async_snap.db", max_concurrent=2,
                               e2b_key="***REMOVED***")

    print("[1] save + restore_from_snapshot")
    async with lc.acquire(template="base", timeout=300) as h:
        await h.stage_in({"data.txt": "async-snap-42"})
        snap = await h.save(name="async-test-snap")
    check("save 返回 id", snap is not None, f"({snap})")
    async with lc.restore_from_snapshot(snap, timeout=120) as h2:
        out = await h2.stage_out(["data.txt"])
        check("快照恢复 state", out.get("data.txt") == b"async-snap-42")
    await lc.cleanup_snapshots()

    print("\n[2] fork (副本进状态机)")
    async with lc.acquire(template="base", timeout=300) as h:
        await h.stage_in({"f.txt": "async-forked"})
        clones = await h.fork(count=2, timeout=60)
        check("fork 2 个", len(clones) == 2)
        if clones:
            out = await clones[0].stage_out(["f.txt"])
            check("副本有 state", out.get("f.txt") == b"async-forked")
            for c in clones:
                await lc._kill_one(c.sid, c.sandbox)

    print("\n[3] pause + resume")
    async with lc.acquire(template="base", timeout=300) as h:
        await h.stage_in({"p.txt": "paused"})
        paused = await h.pause(keep_memory=True)
        check("pause True", paused is True)
        check("sqlite=paused", lc.db.get(h.sid)["state"] == "paused")
        h2 = await h.resume()
        check("resume 后 running", lc.db.get(h.sid)["state"] == "running")
        out = await h2.stage_out(["p.txt"])
        check("resume 后 state 保留", out.get("p.txt") == b"paused")

    await lc.shutdown()
    print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
    if failed: print("FAILED:", failed)

asyncio.run(main())
