"""A3-1 experiment: does cancelling the body task detach the shielded _kill_one?

Exp1: single cancel while body awaits h.run()  -> does kill run inline or detach?
Exp2: a SECOND cancel while the shielded kill is mid-flight -> does it detach?
"""
import asyncio
import os
import sys

os.environ.setdefault("E2B_API_KEY", "***REMOVED***")
sys.path.insert(0, "/root")

from managed_e2b.async_core import AsyncSandboxLifecycle  # noqa: E402


def make_slow_kill(lc, events, loop, delay):
    orig_kill = lc._kill_one

    async def slow_kill(sid, sandbox_obj=None, force=False):
        events.append(("kill_start", round(loop.time(), 2)))
        await asyncio.sleep(delay)          # simulate a slow kill RPC
        events.append(("kill_pre_real", round(loop.time(), 2)))
        try:
            r = await orig_kill(sid, sandbox_obj, force)
        except BaseException as e:          # noqa: BLE001
            events.append(("kill_real_exc", round(loop.time(), 2), type(e).__name__))
            r = False
        events.append(("kill_done", round(loop.time(), 2), r))
        return r

    return slow_kill


async def exp1():
    print("\n===== EXP1: single cancel during body =====")
    lc = AsyncSandboxLifecycle(db_path="/tmp/a3_exp1.db", max_concurrent=4, stale_timeout=600)
    events = []
    loop = asyncio.get_running_loop()
    lc._kill_one = make_slow_kill(lc, events, loop, 2.0)

    async def body():
        async with lc.acquire(template="base", timeout=60) as h:
            await h.run("sleep 10", timeout=20)
            return "ok"

    task = asyncio.create_task(body())
    await asyncio.sleep(3)                 # inside body sleep
    events.append(("cancel1", round(loop.time(), 2)))
    task.cancel()
    try:
        await task
    except BaseException as e:             # noqa: BLE001
        events.append(("body_raised", round(loop.time(), 2), type(e).__name__))
    await asyncio.sleep(4)
    events.append(("after_wait", round(loop.time(), 2)))
    print("EVENTS:")
    for e in events:
        print("   ", e)
    print("inflight still set?:", lc._inflight)
    await lc.shutdown()


async def exp2():
    print("\n===== EXP2: second cancel DURING shielded kill =====")
    lc = AsyncSandboxLifecycle(db_path="/tmp/a3_exp2.db", max_concurrent=4, stale_timeout=600)
    events = []
    loop = asyncio.get_running_loop()
    kill_started = asyncio.Event()
    orig_kill = lc._kill_one

    async def slow_kill(sid, sandbox_obj=None, force=False):
        events.append(("kill_start", round(loop.time(), 2)))
        kill_started.set()
        await asyncio.sleep(3.0)            # slow kill RPC, mid-flight
        events.append(("kill_pre_real", round(loop.time(), 2)))
        try:
            r = await orig_kill(sid, sandbox_obj, force)
        except BaseException as e:          # noqa: BLE001
            events.append(("kill_real_exc", round(loop.time(), 2), type(e).__name__))
            r = False
        events.append(("kill_done", round(loop.time(), 2), r))
        return r

    lc._kill_one = slow_kill

    async def body():
        async with lc.acquire(template="base", timeout=60) as h:
            await h.run("sleep 10", timeout=20)
            return "ok"

    task = asyncio.create_task(body())
    await asyncio.sleep(3)                 # inside body
    events.append(("cancel1", round(loop.time(), 2)))
    task.cancel()                          # 1st cancel -> finally -> shield(slow_kill)
    await kill_started.wait()              # until kill begins
    events.append(("cancel2", round(loop.time(), 2)))
    task.cancel()                          # 2nd cancel DURING shielded kill
    try:
        await task
    except BaseException as e:             # noqa: BLE001
        events.append(("body_raised", round(loop.time(), 2), type(e).__name__))
    await asyncio.sleep(5)                 # let a detached kill finish
    events.append(("after_wait", round(loop.time(), 2)))
    print("EVENTS:")
    for e in events:
        print("   ", e)
    print("inflight still set?:", lc._inflight)
    await lc.shutdown()


async def main():
    await exp1()
    await exp2()


if __name__ == "__main__":
    asyncio.run(main())
