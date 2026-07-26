import asyncio, sys, time
sys.path.insert(0, "/root/managed_e2b")
from _async import AsyncHeartbeat

calls = {"n": 0}
class FakeDB:
    def heartbeat(self, sid):
        calls["n"] += 1
        time.sleep(0.01)
        return sid
class FakeLC:
    def __init__(self): self.db = FakeDB(); self._stale_timeout = 600

async def main():
    lc = FakeLC()
    hb = AsyncHeartbeat(lc, "sid-real", stale_timeout=600)
    hb.start()
    # immediate heartbeat task: return value discarded in start()
    # verify it still ran
    await asyncio.sleep(0.2)
    print("immediate_heartbeat_calls >=1:", calls["n"] >= 1, "calls=", calls["n"])
    await hb.stop()

asyncio.run(main())
