import asyncio, gc, weakref, time

# Faithful re-implementation of AsyncHeartbeat.start() immediate-heartbeat line:
#   loop.create_task(asyncio.to_thread(self._lc.db.heartbeat, self._sid))
# with the return value discarded (NOT stored on self).

ran = 0
completed = 0
dropped_early = 0

class DB:
    def heartbeat(self, sid):
        global ran, completed
        ran += 1
        time.sleep(0.05)
        completed += 1
        return sid

class LC:
    def __init__(self): self.db = DB()

class Heartbeat:
    def __init__(self, lc, sid):
        self._lc = lc; self._sid = sid
        self._task = None
    def start(self):
        loop = asyncio.get_running_loop()
        # exact pattern: return value discarded
        t = loop.create_task(asyncio.to_thread(self._lc.db.heartbeat, self._sid))
        self._wref = weakref.ref(t)   # only for observation; NOT a strong ref
        del t
        self._task = loop.create_task(self._loop())
    async def _loop(self):
        await asyncio.sleep(999)

async def main():
    lc = LC()
    hb = Heartbeat(lc, "sid-A")
    hb.start()
    alive_before = hb._wref() is not None
    gc.collect()
    await asyncio.sleep(0.3)
    gc.collect()
    await asyncio.sleep(0.1)
    alive_after = hb._wref() is not None
    hb._task.cancel()
    try: await hb._task
    except: pass
    print("alive_before_run=%s alive_after_run=%s ran=%d completed=%d"
          % (alive_before, alive_after, ran, completed))

asyncio.run(main())
