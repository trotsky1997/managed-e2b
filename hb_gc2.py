import asyncio, gc, weakref, time, threading

# Stress + root-cause: what holds the to_thread Task alive?
# In 3.12, asyncio.to_thread schedules via loop.call_soon_threadsafe from a worker,
# but the Task object itself -- does anything keep a strong ref to it?

ran = 0
completed = 0
finalized_seen = 0

class DB:
    def heartbeat(self, sid):
        global ran, completed
        ran += 1
        time.sleep(0.02)
        completed += 1
        return sid

class LC:
    def __init__(self): self.db = DB()

async def main():
    global finalized_seen
    lc = LC()
    loop = asyncio.get_running_loop()
    t = loop.create_task(asyncio.to_thread(lc.db.heartbeat, "s"))
    wref = weakref.ref(t, lambda w: globals().__setitem__("finalized_seen", globals()["finalized_seen"]+1))
    del t
    # Force a full GC cycle immediately after creation, before task runs
    for _ in range(5):
        gc.collect()
    alive = wref() is not None
    # Also check all_tasks set
    at = asyncio.all_tasks()
    print("alive_after_del+gc=%s in_all_tasks=%s n_all_tasks=%d"
          % (alive, wref() in at, len(at)))
    await asyncio.sleep(0.2)
    print("ran=%d completed=%d finalized=%d" % (ran, completed, finalized_seen))

asyncio.run(main())
