"""R4: 10-thread concurrent acquire stress + reap-during-acquire + pagination check"""
import os, time, threading, logging, traceback
os.environ["E2B_API_KEY"] = "***REMOVED***"
logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
from managed_e2b import SandboxLifecycle, State

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    tag = "OK" if cond else "FAIL"
    print(f"  {tag} {name} {detail}")

DB = "/root/sb_r4.db"
if os.path.exists(DB): os.remove(DB)
# max_concurrent=4, create_rate=2/s; 10 threads
lc = SandboxLifecycle(db_path=DB, max_concurrent=4, create_rate=2.0, stale_timeout=60)

# ---- [1] 10-thread concurrent acquire stress ----
print("[1] 10-thread concurrent acquire (max_concurrent=4, rate=2/s)")
peak = {"n":0, "max":0}
lk = threading.Lock()
errors = []
done = {"n":0}
def task(i):
    try:
        with lc.acquire(template="base", timeout=120, metadata={"track":f"s{i}"}) as h:
            with lk:
                peak["n"] += 1; peak["max"] = max(peak["max"], peak["n"])
            r = h.sandbox.commands.run(f"echo t{i}")
            assert (r.stdout or "").strip() == f"t{i}", f"bad stdout {r.stdout}"
            time.sleep(0.5)
            with lk:
                peak["n"] -= 1; done["n"] += 1
    except Exception:
        errors.append((i, traceback.format_exc()))
ts = [threading.Thread(target=task, args=(i,)) for i in range(10)]
t0 = time.time()
for t in ts: t.start()
for t in ts: t.join()
elapsed = time.time() - t0
check("no thread errors", len(errors)==0, f"({len(errors)} errors)")
if errors:
    print("FIRST ERR:", errors[0][1][:500])
check("peak <= max_concurrent(4)", peak["max"] <= 4, f"(peak={peak['max']})")
check("all 10 completed", done["n"]==10, f"(done={done['n']})")
st = lc.db.stats()
check("all cleaned in db", st.get("cleaned",0)==10, f"(stats={st})")
check("no leftover running/cleaning", st.get("running",0)==0 and st.get("cleaning",0)==0, f"(stats={st})")
print(f"  elapsed={elapsed:.1f}s (10 creates @ rate=2/s => expect >=4.5s)")

# ---- [2] reap-during-acquire: does reap kill a sandbox mid-commands.run? ----
print("\n[2] reap-during-acquire (reap while threads mid-commands.run)")
DB2 = "/root/sb_r4b.db"
if os.path.exists(DB2): os.remove(DB2)
lc2 = SandboxLifecycle(db_path=DB2, max_concurrent=3, create_rate=2.0, stale_timeout=60)
killed_active = {"v": False}
reap_results = []
def long_task():
    try:
        with lc2.acquire(template="base", timeout=120, metadata={"track":"long"}) as h:
            # run a command that takes a few seconds; reap concurrently
            time.sleep(3)  # hold the slot
            alive = h.sandbox.is_running()
            out = h.sandbox.commands.run("echo midrun_ok")
            killed_active["v"] = (not alive) or ((out.stdout or "").strip() != "midrun_ok")
    except Exception as e:
        killed_active["v"] = True
        print("  long_task err:", e)

def reaper():
    time.sleep(1.0)  # wait for long_task to be mid-run
    for _ in range(3):
        r = lc2.reap()  # default purge_foreign=False
        reap_results.append(r)
        time.sleep(1)

t1 = threading.Thread(target=long_task)
t2 = threading.Thread(target=reaper)
t1.start(); t2.start()
t1.join(); t2.join()
check("reap did NOT kill active sandbox mid-run", not killed_active["v"], f"(reap={reap_results})")
check("reap reported reaped_own=0 (active heartbeat)", all(r["reaped_own"]==0 for r in reap_results), f"({reap_results})")

# ---- [3] pagination bug check: foreign discovery only sees page 1 ----
print("\n[3] reaper pagination: pg recreated each iter => only page 1 seen")
import inspect, managed_e2b as sandbox_lifecycle
src = inspect.getsource(sandbox_lifecycle.SandboxLifecycle.reap)
loop_block = src[src.index("for _it in range"):src.index("return result")]
pg_assigns_in_loop = loop_block.count("pg = Sandbox.list")
check("pg NOT reassigned in loop (#1 fixed)", pg_assigns_in_loop == 0, f"({pg_assigns_in_loop} assigns, should be 0)")
print("  => if >0, pagination re-fetches page 1 each iter; page 2+ never fetched")

print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
if failed: print("FAILED:", failed)
lc.shutdown(); lc2.shutdown()
