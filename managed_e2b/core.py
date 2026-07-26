"""
E2B 沙箱生命周期管理器
======================
状态机(sqlite 落盘的状态):
  RUNNING → CLEANING → CLEANED
  (PREWARMING/READY/CREATING 是瞬时态, 不单独落盘; 失败直接抛异常, 不留 PREWARM_FAIL/CREATE_FAIL 行)
  GRACEFUL_QUIT/TIMEOUT 由 E2B 的 create(timeout=) 自动 kill 触发, 不经本状态机转移。

设计要点:
- 一个沙箱在 sqlite 里只有三态: RUNNING(活跃/持有) → CLEANING(kill 中) → CLEANED(已清)。
- 心跳: acquire 期间后台线程刷新 last_heartbeat; reap/shutdown 只清"无心跳"的 RUNNING(崩溃残留)。
- prewarm(template build) 不落盘: 靠 Template.exists + 进程内 _template_ready 去重。

铁律(实测诊断 + API 查证):
1. Sandbox.list() 在官方 e2b 2.34.0 上已 kill 沙箱立即消失; 但保守起见仍以 sqlite 为真相源,
   list 只当线索, 不靠"列表清空"判断。
   清理器绝不能"循环 list 直到空",否则会无脑重复 kill 同一批已死沙箱。
2. 去重落盘: 每个 sandbox_id 只 kill 一次,kill 完立即写 sqlite CLEANED,再次出现直接跳过。
3. 上限退出: 任何巡检循环有 max_iterations,到顶就停,防止空转。

孤儿策略: 保守 —— 只清 sqlite 里自己追踪的(超时/僵尸态);不在 sqlite 的一律不碰(可能是别人的)。
真相源是 sqlite,list 只当线索。
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
import atexit
import hashlib
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Optional

logger = logging.getLogger("sandbox_lifecycle")


class State(str, Enum):
    QUEUED = "queued"
    PREWARMING = "prewarming"
    READY = "ready"
    CREATING = "creating"
    RUNNING = "running"
    GRACEFUL_QUIT = "graceful_quit"
    TIMEOUT = "timeout"
    CLEANING = "cleaning"
    CLEANED = "cleaned"
    PREWARM_FAIL = "prewarm_fail"
    CREATE_FAIL = "create_fail"

    # 是否为终态
    @property
    def terminal(self) -> bool:
        return self in (State.CLEANED, State.PREWARM_FAIL, State.CREATE_FAIL)


# ---------------- sqlite 真相源 ----------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sandboxes (
    sandbox_id    TEXT PRIMARY KEY,
    template      TEXT,
    state         TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    running_since INTEGER,
    killed_at     INTEGER,
    metadata      TEXT,
    last_error    TEXT,
    last_heartbeat INTEGER
);
CREATE INDEX IF NOT EXISTS idx_state ON sandboxes(state);
"""

# schema 迁移: 旧 db 没有 last_heartbeat 列时补上
_MIGRATIONS = [
    "ALTER TABLE sandboxes ADD COLUMN last_heartbeat INTEGER",
]


class SandboxDB:
    """sqlite 追踪层 —— 唯一真相源。线程安全(每连接一线程 + lock)。"""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, timeout=30)
        self._conn.row_factory = sqlite3.Row
        # WAL + busy_timeout=30s: 防并发写 "database is locked" 崩溃(#3)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.executescript(_SCHEMA)
        # 迁移: 给旧 db 补 last_heartbeat 列 (已存在则忽略)
        for sql in _MIGRATIONS:
            try:
                self._conn.execute(sql)
            except sqlite3.OperationalError:
                pass  # 列已存在
        self._conn.commit()

    @contextmanager
    def _tx(self):
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def upsert(self, sid: str, **fields) -> None:
        with self._tx() as c:
            row = c.execute("SELECT 1 FROM sandboxes WHERE sandbox_id=?", (sid,)).fetchone()
            if row:
                sets = ", ".join(f"{k}=:{k}" for k in fields)
                fields["sandbox_id"] = sid
                c.execute(f"UPDATE sandboxes SET {sets} WHERE sandbox_id=:sandbox_id", fields)
            else:
                fields["sandbox_id"] = sid
                cols = ", ".join(fields.keys())
                ph = ", ".join(f":{k}" for k in fields)
                c.execute(f"INSERT INTO sandboxes ({cols}) VALUES ({ph})", fields)

    def set_state(self, sid: str, state: State, error: Optional[str] = None) -> None:
        extra = {}
        if state == State.RUNNING:
            now = int(time.time())
            extra["running_since"] = now
            extra["last_heartbeat"] = now  # 进 RUNNING 时初始化心跳
        if state in (State.CLEANING, State.GRACEFUL_QUIT, State.TIMEOUT):
            extra["killed_at"] = int(time.time())
        if error is not None:
            extra["last_error"] = error[:500]
        self.upsert(sid, state=state.value, **extra)

    def heartbeat(self, sid: str) -> None:
        """刷新沙箱心跳, 证明它仍被本进程活跃持有。
        条件更新: 只在 state=RUNNING 时刷, 避免心跳线程(stop 不 join)迟写
        污染已 CLEANED/CLEANING 的行(P3-1 守卫)。"""
        with self._lock:
            self._conn.execute(
                "UPDATE sandboxes SET last_heartbeat=? WHERE sandbox_id=? AND state=?",
                (int(time.time()), sid, State.RUNNING.value),
            )
            self._conn.commit()

    def try_claim_for_kill(self, sid: str) -> bool:
        """原子抢占 kill 所有权: 若状态非 CLEANED 则改成 CLEANING 并返回 True;
        已 CLEANED 则改 0 行返回 False。消除 get+set 间的竞态(状态倒退)。"""
        with self._lock:
            # 只排除 CLEANED(已清终态)。CLEANING 允许重入: 崩溃残留的 CLEANING 行
            # (kill 到一半) 需要 reap 重入完成 kill。并发重复 kill 幂等(kill 返回 False),
            # 不致命; 真正要防的是 CLEANED→CLEANING 状态倒退(已死被当活)。
            cur = self._conn.execute(
                "UPDATE sandboxes SET state=?, killed_at=? "
                "WHERE sandbox_id=? AND state != ?",
                (State.CLEANING.value, int(time.time()), sid, State.CLEANED.value),
            )
            return cur.rowcount > 0

    def list_stale_running(self, max_age: int) -> list[sqlite3.Row]:
        """返回 RUNNING 且超过 max_age 秒无心跳的行 —— 崩溃残留的孤儿。
        活跃任务会持续刷新心跳, 不会进这里; 进这里的说明持有它的进程已死。"""
        cutoff = int(time.time()) - max_age
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM sandboxes WHERE state=? AND "
                "(last_heartbeat IS NULL OR last_heartbeat < ?)",
                (State.RUNNING.value, cutoff),
            ).fetchall()

    def get(self, sid: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute("SELECT * FROM sandboxes WHERE sandbox_id=?", (sid,)).fetchone()

    def list_state(self, state: State) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute("SELECT * FROM sandboxes WHERE state=?", (state.value,)).fetchall()

    def all_tracked(self) -> set[str]:
        """所有被追踪的 sandbox_id —— 用于孤儿巡检去重。"""
        with self._lock:
            return {r["sandbox_id"] for r in self._conn.execute("SELECT sandbox_id FROM sandboxes").fetchall()}

    def stats(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT state, COUNT(*) n FROM sandboxes GROUP BY state").fetchall()
            return {r["state"]: r["n"] for r in rows}

    def close(self):
        with self._lock:
            self._conn.close()


# ---------------- 限流器 ----------------

class Limiter:
    """并发上限 + 队列。超过 max_concurrent 的 create 请求排队等待。"""

    def __init__(self, max_concurrent: int):
        self._sem = threading.Semaphore(max_concurrent)
        self.max = max_concurrent

    @contextmanager
    def slot(self):
        self._sem.acquire()
        try:
            yield
        finally:
            self._sem.release()


# ---------------- 速率限流 (token bucket) ----------------

class RateLimiter:
    """按速率限流: 调用之间至少间隔 1/rate 秒, 防突破 E2B 创建速率。
    用于 sandbox create (E2B Hobby 创建速率 1/s)。并发调用串行等待, 不积压。"""

    def __init__(self, rate: float):
        self._min_interval = 1.0 / rate if rate > 0 else 0
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    @contextmanager
    def slot(self):
        with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self._next_allowed = max(now, self._next_allowed) + self._min_interval
        yield


# ---------------- 生命周期管理器 ----------------

@dataclass
class SandboxHandle:
    sid: str
    sandbox: object  # e2b_code_interpreter.Sandbox 实例
    template: str


class _Heartbeat:
    """后台心跳线程: 定期刷新沙箱的 last_heartbeat, 证明它仍被活跃持有。
    进程崩溃 → 心跳停 → reap 识别孤儿。daemon=True, 进程退出时不阻塞。"""

    def __init__(self, lifecycle: "SandboxLifecycle", sid: str, stale_timeout: int):
        self._lc = lifecycle
        self._sid = sid
        # 钳制: interval 必须 < stale_timeout, 否则活跃沙箱会被 list_stale_running
        # 误判 stale 而 reap 误杀(#4)。保证一个 stale 窗口内至少 2 次心跳。
        # 严格保证 interval < stale_timeout(否则活跃沙箱被误判 stale 误杀, #4)。
        # stale//4 作基准, 上限 stale-1(严格小于), 下限 1。
        iv = stale_timeout // 4
        self._interval = max(min(iv, stale_timeout - 1), 1)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self):
        self._lc.db.heartbeat(self._sid)  # 立即刷一次
        self._thread.start()

    def stop(self):
        self._stop.set()
        # 不 join: daemon 线程, stop 后下一轮自然退出; join 会阻塞 acquire 退出

    def _loop(self):
        while not self._stop.wait(self._interval):
            try:
                self._lc.db.heartbeat(self._sid)
            except Exception as e:
                logger.warning(f"heartbeat {self._sid} 失败(非致命): {e}")


class SandboxLifecycle:
    """
    管理一批 E2B 沙箱的完整生命周期。一个实例 = 一条评测任务。

    用法:
        lc = SandboxLifecycle(db_path="/root/sandbox_track.db", max_concurrent=4)
        with lc.acquire(template="base", timeout=300) as h:
            h.sandbox.commands.run("...")
        # 退出 with 即 graceful kill + clean
    """

    def __init__(
        self,
        db_path: str,
        max_concurrent: int = 4,
        e2b_key: Optional[str] = None,
        # 巡检参数: list 不可靠,用保守上限
        reaper_max_iter: int = 5,
        reaper_list_limit: int = 100,
        # RUNNING 态判孤儿的心跳超时: 超过此值无心跳 = 持有进程已崩溃。
        # 默认 600s。注意: stale_timeout 只管 reap 检测延迟(无心跳多久判孤儿),
        # 不 bound orphan bill —— E2B 在 create(timeout=) 到点杀沙箱(默认 1800s)。
        # 建议 stale_timeout 设为评测任务时长的 1/3, 让 reap 及时清崩溃残留。
        stale_timeout: int = 600,
        # 三种并发数独立控制 (不同资源, 不同约束):
        max_build_concurrency: int = 4,    # template build 并发 (E2B ~20, 留余量)
        create_rate: float = 1.0,  # sandbox create 速率 (次/秒); E2B Hobby=1/s, Pro=5/s
        # max_concurrent 控制同时 RUNNING 的沙箱数 (评测并发度, 用户主控)
        e2b_api_url: Optional[str] = None,  # E2B 端点; 不传则用环境变量/官方默认
    ):
        # 端点 + key 都下沉到 env (SDK 每次调用读 os.getenv):
        # 显式传入 > 环境变量 > SDK 默认。火山方舟环境必须设, 否则连错端点/key。
        if e2b_api_url:
            os.environ["E2B_API_URL"] = e2b_api_url
        # e2b_key 也必须下沉 env: SDK 从 os.getenv("E2B_API_KEY") 读, self._key 不传给任何调用。
        # 否则 "key 只走构造参数、不预置 env" 会 AuthenticationException。
        if e2b_key:
            os.environ["E2B_API_KEY"] = e2b_key
        self.db = SandboxDB(db_path)
        # 三个独立 limiter: 资源不同, 不能混用一把锁
        self._build_limiter = Limiter(max_build_concurrency)
        self._create_limiter = RateLimiter(create_rate)  # 按创建速率限流(#7)
        self._run_limiter = Limiter(max_concurrent)  # 同时 RUNNING 的沙箱
        self._key = e2b_key or os.environ.get("E2B_API_KEY")
        if not self._key:
            raise RuntimeError("E2B_API_KEY 未设置")
        self.reaper_max_iter = reaper_max_iter
        self.reaper_list_limit = reaper_list_limit
        if stale_timeout < 10:
            raise ValueError(f"stale_timeout={stale_timeout} 太小: 心跳 interval 会接近/超过它导致活跃沙箱被误杀(#4). 至少 10s")
        self._stale_timeout = stale_timeout
        self._reaper_lock = threading.Lock()
        # atexit 必须在构造时注册: 只用 acquire 不调 reap 的进程
        # 退出时也要清理 RUNNING 沙箱, 否则就是"孤儿进程危机"复现。
        atexit.register(self.shutdown)
        # template 预热: 去重 + 并发 build 锁
        self._template_ready: dict[str, bool] = {}      # name -> 已就绪
        self._template_locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()
        self._inflight: set[str] = set()  # 当前进程持有中的 sandbox_id(#4 shutdown 兜底)
        self._inflight_lock = threading.Lock()

    # ---- 导入 E2B (懒加载, 避免无 key 时 import 失败) ----
    def _sandbox_cls(self):
        from e2b_code_interpreter import Sandbox
        return Sandbox

    def _connect(self, sid: str):
        """凭 sandbox_id 连回沙箱(用于孤儿清理: 只有 id 没有对象)。"""
        Sandbox = self._sandbox_cls()
        return Sandbox.connect(sid)

    # ---- prewarm: build template if need + 去重 ----
    def template_name_for(self, image: str = None, dockerfile: str = None) -> str:
        """输入(image url 或 dockerfile 内容) → 确定性 template 名。
        去重基础: 同输入 → 同名, 不同输入 → 不同名。"""
        src = image or dockerfile
        h = hashlib.sha1(src.encode()).hexdigest()[:12]
        prefix = "img-" if image else "df-"
        return prefix + h

    def prewarm(self, image: str = None, dockerfile: str = None, *,
                cpu_count: int = 2, memory_mb: int = 1024) -> str:
        """预热: 若该输入的 template 还没 build, 就 build; 已存在则直接复用。

        两种输入:
        - image: Docker Hub/registry 镜像 url (走 from_image)
        - dockerfile: Dockerfile 内容字符串 (走 from_dockerfile)

        去重两层:
        - 内容去重: 同输入 → 同 template 名 (template_name_for), 不重复 build。
        - 并发去重: 多任务同时 prewarm 同一输入, 锁保证只 build 一次, 其余等。
        返回 template 名, 供 _create 用。
        """
        if not image and not dockerfile:
            raise ValueError("prewarm 需要 image 或 dockerfile")
        name = self.template_name_for(image, dockerfile)
        # 内存级快路径: 本进程已确认就绪, 直接返回 (省一次 exists RPC)
        if self._template_ready.get(name):
            return name
        lock = self._template_lock(name)
        with lock:  # 同一 template 的 build 串行, 不同 template 不互斥
            # double-check: 拿到锁后可能已被别的线程 build 好
            if self._template_ready.get(name):
                return name
            from e2b import Template
            # #2 修复(v2): exists() 只查 alias 在不在, 不查 build 成功。
            # build_in_background 是异步的, 单次 get_build_status 必返回 BUILDING(v1 死代码)。
            # 正确: exists 命中 → build_in_background(同 name) 会复用已有 build, 轮询到终态;
            #       READY→缓存, ERROR→换名重建; 不存在→build。
            builder = Template().from_image(image) if image else Template().from_dockerfile(dockerfile)
            name_to_use = name

            with self._build_limiter.slot():
                ready3 = self._template_ready.get(name_to_use)
                if ready3:
                    return name_to_use
                alias_exists = False
                try:
                    alias_exists = Template.exists(name_to_use)
                except Exception as e:
                    logger.warning(f"exists({name_to_use}) 失败, 当作不存在: {e}")

                if alias_exists:
                    # 轮询已有 build 到终态(build_in_background 复用同 name 的进行中/已完成 build)
                    sval = self._poll_template_status(Template, builder, name_to_use)
                    if sval == "ready":
                        self._template_ready[name_to_use] = True
                        logger.info(f"template {name_to_use} 已就绪(复用)")
                        return name_to_use
                    # ERROR/损坏: 换名重建
                    name_to_use = name + "-r" + str(int(time.time()))[-5:]
                    logger.warning(f"template {name} 状态={sval}, 换名 {name_to_use} 重建")

                last_err = None
                for attempt in range(2):
                    try:
                        self._build_with_timeout(builder, name_to_use, cpu_count, memory_mb)
                        self._template_ready[name_to_use] = True
                        # R5-2: 规范名也指向已建的 renamed template, 重试同输入时走快路径不重建
                        if name_to_use != name:
                            self._template_ready[name] = name_to_use
                        logger.info(f"template {name_to_use} build 完成")
                        return name_to_use
                    except Exception as e:
                        last_err = e
                        logger.warning(f"template {name_to_use} build 第{attempt+1}次失败: {e}")
                raise RuntimeError(f"prewarm build {name_to_use} 失败: {last_err}") from last_err

    def _poll_template_status(self, Template, builder, name: str, timeout: int = 300) -> str:
        """轮询 template build 到终态(ready/error)。build_in_background 复用同 name 的 build。"""
        deadline = time.time() + timeout
        try:
            info = Template.build_in_background(builder, name)
        except Exception as e:
            # 防御: 2.34.0 上 build_in_background 对已存在 alias 返回 202+BuildInfo 不抛;
            # 若未来版本抛 400("already"/"not in waiting"), 视为已 ready。
            msg = str(e).lower()
            if "ready" in msg or "not in waiting" in msg or "already" in msg:
                return "ready"
            return "error"
        while time.time() < deadline:
            try:
                status = Template.get_build_status(info)
                sval = str(getattr(status, "status", status)).lower()
                if "ready" in sval:
                    return "ready"
                if "error" in sval or "fail" in sval:
                    return "error"
            except Exception:
                pass
            time.sleep(3)
        return "timeout"

    def _build_with_timeout(self, builder, name: str, cpu_count: int, memory_mb: int,
                            timeout: int = 600) -> None:
        """带硬超时的 build: build_in_background 提交 + get_build_status 轮询。
        防止 Template.build() RPC 挂起 → 永久占 build slot + 模板锁(P2)。
        超时则抛 TimeoutError, 调用方重试或放弃, slot/锁正常释放。"""
        from e2b import Template
        info = Template.build_in_background(builder, name, cpu_count=cpu_count, memory_mb=memory_mb)
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = Template.get_build_status(info)
            # status 可能是字符串或对象, 兼容处理
            s = str(getattr(status, "status", status)).lower()
            if "success" in s or "ready" in s:
                return
            if "error" in s or "fail" in s:
                raise RuntimeError(f"template {name} build 失败: {s}")
            time.sleep(3)
        raise TimeoutError(f"template {name} build 超时 ({timeout}s)")

    # 每个 template 一把锁 (按 name 分桶), 避免全局串行
    def _template_lock(self, name: str):
        with self._locks_guard:
            if name not in self._template_locks:
                self._template_locks[name] = threading.Lock()
            return self._template_locks[name]

    def cleanup_templates(self) -> dict:
        """P7: template 累积是已知限制。
        E2B SDK 2.34.0 无 list-templates / Template.remove API, 故无法遍历删除。
        本方法返回本进程 build 过的 template 名(供审计/手动清理), 删除需在 E2B 控制台做。
        长期累积到配额上限会 build 失败 —— 建议: 评测用固定少量镜像, template 数自然有界。"""
        return {"built_templates": sorted(self._template_ready.keys()),
                "note": "E2B SDK 无 template 删除 API, 需控制台清理"}

    # ---- create ----
    def _create(self, template: str, timeout: int, metadata: dict) -> SandboxHandle:
        """create 沙箱并落盘。关键: 先 create 拿到真实 id, 再写 db;
        若 create 成功但写 db 失败, 仍要 kill 沙箱避免孤儿(见 acquire 的 except)。"""
        Sandbox = self._sandbox_cls()
        sbx = Sandbox.create(template=template, timeout=timeout, metadata=metadata)
        real_id = sbx.sandbox_id
        # 直接用真实 id 落盘 (不要临时 id + rename, 那套在并发下易竞态/漏字段)
        try:
            self.db.upsert(
                real_id,
                state=State.RUNNING.value,
                template=template,
                created_at=int(time.time()),
                metadata=str(metadata),
            )
        except Exception as e:
            # db 写失败但沙箱已建 → 必须 kill, 否则真孤儿
            logger.error(f"db 写入失败, 回收沙箱 {real_id}: {e}")
            try:
                sbx.kill()
            except Exception:
                pass
            raise
        return SandboxHandle(sid=real_id, sandbox=sbx, template=template)

    # ---- graceful kill + clean (核心: 用 is_running 确认, 不信 list) ----
    def _kill_one(self, sid: str, sandbox_obj: object = None, force: bool = False) -> bool:
        """杀一个沙箱并确认死透。返回是否真的 kill 了(去重:已 CLEANED 的不重复杀)。

        原子抢占: 用单条 SQL "WHERE state != CLEANED" 一次性把状态改成 CLEANING,
        若已被标 CLEANED 则改 0 行 → 返回 False。避免 get+set 两步间的竞态导致
        CLEANED→CLEANING 状态倒退(已死沙箱被重新 connect/kill)。
        真死确认: kill 后用 is_running() 复核; 不达标不标 CLEANED, 留 CLEANING 等下轮。
        """
        # 原子抢占: 非 CLEANED → CLEANING, 返回是否抢到
        # force=True (外部 sid, 不在 DB): 绕过 DB 门控, 直接 kill + 落盘(#2 修复 purge_foreign)
        if not force:
            if not self.db.try_claim_for_kill(sid):
                return False  # 已 CLEANED, 跳过
        try:
            if sandbox_obj is not None:
                sandbox_obj.kill()
            else:
                # 只凭 id: connect 回去再 kill (SandboxApi 路径在 2.34.0 不存在)
                self._connect(sid).kill()
        except Exception as e:
            logger.warning(f"kill {sid} 异常(可能已死): {e}")

        # 真死确认: kill() 不保证沙箱立刻消失, 用 is_running 复核
        confirmed_dead = self._confirm_dead(sid, sandbox_obj)
        if confirmed_dead:
            if force:
                # 外部 sid 不在 DB: 补全 NOT NULL 字段后落盘 CLEANED
                self.db.upsert(sid, state=State.CLEANED.value, template="(foreign)",
                               created_at=int(time.time()), killed_at=int(time.time()))
            else:
                self.db.set_state(sid, State.CLEANED)
        else:
            # 没死透: 保留 CLEANING 状态, 下轮 reap 重试(不标 CLEANED, 防假清)
            logger.warning(f"kill {sid} 后 is_running 仍 True, 留 CLEANING 待重试")
        return confirmed_dead

    def _confirm_dead(self, sid: str, sandbox_obj: object = None) -> bool:
        """确认沙箱已死。kill 后 is_running 可能有短暂延迟, 重试几次。

        异常区分(P1-3):
        - NotFound/SandboxNotFound 类异常 → 沙箱不存在 = 已死, 视为死。
        - 网络错误/超时/5xx → 不能确定, continue 重试, 不误判(避免假 CLEANED)。
        进程崩溃后重启: 残留 CLEANING 态沙箱多半已被 E2B timeout 杀,
        connect 已死 id 抛 SandboxNotFoundException → 视为已死 → 标 CLEANED, 自愈。
        """
        Sandbox = self._sandbox_cls()
        for _ in range(3):
            try:
                if sandbox_obj is not None:
                    if not sandbox_obj.is_running():
                        return True
                else:
                    if not self._connect(sid).is_running():
                        return True
            except Exception as e:
                en = type(e).__name__
                msg = str(e).lower()
                # 沙箱不存在类异常 → 已死
                if "notfound" in en.lower() or "not found" in msg or "sandboxnotfound" in en.lower():
                    return True
                # 网络/超时/5xx → 不确定, 重试
                logger.warning(f"_confirm_dead {sid} 瞬时错误(重试): {en}: {str(e)[:80]}")
            time.sleep(0.5)
        return False

    # ---- 上下文管理器: 完整生命周期 ----
    @contextmanager
    def acquire(self, image: Optional[str] = None, dockerfile: Optional[str] = None,
                template: Optional[str] = None, timeout: int = 1800,  # #6: 默认1800s(评测常>5min); E2B到点自动kill
                metadata: Optional[dict] = None):
        # ⚠️ timeout 是硬上限(#1 文档化): create(timeout=) 到点 E2B 自动 kill 沙箱 (on_timeout=kill)。
        # 本状态机不在心跳里 set_timeout 续命 (保持简单)。调用方必须传一个超过最坏任务时长的值,
        # 否则长任务会被 E2B 在执行中途 kill。默认 300s 只够短任务。
        # 若沙箱已被 E2B 超时杀, _kill_one 的 kill() 返回 False、_confirm_dead 视为已死, 正常收敛。
        """获取一个沙箱,用完自动 graceful kill + clean。

        三种并发数独立:
        - build: prewarm 的 template build (max_build_concurrency)
        - create: Sandbox.create 速率 (create_rate, 默认 1/s = E2B Hobby)
        - run: 沙箱 RUNNING 期间 (max_concurrent, 评测并发度)

        三种输入(按优先级):
        - image: Docker 镜像 url, 走 prewarm (build if need + 去重)。
        - dockerfile: Dockerfile 内容字符串, 同样走 prewarm。
        - template: 直接用现成 template 名 (如 "base"), 不 build。
        全不传则用 "base"。
        """
        if not image and not dockerfile and not template:
            template = "base"
        md = metadata or {}
        md.setdefault("managed_by", "sandbox_lifecycle")

        h: Optional[SandboxHandle] = None
        # prewarm (build if need) 在 run_limiter 之外: build 不占沙箱额度
        if image or dockerfile:
            template = self.prewarm(image=image, dockerfile=dockerfile)
        # create + yield + kill 全在一个 try: 任何中断(含 KeyboardInterrupt)
        # finally 都能拿到 h 并清理, 杜绝"create 成功但 h 未赋值就中断"的泄漏。
        try:
            with self._create_limiter.slot():
                h = self._create(template, timeout, md)
            with self._inflight_lock:
                self._inflight.add(h.sid)
            # 心跳: 后台线程定期刷新 last_heartbeat, 证明沙箱仍被本进程活跃持有。
            # 进程崩溃 → 心跳停 → reap 凭"超 stale_timeout 无心跳"识别孤儿并清。
            # 活跃长任务(合法跑 >1h)心跳持续刷新, 不会被 reap 误杀。
            hb = _Heartbeat(self, h.sid, self._stale_timeout)
            hb.start()
            try:
                with self._run_limiter.slot():  # RUNNING 用 run_limiter (评测并发度)
                    yield h
            finally:
                hb.stop()
        finally:
            if h is not None:
                self._kill_one(h.sid, h.sandbox)
                with self._inflight_lock:
                    self._inflight.discard(h.sid)

    # ---- 启动时对账: 清崩溃残留 (#5) ----
    def reconcile(self) -> dict:
        """启动时调用: 对账 sqlite 的 RUNNING/CLEANING 行与 E2B 实际沙箱。
        sqlite 有记录但 E2B 已不存在的(进程崩溃被 E2B timeout 杀) → 标 CLEANED。
        解决: 进程崩溃后用新 db_path 启动时, 旧 db 的 RUNNING 残留无法被 reap 清(#5)。
        要求: db_path 跨运行稳定复用, 否则旧记录对不上。"""
        result = {"reconciled": 0, "list_failed": False}
        Sandbox = self._sandbox_cls()
        from e2b.sandbox.sandbox_api import SandboxQuery
        from e2b.api.client.models.sandbox_state import SandboxState
        # 分页收集所有 live sid(复用 reap 的翻页模式), 修复单页 false-death(#2)
        live = set()
        try:
            pg = Sandbox.list(query=SandboxQuery(state=[SandboxState.RUNNING, SandboxState.PAUSED]),
                              limit=self.reaper_list_limit)
            for _ in range(self.reaper_max_iter):
                box = {"items": [], "err": None}
                def _f():
                    try: box["items"] = pg.next_items() or []
                    except Exception as e: box["err"] = str(e)
                t = threading.Thread(target=_f, daemon=True); t.start(); t.join(10)
                if box.get("err") or t.is_alive():
                    # list 失败: 短路不清, 防止 live=空→全部误标 CLEANED(#3 静默数据丢失)
                    result["list_failed"] = True
                    logger.warning("reconcile list 失败, 跳过清理(避免误清)")
                    return result
                live.update(getattr(s, "sandbox_id", getattr(s, "id", None)) for s in box["items"])
                if not pg.has_next:
                    break
        except Exception as e:
            result["list_failed"] = True
            logger.warning(f"reconcile list 异常, 跳过: {e}")
            return result
        # R5-1 修复: 不对"sqlite有E2B无"的 RUNNING 直接标 CLEANED(不kill) ——
        # 那会与并发 acquire 竞态: acquire-finally 见行已CLEANED跳过kill → 沙箱留E2B泄漏。
        # 改为只清 stale 的 RUNNING(像 reap, 无心跳=崩溃残留, E2B已timeout杀, 标CLEANED即可);
        # 活跃的(心跳新)不碰, 由 acquire finally 自己 kill。
        # CLEANING 态(kill到一半的残留)直接标 CLEANED(E2B已杀)。
        for row in self.db.list_stale_running(self._stale_timeout):
            sid = row["sandbox_id"]
            self.db.set_state(sid, State.CLEANED)
            result["reconciled"] += 1
        for row in self.db.list_state(State.CLEANING):
            sid = row["sandbox_id"]
            self.db.set_state(sid, State.CLEANED)
            result["reconciled"] += 1
        return result

    # ---- 孤儿巡检 (保守: 只清自己 sqlite 追踪的超时/僵尸) ----
    def reap(self, purge_foreign: bool = False) -> dict:
        """
        巡检孤儿。
        - 自己 sqlite 里状态为 RUNNING/CLEANING 但已超时(无心跳)的: 确认 + kill 一次 + 落盘。
        - 不在 sqlite 的(purge_foreign=True 时): 列出但不默认杀。

        铁律1: list 不可靠,杀过的会残留 → 用 sqlite 去重,不重复杀。
        铁律2: 每个 id 只杀一次。
        铁律3: reaper_max_iter 限制循环轮数。
        """
        result = {"reaped_own": 0, "foreign": 0, "already_dead": 0, "iters": 0}

        with self._reaper_lock:
            tracked = self.db.all_tracked()
            killed_ids: set[str] = set()
            foreign_seen: set[str] = set()  # 去重: list 缓存会让同一 id 跨轮重复出现

            # 1) 清两类自己 sqlite 里的孤儿:
            #    a) CLEANING 态: kill 到一半进程崩了, 残留。
            #    b) RUNNING 态但无心跳(超过 stale_timeout): 持有它的进程已崩溃,
            #       acquire 的 finally 没跑成。活跃任务会持续刷新心跳, 不会进这。
            #    用心跳而非 running_since 判孤儿: 活跃长任务(合法跑 >1h)心跳持续刷新,
            #    不会被误杀; 只有进程崩了(心跳停)才清。
            stale_timeout = self._stale_timeout
            for row in self.db.list_state(State.CLEANING):
                if row["sandbox_id"] not in killed_ids:
                    if self._kill_one(row["sandbox_id"]):  # 已 CLEANED 会自动跳过
                        result["reaped_own"] += 1
                    killed_ids.add(row["sandbox_id"])
            for row in self.db.list_stale_running(stale_timeout):
                if row["sandbox_id"] not in killed_ids:
                    if self._kill_one(row["sandbox_id"]):
                        result["reaped_own"] += 1
                    killed_ids.add(row["sandbox_id"])

            # 2) list 候选: 只用来发现"sqlite 没追踪但可能是孤儿"的, 默认不杀
            Sandbox = self._sandbox_cls()
            from e2b.sandbox.sandbox_api import SandboxQuery
            from e2b.api.client.models.sandbox_state import SandboxState
            # pg 只建一次(循环外): 循环内重建会让 next_items 永远取第1页(#1 分页bug)
            try:
                pg = Sandbox.list(query=SandboxQuery(state=[SandboxState.RUNNING, SandboxState.PAUSED]),
                                  limit=self.reaper_list_limit)
            except Exception as e:
                logger.warning(f"list 失败: {e}")
                pg = None
            for _it in range(self.reaper_max_iter):  # 铁律3: 上限
                if pg is None:
                    break
                result["iters"] += 1
                batch = []
                box = {"items": []}
                def _fetch():
                    try:
                        box["items"] = pg.next_items() or []
                    except Exception as e:
                        box["err"] = str(e)
                t = threading.Thread(target=_fetch, daemon=True)
                t.start(); t.join(10)
                batch = box["items"]
                if t.is_alive():
                    logger.warning("list next_items 超时, 跳过本轮")
                    break

                new_foreign = []
                for s in batch:
                    sid = getattr(s, "sandbox_id", getattr(s, "id", None))
                    if not sid or sid in killed_ids or sid in tracked or sid in foreign_seen:
                        continue  # 铁律2: 已杀过 / 自己追踪 / 已统计过的跳过
                    new_foreign.append(sid)
                    foreign_seen.add(sid)

                if not new_foreign:
                    break  # 没有新候选了, 退出(不靠"列表清空"判断)
                result["foreign"] += len(new_foreign)

                if purge_foreign:
                    for sid in new_foreign:
                        if self._kill_one(sid, force=True):
                            killed_ids.add(sid)
                # 不 purge 时只报告不杀
                if not pg.has_next:
                    break

        return result

    def shutdown(self):
        """进程退出时: 清理崩溃残留 + in-flight 沙箱。
        - in-flight(本进程持有中): force-kill, 防 daemon worker/SIGTERM 时 acquire finally 没跑(#4)。
        - CLEANING 态: kill 到一半中断的, 收尾。
        - RUNNING 无心跳: 崩溃残留。
        有心跳的 RUNNING = 其他活跃线程正持有, 正常退出时它们的 finally 会清, 不杀。
        """
        with self._inflight_lock:
            inflight = list(self._inflight)
        for sid in inflight:
            self._kill_one(sid, force=True)
        # R5-3: 清空 _inflight, 防重入 shutdown 重复对已 CLEANED 的 sid 发 RPC
        with self._inflight_lock:
            self._inflight.clear()
        for row in self.db.list_state(State.CLEANING):
            self._kill_one(row["sandbox_id"])
        for row in self.db.list_stale_running(self._stale_timeout):
            self._kill_one(row["sandbox_id"])
        logger.info(f"shutdown 完成, stats={self.db.stats()}")
