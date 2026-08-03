"""me2b 异步核心: AsyncSandboxLifecycle + AsyncSandboxHandle。

同步 core.py 的 async 镜像。所有沙箱操作 await AsyncSandbox;
所有 SandboxDB 调用经 asyncio.to_thread (sqlite 阻塞)。
models/errors/config 共享; SandboxDB 复用 (from .core import)。
"""
from __future__ import annotations

import asyncio
import os
import time
import logging
import atexit
from contextlib import asynccontextmanager
from typing import Optional

from .core import SandboxDB
from .models import State, SandboxRecord, SandboxConfig, AcquireRequest
from .errors import (
    Me2bError, StateTransitionError, SandboxLeakError,
    PrewarmError, ConfigError, TosError,
)
from ._async import AsyncLimiter, AsyncRateLimiter, AsyncHeartbeat

logger = logging.getLogger("sandbox_lifecycle")


class AsyncSandboxHandle:
    """AsyncSandbox 的薄包装。每个方法 async def, await 沙箱操作。
    不持有创建/kill 责任 (由 lifecycle 的 acquire finally 管)。"""

    def __init__(self, sid: str, sandbox, template: str, lifecycle=None):
        self.sid = sid
        self.sandbox = sandbox
        self.template = template
        self.lifecycle = lifecycle

    async def stage_in(self, files: dict, prefix: str = "/task") -> dict:
        """stage in: 推文件进沙箱 (ephemeral)。files: {路径: content}。"""
        paths = {}
        for name, content in files.items():
            dst = name if name.startswith("/") else f"{prefix}/{name}"
            parent = "/".join(dst.split("/")[:-1]) or "/"
            await self.sandbox.files.make_dir(parent)
            await self.sandbox.files.write(dst, content)
            paths[name] = dst
        return paths

    async def stage_out(self, paths, prefix: str = "/task") -> dict:
        """stage out: 从沙箱取文件。返回 {路径: bytes}。"""
        out = {}
        for p in paths:
            src = p if p.startswith("/") else f"{prefix}/{p}"
            out[p] = await self.sandbox.files.read(src, format="bytes")
        return out

    async def run_script(self, script: str, args: list = None, interpreter: str = None,
                         workdir: str = "/task", timeout: int = 60, env: dict = None) -> dict:
        """执行 stage in 推入的脚本。自动推断解释器。"""
        path = script if script.startswith("/") else f"{workdir}/{script}"
        if interpreter is None:
            ext = path.rsplit(".", 1)[-1] if "." in path else "sh"
            interpreter = {".py": "python3", "py": "python3", ".js": "node", "js": "node",
                           ".sh": "bash", "sh": "bash"}.get(ext if ext.startswith(".") else "." + ext, "bash")
        cmd = f"{interpreter} {path}"
        if args:
            cmd += " " + " ".join(str(a) if " " not in str(a) else repr(str(a)) for a in args)
        if env:
            cmd = " ".join(f"{k}={v}" for k, v in env.items()) + " " + cmd
        r = await self.sandbox.commands.run(f"cd {workdir} && {cmd}", timeout=timeout)
        return {"stdout": r.stdout or "", "stderr": r.stderr or "", "exit_code": r.exit_code}

    async def run(self, command: str, timeout: int = 60) -> dict:
        """执行任意命令 (raw)。"""
        r = await self.sandbox.commands.run(command, timeout=timeout)
        return {"stdout": r.stdout or "", "stderr": r.stderr or "", "exit_code": r.exit_code}

    async def mount_tos(self, bucket: str, mount_point: str = "/mnt/tos", region: str = "cn-beijing"):
        """挂载火山 TOS 桶为本地目录 (s3fs FUSE)。需 E2B_TOS_AK/SK 环境变量。"""
        ak = os.environ.get("E2B_TOS_AK") or os.environ.get("TOS_ACCESS_KEY")
        sk = os.environ.get("E2B_TOS_SK") or os.environ.get("TOS_SECRET_KEY")
        if not ak or not sk:
            raise TosError("mount_tos 需 E2B_TOS_AK/E2B_TOS_SK 环境变量")
        await self.run("sudo apt-get update -qq >/dev/null 2>&1; sudo apt-get install -y -qq s3fs >/dev/null 2>&1", timeout=180)
        cred = f"{ak}:{sk}"
        await self.run('echo ' + repr(cred) + ' | sudo tee /etc/passwd-s3fs >/dev/null; sudo chmod 600 /etc/passwd-s3fs; sudo mkdir -p ' + mount_point, timeout=30)
        await self.run(f"sudo /usr/bin/s3fs {bucket} {mount_point} -o url=https://tos-s3-cn-beijing.volces.com -o endpoint={region} -o passwd_file=/etc/passwd-s3fs -o allow_other", timeout=30)

    async def save(self, name: str = None) -> str:
        """保存当前沙箱为快照 (持久, 记入 sqlite)。返回 snapshot id。"""
        info = await self.sandbox.create_snapshot(name=name)
        snap_id = getattr(info, "id", None) or getattr(info, "snapshot_id", None)
        if snap_id and self.lifecycle:
            await asyncio.to_thread(self.lifecycle.db.record_snapshot, snap_id, name, self.sid)
        return snap_id

    async def fork(self, count: int = 1, timeout: int = 60) -> list:
        """复制当前沙箱 (带状态) 为 count 个。副本进状态机 (RUNNING + _inflight)。"""
        forks = await self.sandbox.fork(count=count, timeout=timeout)
        handles = []
        now = int(time.time())
        for f in forks:
            if isinstance(f, Exception):
                logger.warning(f"fork 失败: {f}")
                continue
            h = AsyncSandboxHandle(sid=f.sandbox_id, sandbox=f, template=self.template, lifecycle=self.lifecycle)
            if self.lifecycle:
                await asyncio.to_thread(
                    self.lifecycle.db.upsert, f.sandbox_id,
                    state=State.RUNNING.value, template=self.template, created_at=now,
                    last_heartbeat=now, metadata='{"forked_from":"' + self.sid + '"}')
                self.lifecycle._inflight.add(f.sandbox_id)
            handles.append(h)
        return handles

    async def pause(self, keep_memory: bool = True) -> bool:
        """暂停沙箱。状态机: RUNNING→PAUSED。"""
        r = await self.sandbox.pause(keep_memory=keep_memory)
        if r and self.lifecycle:
            await asyncio.to_thread(self.lifecycle.db.set_state, self.sid, State.PAUSED)
        return r

    async def resume(self, timeout: int = None) -> "AsyncSandboxHandle":
        """恢复已暂停沙箱 (connect auto-resume)。PAUSED→RUNNING + 重启心跳。"""
        Sandbox = type(self.sandbox)
        sbx = await Sandbox.connect(self.sid, timeout=timeout)
        h = AsyncSandboxHandle(sid=self.sid, sandbox=sbx, template=self.template, lifecycle=self.lifecycle)
        if self.lifecycle:
            await asyncio.to_thread(self.lifecycle.db.set_state, self.sid, State.RUNNING)
            hb = AsyncHeartbeat(self.lifecycle, self.sid, self.lifecycle._stale_timeout)
            hb.start()
            self.lifecycle._hb_tasks.add(hb._task)
        return h

    # ---- 端口转发: 获取沙箱端口的外部访问地址 ----
    async def get_host(self, port: int) -> str:
        """获取沙箱端口的外部访问主机地址 (host:port 格式)。
        用此地址可从沙箱外部通过 HTTP/WebSocket 连接到沙箱内端口。"""
        return self.sandbox.get_host(port)

    async def get_url(self, port: int, scheme: str = "https") -> str:
        """获取沙箱端口的外部访问完整 URL。
        scheme: http 或 https (默认 https)"""
        host = self.sandbox.get_host(port)
        return f"{scheme}://{host}"

    async def expose_port(self, port: int, command: str = None, allow_public: bool = True):
        """暴露沙箱端口供外部访问。记录到 sqlite (生命周期追踪)。

        Args:
            port: 沙箱内端口号
            command: 可选, 在沙箱内后台启动该端口的服务命令
            allow_public: 是否更新网络配置允许公开访问

        Returns:
            PortForward: 包含 host, url 等信息的端口转发对象
        """
        from managed_e2b.models import PortForward
        if command:
            await self.sandbox.commands.run(f"{command} &", background=True, timeout=5)
        if allow_public:
            try:
                from e2b import SandboxNetworkUpdate
                self.sandbox.update_network(
                    SandboxNetworkUpdate(allow_internet_access=True)
                )
            except Exception as e:
                logger.warning(f"update_network failed (non-fatal): {e}")
        host = self.sandbox.get_host(port)
        pf = PortForward(
            port=port,
            host=host,
            url=f"https://{host}",
            command=command,
            sandbox_id=self.sid,
        )
        # 落盘: 端口转发生命周期追踪
        if self.lifecycle:
            await asyncio.to_thread(
                self.lifecycle.db.record_port_forward,
                self.sid, port, host, pf.url, command,
            )
        return pf

    async def list_ports(self) -> list:
        """列出当前沙箱已暴露的端口 (从 sqlite 查)。返回 PortForward 列表。"""
        from managed_e2b.models import PortForward
        if not self.lifecycle:
            return []
        rows = await asyncio.to_thread(self.lifecycle.db.list_port_forwards, self.sid)
        return [PortForward(
            port=r["port"], host=r["host"], url=r["url"],
            command=r["command"], sandbox_id=r["sandbox_id"],
        ) for r in rows]

    async def close_port(self, port: int) -> bool:
        """关闭沙箱端口: kill 进程 + 删 sqlite 记录。"""
        if not self.lifecycle:
            return False
        rows = await asyncio.to_thread(self.lifecycle.db.list_port_forwards, self.sid)
        found = any(r["port"] == port for r in rows)
        if not found:
            return False
        try:
            await self.sandbox.commands.run(
                f"sh -c 'fuser -k {port}/tcp 2>/dev/null; pkill -f :{port} 2>/dev/null; true'",
                timeout=10,
            )
        except Exception as e:
            logger.warning(f"close_port kill process on {port} failed (non-fatal): {e}")
        await asyncio.to_thread(self._delete_port_forward, self.sid, port)
        return True

    def _delete_port_forward(self, sandbox_id: str, port: int):
        """同步删 sqlite 记录 (供 to_thread 调用)。"""
        with self.lifecycle.db._lock:
            self.lifecycle.db._conn.execute(
                "DELETE FROM port_forwards WHERE sandbox_id=? AND port=?",
                (sandbox_id, port),
            )
            self.lifecycle.db._conn.commit()


class AsyncSandboxLifecycle:
    """异步生命周期管理器。SandboxDB 复用 (经 to_thread); 3 个 asyncio limiter;
    _inflight set + 心跳 task 注册表。状态机与 sync 一致 (RUNNING→CLEANING→CLEANED)。

    用法:
        lc = AsyncSandboxLifecycle(db_path="me2b.db", max_concurrent=4)
        async with lc.acquire(template="base", timeout=300) as h:
            await h.run("echo hi")
        await lc.shutdown()
    """

    def __init__(self, db_path: str, max_concurrent: int = 4, e2b_key: str = None,
                 e2b_api_url: str = None, create_rate: float = 1.0,
                 max_build_concurrency: int = 4, stale_timeout: int = 600,
                 reaper_max_iter: int = 5, reaper_list_limit: int = 100):
        cfg = SandboxConfig(db_path=db_path, max_concurrent=max_concurrent,
                            create_rate=create_rate, max_build_concurrency=max_build_concurrency,
                            stale_timeout=stale_timeout, reaper_max_iter=reaper_max_iter,
                            reaper_list_limit=reaper_list_limit, e2b_key=e2b_key, e2b_api_url=e2b_api_url)
        if e2b_api_url:
            os.environ["E2B_API_URL"] = e2b_api_url
        if e2b_key:
            os.environ["E2B_API_KEY"] = e2b_key
        self.db = SandboxDB(db_path)
        self._stale_timeout = stale_timeout
        self._build_limiter = AsyncLimiter(max_build_concurrency)
        self._create_limiter = AsyncRateLimiter(create_rate)
        self._run_limiter = AsyncLimiter(max_concurrent)
        self._key = e2b_key or os.environ.get("E2B_API_KEY")
        if not self._key:
            raise ConfigError("E2B_API_KEY 未设置")
        self._reaper_max_iter = reaper_max_iter
        self._reaper_list_limit = reaper_list_limit
        self._reaper_lock = asyncio.Lock()
        self._inflight: set[str] = set()
        self._inflight_lock = asyncio.Lock()
        self._template_ready: dict[str, bool] = {}
        self._template_alias: dict[str, str] = {}
        self._template_locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()
        self._hb_tasks: set = set()  # 心跳 task 注册表 (防 GC + shutdown 清理)
        atexit.register(self._atexit_shutdown)

    # ---- E2B 懒加载 ----
    def _sandbox_cls(self):
        from e2b_code_interpreter import AsyncSandbox
        return AsyncSandbox

    async def _connect(self, sid: str):
        Sandbox = self._sandbox_cls()
        return await Sandbox.connect(sid)

    async def _next_items(self, pg, timeout: int = 10):
        """版本无关地取 paginator 下一页。e2b 2.34 的 AsyncSandboxPaginator.next_items
        是协程(该 await);2.35 返回同步 SandboxPaginator, next_items 返回 list(不该 await)。
        运行时检测: 协程就 await+wait_for, 否则直接返回(同步 list)。"""
        ni = pg.next_items()
        if asyncio.iscoroutine(ni):
            return await asyncio.wait_for(ni, timeout=timeout)
        return ni

    # ---- create ----
    async def _create(self, template: str, timeout: int, metadata: dict,
                      allow_internet_access: bool = True, network: dict = None) -> AsyncSandboxHandle:
        """create 沙箱并落盘。先 create 拿真实 id (必须 await), 再写 db。
        allow_internet_access/network 透传给 E2B create (控制沙箱网络)。"""
        Sandbox = self._sandbox_cls()
        sbx = await Sandbox.create(template=template, timeout=timeout, metadata=metadata,
                                   allow_internet_access=allow_internet_access, network=network)
        real_id = sbx.sandbox_id
        try:
            now = int(time.time())
            rec = SandboxRecord(sandbox_id=real_id, state=State.RUNNING,
                                template=template, created_at=now,
                                last_heartbeat=now, metadata=str(metadata))
            await asyncio.to_thread(self.db.upsert, real_id, **rec.to_db_row())
        except Exception as e:
            logger.error(f"db 写入失败, 回收沙箱 {real_id}: {e}")
            try:
                await sbx.kill()
            except Exception:
                pass
            raise
        return AsyncSandboxHandle(sid=real_id, sandbox=sbx, template=template, lifecycle=self)

    # ---- kill + confirm ----
    async def _kill_one(self, sid: str, sandbox_obj=None, force: bool = False) -> bool:
        """杀沙箱并确认死透。force=True 对外部 sid 绕过 DB 门控。"""
        if not force:
            if not await asyncio.to_thread(self.db.try_claim_for_kill, sid):
                return False
        try:
            if sandbox_obj is not None:
                await sandbox_obj.kill()
            else:
                sbx = await self._connect(sid)
                await sbx.kill()
        except Exception as e:
            logger.warning(f"kill {sid} 异常(可能已死): {e}")
        confirmed = await self._confirm_dead(sid, sandbox_obj)
        if confirmed:
            if force:
                now = int(time.time())
                rec = SandboxRecord(sandbox_id=sid, state=State.CLEANED,
                                    template="(foreign)", created_at=now, killed_at=now)
                await asyncio.to_thread(self.db.upsert, sid, **rec.to_db_row())
            else:
                await asyncio.to_thread(self.db.set_state, sid, State.CLEANED)
                # 清理该沙箱的端口转发记录
                await asyncio.to_thread(self.db.delete_port_forwards, sid)
        else:
            logger.warning(f"kill {sid} 后 is_running 仍 True, 留 CLEANING 待重试")
        return confirmed

    async def _confirm_dead(self, sid: str, sandbox_obj=None) -> bool:
        """确认沙箱已死。NotFound/400=死, 网络=重试。"""
        for _ in range(3):
            try:
                if sandbox_obj is not None:
                    if not await sandbox_obj.is_running():
                        return True
                else:
                    sbx = await self._connect(sid)
                    if not await sbx.is_running():
                        return True
            except Exception as e:
                en = type(e).__name__
                msg = str(e).lower()
                if ("notfound" in en.lower() or "not found" in msg or "sandboxnotfound" in en.lower()
                        or "invalid sandbox id" in msg or ("400" in msg and "invalid" in msg)):
                    return True
                logger.warning(f"_confirm_dead {sid} 瞬时错误(重试): {en}: {str(e)[:80]}")
            await asyncio.sleep(0.5)
        return False

    # ---- prewarm ----
    def template_name_for(self, image: str = None, dockerfile: str = None) -> str:
        import hashlib
        src = image or dockerfile
        h = hashlib.sha1(src.encode()).hexdigest()[:12]
        return ("img-" if image else "df-") + h

    async def _template_lock(self, name: str) -> asyncio.Lock:
        async with self._locks_guard:
            if name not in self._template_locks:
                self._template_locks[name] = asyncio.Lock()
            return self._template_locks[name]

    async def _poll_template_status(self, Template, builder, name: str, timeout: int = 300) -> str:
        """轮询 template build 到终态(ready/error)。"""
        deadline = time.time() + timeout
        try:
            info = await Template.build_in_background(builder, name)
        except Exception as e:
            msg = str(e).lower()
            if "ready" in msg or "not in waiting" in msg or "already" in msg:
                return "ready"
            return "error"
        while time.time() < deadline:
            try:
                status = await Template.get_build_status(info)
                sval = str(getattr(status, "status", status)).lower()
                if "ready" in sval:
                    return "ready"
                if "error" in sval or "fail" in sval:
                    return "error"
            except Exception:
                pass
            await asyncio.sleep(3)
        return "timeout"

    async def _build_with_timeout(self, builder, name: str, cpu_count: int, memory_mb: int,
                                  timeout: int = 600) -> None:
        """带硬超时的 build: build_in_background + 轮询。"""
        from e2b import AsyncTemplate
        info = await AsyncTemplate.build_in_background(builder, name, cpu_count=cpu_count, memory_mb=memory_mb)
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = await AsyncTemplate.get_build_status(info)
            sval = str(getattr(status, "status", status)).lower()
            if "success" in sval or "ready" in sval:
                return
            if "error" in sval or "fail" in sval:
                raise PrewarmError(f"template {name} build 失败: {sval}")
            await asyncio.sleep(3)
        raise asyncio.TimeoutError(f"template {name} build 超时 ({timeout}s)")

    async def prewarm(self, image: str = None, dockerfile: str = None, *,
                      cpu_count: int = 2, memory_mb: int = 1024) -> str:
        """预热: build template if need + 去重。"""
        if not image and not dockerfile:
            raise ConfigError("prewarm 需要 image 或 dockerfile")
        name = self.template_name_for(image, dockerfile)
        ready = self._template_ready.get(name)
        if ready:
            return self._template_alias.get(name, name)
        lock = await self._template_lock(name)
        async with lock:
            ready = self._template_ready.get(name)
            if ready:
                return self._template_alias.get(name, name)
            from e2b import AsyncTemplate
            builder = AsyncTemplate().from_image(image) if image else AsyncTemplate().from_dockerfile(dockerfile)
            name_to_use = name
            async with self._build_limiter.slot():
                ready3 = self._template_ready.get(name_to_use)
                if ready3:
                    return name_to_use
                alias_exists = False
                try:
                    alias_exists = await AsyncTemplate.exists(name_to_use)
                except Exception as e:
                    logger.warning(f"exists({name_to_use}) 失败, 当作不存在: {e}")
                if alias_exists:
                    sval = await self._poll_template_status(AsyncTemplate, builder, name_to_use)
                    if sval == "ready":
                        self._template_ready[name_to_use] = True
                        logger.info(f"template {name_to_use} 已就绪(复用)")
                        return name_to_use
                    name_to_use = name + "-r" + str(int(time.time()))[-5:]
                    logger.warning(f"template {name} 状态={sval}, 换名 {name_to_use} 重建")
                last_err = None
                for attempt in range(2):
                    try:
                        await self._build_with_timeout(builder, name_to_use, cpu_count, memory_mb)
                        self._template_ready[name_to_use] = True
                        if name_to_use != name:
                            self._template_ready[name] = True
                            self._template_alias[name] = name_to_use
                        logger.info(f"template {name_to_use} build 完成")
                        return name_to_use
                    except Exception as e:
                        last_err = e
                        logger.warning(f"template {name_to_use} build 第{attempt+1}次失败: {e}")
                raise PrewarmError(f"prewarm build {name_to_use} 失败: {last_err}") from last_err

    # ---- acquire ----
    @asynccontextmanager
    async def acquire(self, image: str = None, dockerfile: str = None,
                      template: str = None, timeout: int = 1800, metadata: dict = None,
                      allow_internet_access: bool = True, network: dict = None):
        """获取沙箱, 用完自动 kill + clean。带 limiter + 心跳。
        allow_internet_access/network 透传给 E2B create (控制沙箱网络)。"""
        req = AcquireRequest(image=image, dockerfile=dockerfile, template=template,
                             timeout=timeout, metadata=metadata or {},
                             allow_internet_access=allow_internet_access, network=network)
        image, dockerfile, template, timeout = req.image, req.dockerfile, req.template, req.timeout
        if not image and not dockerfile and not template:
            template = "base"
        md = dict(req.metadata)
        md.setdefault("managed_by", "sandbox_lifecycle")
        h = None
        hb = None
        if image or dockerfile:
            template = await self.prewarm(image=image, dockerfile=dockerfile)
        try:
            async with self._create_limiter.slot():
                h = await self._create(template, timeout, md, req.allow_internet_access, req.network)
            self._inflight.add(h.sid)
            hb = AsyncHeartbeat(self, h.sid, self._stale_timeout)
            hb.start()
            if hb._task:
                self._hb_tasks.add(hb._task)
            async with self._run_limiter.slot():
                yield h
        finally:
            if hb is not None:
                await hb.stop()
                if hb._task:
                    self._hb_tasks.discard(hb._task)
            if h is not None:
                # 持 task 引用 + 超时, 防 shield detached/GC/卡死 (审查 A4-2)
                kill_t = asyncio.create_task(self._kill_one(h.sid, h.sandbox))
                try:
                    await asyncio.wait_for(asyncio.shield(kill_t), timeout=120)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    logger.warning(f"acquire kill {h.sid} 超时/取消, 后台继续")
                self._inflight.discard(h.sid)

    # ---- restore from snapshot ----
    @asynccontextmanager
    async def restore_from_snapshot(self, snapshot_id: str, timeout: int = 300, metadata: dict = None):
        md = metadata or {}
        md.setdefault("managed_by", "sandbox_lifecycle")
        md["restored_from"] = snapshot_id
        async with self._create_limiter.slot():
            h = await self._create(snapshot_id, timeout, md)
        self._inflight.add(h.sid)
        hb = AsyncHeartbeat(self, h.sid, self._stale_timeout)
        hb.start()
        if hb._task:
            self._hb_tasks.add(hb._task)
        try:
            async with self._run_limiter.slot():
                yield h
        finally:
            await hb.stop()
            if hb._task:
                self._hb_tasks.discard(hb._task)
            kill_t = asyncio.create_task(self._kill_one(h.sid, h.sandbox))
            try:
                await asyncio.wait_for(asyncio.shield(kill_t), timeout=120)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                logger.warning(f"restore kill {h.sid} 超时/取消, 后台继续")
            self._inflight.discard(h.sid)

    async def resume_sandbox(self, sid: str) -> AsyncSandboxHandle:
        Sandbox = self._sandbox_cls()
        sbx = await Sandbox.connect(sid)
        now = int(time.time())
        await asyncio.to_thread(self.db.upsert, sid, state=State.RUNNING.value,
                                template="(resumed)", created_at=now, last_heartbeat=now)
        self._inflight.add(sid)
        h = AsyncSandboxHandle(sid=sid, sandbox=sbx, template="(resumed)", lifecycle=self)
        hb = AsyncHeartbeat(self, sid, self._stale_timeout)
        hb.start()
        if hb._task:
            self._hb_tasks.add(hb._task)
        return h

    async def _release(self, handle: AsyncSandboxHandle) -> None:
        """释放一个 handle (kill + db clean + 移出 _inflight)。
        供非 context-manager 场景 (如 Harbor stop) 用, 对应 acquire 的 finally。"""
        if handle is None:
            return
        await asyncio.shield(self._kill_one(handle.sid, handle.sandbox))
        self._inflight.discard(handle.sid)

    # ---- snapshot cleanup ----
    async def cleanup_snapshots(self, keep: set = None) -> dict:
        Sandbox = self._sandbox_cls()
        keep = keep or set()
        result = {"deleted": 0, "failed": 0}
        snaps = await asyncio.to_thread(self.db.list_snapshots)
        for row in snaps:
            sid = row["snapshot_id"]
            if sid in keep:
                continue
            try:
                await Sandbox.delete_snapshot(sid)
                await asyncio.to_thread(self.db.forget_snapshot, sid)
                result["deleted"] += 1
            except Exception as e:
                logger.warning(f"删快照 {sid} 失败: {e}")
                result["failed"] += 1
        return result

    # ---- reap (孤儿巡检) ----
    async def reap(self, purge_foreign: bool = False) -> dict:
        result = {"reaped_own": 0, "foreign": 0, "already_dead": 0, "iters": 0}
        async with self._reaper_lock:
            tracked = await asyncio.to_thread(self.db.all_tracked)
            killed_ids: set[str] = set()
            foreign_seen: set[str] = set()
            # CLEANING 残留
            for row in await asyncio.to_thread(self.db.list_state, State.CLEANING):
                if row["sandbox_id"] not in killed_ids:
                    if await self._kill_one(row["sandbox_id"]):
                        result["reaped_own"] += 1
                    killed_ids.add(row["sandbox_id"])
            # stale RUNNING
            for row in await asyncio.to_thread(self.db.list_stale_running, self._stale_timeout):
                if row["sandbox_id"] not in killed_ids:
                    if await self._kill_one(row["sandbox_id"]):
                        result["reaped_own"] += 1
                    killed_ids.add(row["sandbox_id"])
            # list 候选 (外部)
            Sandbox = self._sandbox_cls()
            from e2b.sandbox.sandbox_api import SandboxQuery
            from e2b.api.client.models.sandbox_state import SandboxState
            try:
                pg = Sandbox.list(query=SandboxQuery(state=[SandboxState.RUNNING, SandboxState.PAUSED]),
                                   limit=self._reaper_list_limit)
            except Exception as e:
                logger.warning(f"reap list 失败: {e}")
                pg = None
            for _it in range(self._reaper_max_iter):
                if pg is None:
                    break
                result["iters"] += 1
                try:
                    batch = await self._next_items(pg, timeout=10)
                except asyncio.TimeoutError:
                    logger.warning("reap next_items 超时, 跳过本轮")
                    break
                except Exception as e:
                    logger.warning(f"reap next_items 失败: {e}")
                    break
                new_foreign = []
                for s in batch:
                    sid = getattr(s, "sandbox_id", getattr(s, "id", None))
                    if not sid or sid in killed_ids or sid in tracked or sid in foreign_seen:
                        continue
                    new_foreign.append(sid)
                    foreign_seen.add(sid)
                if not new_foreign:
                    break
                result["foreign"] += len(new_foreign)
                if purge_foreign:
                    for sid in new_foreign:
                        if await self._kill_one(sid, force=True):
                            killed_ids.add(sid)
                if not pg.has_next:
                    break
        return result

    # ---- reconcile ----
    async def reconcile(self) -> dict:
        result = {"reconciled": 0, "list_failed": False}
        Sandbox = self._sandbox_cls()
        live = set()
        try:
            pg = Sandbox.list(query=__import__("e2b.sandbox.sandbox_api", fromlist=["SandboxQuery"]).SandboxQuery(
                state=[__import__("e2b.api.client.models.sandbox_state", fromlist=["SandboxState"]).SandboxState.RUNNING,
                       __import__("e2b.api.client.models.sandbox_state", fromlist=["SandboxState"]).SandboxState.PAUSED]),
                limit=self._reaper_list_limit)
            for _ in range(self._reaper_max_iter):
                try:
                    items = await self._next_items(pg, timeout=10)
                except (asyncio.TimeoutError, Exception):
                    result["list_failed"] = True
                    logger.warning("reconcile list 失败, 跳过清理")
                    return result
                live.update(getattr(s, "sandbox_id", getattr(s, "id", None)) for s in items)
                if not pg.has_next:
                    break
        except Exception as e:
            result["list_failed"] = True
            logger.warning(f"reconcile list 异常, 跳过: {e}")
            return result
        for row in await asyncio.to_thread(self.db.list_stale_running, self._stale_timeout):
            if await self._kill_one(row["sandbox_id"]):
                result["reconciled"] += 1
        for row in await asyncio.to_thread(self.db.list_state, State.CLEANING):
            if await self._kill_one(row["sandbox_id"]):
                result["reconciled"] += 1
        return result

    # ---- shutdown + atexit ----
    async def shutdown(self):
        """进程退出时: 取消心跳 task + kill in-flight + 清 CLEANING/stale RUNNING。"""
        # 取消心跳
        for task in list(self._hb_tasks):
            if not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._hb_tasks.clear()
        # kill in-flight
        inflight = list(self._inflight)
        for sid in inflight:
            await self._kill_one(sid, force=True)
        self._inflight.clear()
        for row in await asyncio.to_thread(self.db.list_state, State.CLEANING):
            await self._kill_one(row["sandbox_id"])
        for row in await asyncio.to_thread(self.db.list_stale_running, self._stale_timeout):
            await self._kill_one(row["sandbox_id"])
        logger.info(f"async shutdown 完成, stats={await asyncio.to_thread(self.db.stats)}")

    def _atexit_shutdown(self):
        """atexit (loop 已关, 不能 await e2b): 只标 DB, 下次 reconcile 清。"""
        try:
            for sid in self._inflight:
                if self.db.get(sid) and self.db.get(sid)["state"] != State.CLEANED.value:
                    self.db.set_state(sid, State.CLEANING)
        except Exception:
            pass
