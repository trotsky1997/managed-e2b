"""asyncio 原语: AsyncLimiter / AsyncRateLimiter / AsyncHeartbeat。

独立模块, 不依赖 e2b。供 async_core.py 使用。
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager

logger = logging.getLogger("sandbox_lifecycle")


class AsyncLimiter:
    """asyncio.Semaphore 包装。slot() 是 async context manager。"""

    def __init__(self, max_concurrent: int):
        self._sem = asyncio.Semaphore(max_concurrent)
        self.max = max_concurrent

    @asynccontextmanager
    async def slot(self):
        await self._sem.acquire()
        try:
            yield
        finally:
            self._sem.release()


class AsyncRateLimiter:
    """token-bucket 速率限流: 调用间至少间隔 1/rate 秒。
    持有 asyncio.Lock 跨 await asyncio.sleep, 序列化创建到速率内。"""

    def __init__(self, rate: float):
        self._min_interval = 1.0 / rate if rate > 0 else 0
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    @asynccontextmanager
    async def slot(self):
        async with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._next_allowed = max(now, self._next_allowed) + self._min_interval
        yield


class AsyncHeartbeat:
    """无线程心跳: asyncio.create_task 驱动, loop 用 wait_for(event, interval)。
    对应 sync _Heartbeat。start() 必须在运行中的 loop 里调。"""

    def __init__(self, lifecycle, sid: str, stale_timeout: int):
        self._lc = lifecycle
        self._sid = sid
        iv = stale_timeout // 4
        self._interval = max(min(iv, stale_timeout - 1), 1) if stale_timeout > 10 else 5
        self._stop = asyncio.Event()
        self._task = None

    def start(self):
        """同步启动: 立即刷一次心跳(经 to_thread 不阻塞 loop) + 起后台 task。
        必须在 running loop 内调。"""
        loop = asyncio.get_running_loop()
        loop.create_task(asyncio.to_thread(self._lc.db.heartbeat, self._sid))
        self._task = loop.create_task(self._loop())

    async def stop(self):
        """异步停止: set event + cancel task + await join。"""
        self._stop.set()
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _loop(self):
        while not self._stop.is_set():
            try:
                # wait_for: event 被 set → 返回(退出); 超时 → TimeoutError(刷心跳)
                await asyncio.wait_for(self._stop.wait(), self._interval)
            except asyncio.TimeoutError:
                # 间隔到, 刷心跳(经 to_thread 不阻塞 loop)
                try:
                    await asyncio.to_thread(self._lc.db.heartbeat, self._sid)
                except Exception as e:
                    logger.warning(f"heartbeat {self._sid} 失败(非致命): {e}")
            except asyncio.CancelledError:
                raise  # 取消要传播, 不被 except Exception 吞
