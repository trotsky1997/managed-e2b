"""managed_e2b (me2b) — managed E2B sandbox lifecycle.

A state-machine + sqlite-tracked lifecycle manager for E2B sandboxes:
queue/limiter → prewarm (build template if needed, dedup) → create → running
(heartbeat) → graceful quit/timeout → clean. Prevents orphaned sandboxes.

Quick start:
    from managed_e2b import SandboxLifecycle

    lc = SandboxLifecycle(db_path="sandboxes.db", max_concurrent=4)
    with lc.acquire(template="base", timeout=300) as h:
        h.sandbox.commands.run("echo hello")
    # with-exit auto kills + cleans; atexit reaps any stragglers.

Stage in/out + script execution + TOS mount:
    with lc.acquire(template="base") as h:
        h.stage_in({"solve.py": code})
        r = h.run_script("solve.py")
        out = h.stage_out(["result.txt"])
        h.mount_tos("my-bucket")  # needs E2B_TOS_AK/SK env
"""
from .core import (
    SandboxLifecycle,
    SandboxHandle,
    SandboxDB,
    Limiter,
    RateLimiter,
)
from .models import (
    State,
    SandboxRecord,
    SandboxConfig,
    AcquireRequest,
    PortForward,
)
from .errors import (
    Me2bError,
    StateTransitionError,
    SandboxLeakError,
    PrewarmError,
    ConfigError,
    TosError,
)
from .config import E2BConfig, load_env, get_e2b_config

__version__ = "0.1.0"

# async 支持 (懒加载: 不在 import 时拉 asyncio/e2b, sync-only 调用方不受影响)
def __getattr__(name):
    if name == "AsyncSandboxLifecycle":
        from .async_core import AsyncSandboxLifecycle
        return AsyncSandboxLifecycle
    if name == "AsyncSandboxHandle":
        from .async_core import AsyncSandboxHandle
        return AsyncSandboxHandle
    raise AttributeError(f"module 'managed_e2b' has no attribute {name!r}")

__all__ = [
    # core
    "SandboxLifecycle",
    "SandboxHandle",
    "SandboxDB",
    "Limiter",
    "RateLimiter",
    # models
    "State",
    "SandboxRecord",
    "SandboxConfig",
    "AcquireRequest",
    "PortForward",
    # errors
    "Me2bError",
    "StateTransitionError",
    "SandboxLeakError",
    "PrewarmError",
    "ConfigError",
    "TosError",
    # config
    "E2BConfig",
    "load_env",
    "get_e2b_config",
    # async (懒加载)
    "AsyncSandboxLifecycle",
    "AsyncSandboxHandle",
]
