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
"""
from .core import (
    SandboxLifecycle,
    SandboxHandle,
    State,
    SandboxDB,
    Limiter,
    RateLimiter,
)

__version__ = "0.1.0"
__all__ = ["SandboxLifecycle", "SandboxHandle", "State", "SandboxDB", "Limiter", "RateLimiter"]
