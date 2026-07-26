"""验证 _next_items 版本无关: 同步 paginator (2.35) 不 await, 协程 (2.34) await"""
import asyncio
from managed_e2b.async_core import AsyncSandboxLifecycle

passed, failed = [], []
def check(name, cond, detail=""):
    (passed if cond else failed).append(name)
    print(f"  {'✓' if cond else '✗'} {name} {detail}")

class SyncPaginator:
    """模拟 e2b 2.35 的 SandboxPaginator: next_items 返回 list (非协程)"""
    def __init__(self, pages):
        self._pages = list(pages)
        self.has_next = True
    def next_items(self):
        if self._pages:
            items = self._pages.pop(0)
            self.has_next = bool(self._pages)
            return items  # 返回 list, 不是协程
        self.has_next = False
        return []

class AsyncPaginator:
    """模拟 e2b 2.34 的 AsyncSandboxPaginator: next_items 是协程"""
    def __init__(self, pages):
        self._pages = list(pages)
        self.has_next = True
    async def next_items(self):
        if self._pages:
            items = self._pages.pop(0)
            self.has_next = bool(self._pages)
            return items
        self.has_next = False
        return []

async def main():
    lc = AsyncSandboxLifecycle.__new__(AsyncSandboxLifecycle)  # 不连 E2B
    lc._stale_timeout = 600

    print("[1] 同步 paginator (e2b 2.35 行为): next_items 返回 list")
    pg = SyncPaginator([[{"sandbox_id": "a"}, {"sandbox_id": "b"}], [{"sandbox_id": "c"}]])
    batch = await lc._next_items(pg)
    check("同步返回 list", isinstance(batch, list), f"({type(batch).__name__})")
    check("内容正确", [s["sandbox_id"] for s in batch] == ["a", "b"])
    batch2 = await lc._next_items(pg)
    check("第二页", [s["sandbox_id"] for s in batch2] == ["c"])

    print("\n[2] 协程 paginator (e2b 2.34 行为): next_items 是协程")
    pg2 = AsyncPaginator([[{"sandbox_id": "x"}], []])
    batch = await lc._next_items(pg2)
    check("协程返回 list", isinstance(batch, list))
    check("内容正确", batch[0]["sandbox_id"] == "x")

    print(f"\n=== {len(passed)} passed, {len(failed)} failed ===")
    if failed: print("FAILED:", failed)

asyncio.run(main())
