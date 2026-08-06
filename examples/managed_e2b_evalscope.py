"""E2B code-execution backend for evalscope.

Drop-in replacement for evalscope's ms-enclave docker sandbox: executes
generated code in an E2B sandbox managed by me2b (managed_e2b), instead of a
local docker container. humaneval/mbpp just need execute() to return
{'status': 'success'} or {'status': 'error'/'timeout'}.

Usage:
    # monkeypatch evalscope to use E2B for code execution
    from managed_e2b_evalscope import install_e2b_backend
    install_e2b_backend(template="base", e2b_key="e2b_...", timeout=120)
    # then run evalscope eval normally
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Union

logger = logging.getLogger("managed_e2b_evalscope")

# me2b SandboxLifecycle 实例 (延迟创建, 复用跨题目)
_lc = None
_TEMPLATE = "base"
_TIMEOUT = 120
_KEY = None


class E2BCodeExecutionBackend:
    """执行代码的 E2B 后端, 对齐 evalscope CodeExecutionBackend 接口。
    execute(code, timeout, language) → {'status': 'success'|'error'|'timeout', ...}
    """

    def __init__(self, benchmark_meta=None, task_config=None, image_spec_provider=None):
        # 不依赖 task_config (那是 ms-enclave 的); 用模块级 me2b 实例
        pass

    def start(self) -> None:
        global _lc
        if _lc is None:
            from managed_e2b import SandboxLifecycle
            _lc = SandboxLifecycle(
                db_path="/root/me2b_eval.db",
                max_concurrent=4,
                e2b_key=_KEY,
                stale_timeout=600,
            )
        logger.info(f"E2B backend started (template={_TEMPLATE})")

    def is_ready(self) -> bool:
        return _lc is not None

    def execute(self, code: Union[str, List[str]], timeout: int, language: str) -> Dict[str, Any]:
        if isinstance(code, list):
            code = "\n".join(code)
        if _lc is None:
            return {"status": "error", "error": "E2B backend not started"}
        try:
            with _lc.acquire(template=_TEMPLATE, timeout=_TIMEOUT) as h:
                # 写入临时文件再执行(避免 import 提示污染 stdout)
                h.sandbox.files.write("/tmp/_eval_prog.py", code)
                r = h.sandbox.commands.run(f"python3 /tmp/_eval_prog.py", timeout=timeout)
            exit_code = getattr(r, "exit_code", 1)
            # humaneval: 程序跑完 exit 0 = success
            if exit_code == 0:
                return {"status": "success", "exit_code": 0,
                        "stdout": getattr(r, "stdout", "") or "",
                        "stderr": getattr(r, "stderr", "") or ""}
            return {"status": "error", "exit_code": exit_code,
                    "stdout": getattr(r, "stdout", "") or "",
                    "stderr": getattr(r, "stderr", "") or "",
                    "error": (getattr(r, "stderr", "") or "")[:500]}
        except TimeoutError:
            return {"status": "timeout", "error": "E2B execution timed out"}
        except Exception as e:
            logger.exception("E2B execute failed")
            return {"status": "error", "error": str(e)[:500]}

    def stop(self) -> None:
        # me2b 的 lifecycle 由 atexit 兜底; 这里不动
        pass


def install_e2b_backend(template: str = "base", e2b_key: str = None,
                         timeout: int = 120, **lc_kwargs):
    """让 evalscope 的代码执行改用 E2B (替换 EnclaveCodeExecutionBackend)。

    monkeypatch CodeExecutionSandboxMixin._get_backend 返回 E2B 后端,
    并预建 me2b SandboxLifecycle。
    """
    global _TEMPLATE, _TIMEOUT, _KEY
    _TEMPLATE = template
    _TIMEOUT = timeout
    _KEY = e2b_key

    from evalscope.api.mixin.code_execution_sandbox_mixin import CodeExecutionSandboxMixin

    def _e2b_get_backend(self):
        if not getattr(self, "_e2b_backend", None):
            self._e2b_backend = E2BCodeExecutionBackend(
                getattr(self, "_benchmark_meta", None),
                getattr(self, "_task_config", None),
            )
        return self._e2b_backend

    CodeExecutionSandboxMixin._get_backend = _e2b_get_backend
    logger.info(f"evalscope code execution patched → E2B (template={template})")
