"""me2b 异常层级。

所有 me2b 抛出的异常继承 Me2bError, 便于调用方 `except Me2bError` 一网打尽,
区别于 E2B SDK / pydantic / 网络的异常。
"""


class Me2bError(Exception):
    """me2b 所有异常的基类。"""


class StateTransitionError(Me2bError):
    """非法状态转移 (如 RUNNING→CLEANED 跳跃, 终态转出)。"""


class SandboxLeakError(Me2bError):
    """沙箱泄漏: create 成功但未能 kill, 或 kill 后确认未死透。"""


class PrewarmError(Me2bError):
    """template build/验证失败 (含 ERROR alias 毒化)。"""


class ConfigError(Me2bError):
    """配置错误 (如缺少必需的凭据/参数)。"""


class TosError(Me2bError):
    """TOS 操作失败 (挂载/上传/下载)。"""
