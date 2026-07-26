"""Pydantic 数据模型层 —— 类型安全 + 状态机校验。

设计:
- State: 状态枚举 + 合法转移图 (transition guard)。
- SandboxRecord: 一行沙箱的强类型模型, 字段约束 + 状态转移校验。
- SandboxConfig: SandboxLifecycle 的配置模型, 参数校验集中化。

sqlite 仍是真相源 (持久化); pydantic 模型是内存中的强类型表示,
upsert/set_state 用模型序列化进 sqlite, 状态转移走模型校验。
"""
from __future__ import annotations

import time
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator


class State(str, Enum):
    """沙箱状态机。sqlite 落盘的只有 RUNNING/CLEANING/CLEANED 三态,
    其余是瞬时态(不单独落盘)。完整枚举用于状态机文档与转移校验。"""

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

    @property
    def terminal(self) -> bool:
        return self in (State.CLEANED, State.PREWARM_FAIL, State.CREATE_FAIL)

    # 合法转移图: from_state -> {允许的 to_state}
    # 用模块级常量避免被当字段
    def can_transition_to(self, target: "State") -> bool:
        """该状态是否允许转移到 target。终态不允许转出(除非 target==self)。"""
        if self == target:
            return True
        return target in _TRANSITIONS.get(self, frozenset())


# 合法转移图: from_state -> {允许的 to_state} (模块级, 避免被当 pydantic 字段)
_TRANSITIONS: dict[State, frozenset] = {
    State.QUEUED: frozenset({State.PREWARMING, State.READY, State.CREATING}),
    State.PREWARMING: frozenset({State.READY, State.PREWARM_FAIL}),
    State.READY: frozenset({State.CREATING, State.PREWARM_FAIL}),
    State.CREATING: frozenset({State.RUNNING, State.CREATE_FAIL}),
    State.RUNNING: frozenset({State.GRACEFUL_QUIT, State.TIMEOUT, State.CLEANING}),
    State.GRACEFUL_QUIT: frozenset({State.CLEANING}),
    State.TIMEOUT: frozenset({State.CLEANING}),
    State.CLEANING: frozenset({State.CLEANED, State.CLEANING}),  # 可重入(崩溃残留重试)
    State.CLEANED: frozenset(),                        # 终态
    State.PREWARM_FAIL: frozenset(),
    State.CREATE_FAIL: frozenset(),
}


class SandboxRecord(BaseModel):
    """一行沙箱的强类型记录 (对应 sqlite 的 sandboxes 表一行)。"""

    model_config = ConfigDict(use_enum_values=False, validate_assignment=True)

    sandbox_id: str = Field(..., min_length=1)
    template: Optional[str] = None
    state: State = Field(..., description="当前状态")
    created_at: int = Field(..., description="入队时间 (unix epoch)")
    running_since: Optional[int] = Field(None, description="进入 RUNNING 的时间")
    killed_at: Optional[int] = Field(None, description="进入 CLEANING 的时间")
    metadata: Optional[str] = Field(None, description="JSON 字符串, 审计用")
    last_error: Optional[str] = Field(None, max_length=500)
    last_heartbeat: Optional[int] = Field(None, description="最近心跳时间")

    @field_validator("last_error")
    @classmethod
    def _truncate_error(cls, v: Optional[str]) -> Optional[str]:
        return v[:500] if v else v

    @model_validator(mode="after")
    def _check_timestamps(self) -> "SandboxRecord":
        """时间戳一致性: running_since/killed_at 不应早于 created_at。"""
        if self.running_since is not None and self.running_since < self.created_at:
            raise ValueError("running_since 早于 created_at")
        if self.killed_at is not None and self.killed_at < self.created_at:
            raise ValueError("killed_at 早于 created_at")
        return self

    def transition_to(self, target: State, now: Optional[int] = None) -> "SandboxRecord":
        """带校验的状态转移: 不合法抛 ValueError。返回转移后的新记录(不可变语义)。
        状态副作用(running_since/last_heartbeat/killed_at)在此集中管理。"""
        if not self.state.can_transition_to(target):
            raise ValueError(f"非法状态转移: {self.state.value} → {target.value}")
        now = now if now is not None else int(time.time())
        updates: dict = {"state": target}
        if target == State.RUNNING:
            updates["running_since"] = now
            updates["last_heartbeat"] = now
        if target in (State.CLEANING, State.GRACEFUL_QUIT, State.TIMEOUT):
            updates["killed_at"] = now
        # 用 model_copy 应用更新 (validate_assignment 会再校验)
        return self.model_copy(update=updates)

    def to_db_row(self) -> dict:
        """序列化为 sqlite 可用的 dict (state 用 .value, 去掉 None 保持兼容)。"""
        d = self.model_dump()
        d["state"] = self.state.value
        return {k: v for k, v in d.items() if v is not None}

    @classmethod
    def from_db_row(cls, row) -> "SandboxRecord":
        """从 sqlite Row/dict 构造 (state 字符串 → State 枚举)。"""
        d = dict(row)
        d["state"] = State(d["state"])
        return cls(**d)


class SandboxConfig(BaseModel):
    """SandboxLifecycle 配置模型, 参数校验集中化。"""

    model_config = ConfigDict(extra="forbid")

    db_path: str = Field(..., description="sqlite 路径, 跨运行需稳定复用以支持崩溃恢复")
    max_concurrent: int = Field(4, ge=1, description="同时 RUNNING 的沙箱数 (评测并发度)")
    create_rate: float = Field(1.0, gt=0, description="create 速率 (次/秒); E2B Hobby=1/s")
    max_build_concurrency: int = Field(4, ge=1, description="template build 并发")
    stale_timeout: int = Field(600, ge=10, description="无心跳多久判孤儿 (>=10, 心跳间隔<它)")
    reaper_max_iter: int = Field(5, ge=1)
    reaper_list_limit: int = Field(100, ge=1)
    e2b_key: Optional[str] = Field(None, description="E2B API key (默认走 env)")
    e2b_api_url: Optional[str] = Field(None, description="E2B 端点 (火山方舟需显式设)")
    build_timeout: int = Field(600, ge=30, description="template build 硬超时 (秒)")

    @model_validator(mode="after")
    def _check_stale_vs_build(self) -> "SandboxConfig":
        """stale_timeout 必须 >= 10 (心跳间隔要 < 它), 已由 Field ge=10 保证;
        这里额外确保 build_timeout 合理。"""
        return self
