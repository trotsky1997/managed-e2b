"""配置加载: 从环境变量或 .env 文件读凭据, 杜绝硬编码。

环境变量优先级: 进程环境 > .env 文件。
- E2B_API_KEY / E2B_API_URL: E2B 访问
- E2B_TOS_AK / E2B_TOS_SK: 火山 TOS (mount_tos 用)
- ME2B_DB_PATH: sqlite 路径 (默认 ./me2b.db)

用法:
    # .env 文件 (不提交到 git)
    E2B_API_KEY=e2b_...
    E2B_TOS_AK=AKLT...
    E2B_TOS_SK=...

    from managed_e2b.config import load_env, get_e2b_config
    load_env()  # 读 .env
    cfg = get_e2b_config()
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def load_env(env_path: str | os.PathLike = ".env") -> None:
    """从 .env 文件加载环境变量 (不覆盖已存在的)。"""
    p = Path(env_path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip("'\"")
        if k and k not in os.environ:
            os.environ[k] = v


@dataclass
class E2BConfig:
    api_key: str
    api_url: str | None = None
    tos_ak: str | None = None
    tos_sk: str | None = None
    db_path: str = "me2b.db"

    def apply(self) -> None:
        """把配置写进环境变量 (E2B SDK / mount_tos 读这些)。"""
        os.environ["E2B_API_KEY"] = self.api_key
        if self.api_url:
            os.environ["E2B_API_URL"] = self.api_url
        if self.tos_ak:
            os.environ["E2B_TOS_AK"] = self.tos_ak
        if self.tos_sk:
            os.environ["E2B_TOS_SK"] = self.tos_sk


def get_e2b_config(load_env_first: bool = True) -> E2BConfig:
    """从环境变量组装 E2BConfig。"""
    if load_env_first:
        load_env()
    key = os.environ.get("E2B_API_KEY")
    if not key:
        from .errors import ConfigError
        raise ConfigError("E2B_API_KEY 未设置 (环境变量或 .env)")
    return E2BConfig(
        api_key=key,
        api_url=os.environ.get("E2B_API_URL") or None,
        tos_ak=os.environ.get("E2B_TOS_AK") or None,
        tos_sk=os.environ.get("E2B_TOS_SK") or None,
        db_path=os.environ.get("ME2B_DB_PATH", "me2b.db"),
    )
