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
from managed_e2b.models import State, SandboxRecord, SandboxConfig, AcquireRequest
from managed_e2b.errors import (
    Me2bError, StateTransitionError, SandboxLeakError,
    PrewarmError, ConfigError, TosError,
)
from typing import Optional

logger = logging.getLogger("sandbox_lifecycle")


# ---------------- 二进制缓存: chisel + cloudflared 共用的下载/解压/冒烟管线 ----------------
# 两个工具 (chisel = 沙箱→本地反向隧道; cloudflared = 本地→公网 quick/named tunnel)
# 都要从 GitHub release 下一个单二进制到本地缓存、按 archive 后缀解压、smoke-test --version,
# 且都可能被杀软拦 (chisel.exe 高误报)。下面的通用管线把差异收口到一个 asset 解析器 +
# 一个解压回调, 避免两份几乎相同的 ~60 行实现 (之前还 arch 映射漂移: armv7l→armv5 vs arm)。

def _normalize_platform_arch(platform: str, arch: str) -> tuple[str, str]:
    """归一化平台 + 通用 arch 别名 (x86_64→amd64, aarch64→arm64, i386/i686→386)。

    ARM 细分 (armv7l/armv6l → 各工具自己的 release arch: chisel=armv5, cloudflared=arm/armhf)
    不在这里统一, 留给各工具的 asset 表, 因为两个 release 的 ARM 命名不同 (cloudflared 是
    linux-arm / linux-armhf; chisel 是 linux_armv5), 强行统一会下到不存在的资产。
    """
    p = platform.lower()
    if p in ("win32", "cygwin", "msys"):
        p = "windows"
    a = arch.lower()
    a = {"x86_64": "amd64", "x64": "amd64", "aarch64": "arm64",
         "i386": "386", "i686": "386"}.get(a, a)
    return p, a


def _bin_cache_dir(name: str):
    """某工具的二进制缓存目录: ~/.cache/managed_e2b/<name>/。"""
    import pathlib as _pl
    cache = _pl.Path.home() / ".cache" / "managed_e2b" / name
    cache.mkdir(parents=True, exist_ok=True)
    return cache


def _extract_release_archive(suffix: str, archive_path, bin_path, cache_dir, binname: str) -> None:
    """按 archive 后缀解压到 cache_dir/bin_path。suffix ∈ {None, gz, zip, tgz}。

    None (裸二进制) 与非 tgz 的单文件后缀由 ensure_cached_binary 在 urlretrieve 时直接
    写到 bin_path, 这里只处理需要解压的 gz/zip/tgz。
    """
    import os as _o
    import tarfile, gzip, zipfile
    if suffix == "gz":
        with gzip.open(archive_path, "rb") as gz, open(bin_path, "wb") as out:
            out.write(gz.read())
    elif suffix == "zip":
        with zipfile.ZipFile(archive_path) as z:
            z.extract(binname, cache_dir)
    elif suffix == "tgz":
        with tarfile.open(archive_path, "r:gz") as tf:
            # cloudflared 的 tar 里二进制就叫 binname (无目录前缀的情况也按 basename 兜一下)
            member = next((m for m in tf.getmembers()
                           if _o.path.basename(m.name) == binname), None)
            if member is None:
                raise KeyError(f"no member named {binname!r} in {archive_path}")
            tf.extract(member, cache_dir)


def ensure_cached_binary(
    *, resolver, cache_subdir: str, label: str, smoke_timeout: int = 15,
):
    """通用: 确保本地缓存里有某 release 二进制, 返回其 Path。

    resolver: () -> (download_url, archive_suffix, binary_name) —— 工具各自的资产解析器。
    cache_subdir: ~/.cache/managed_e2b/<cache_subdir>/。
    label: 日志/报错里显示的工具名 (如 "chisel")。

    命中缓存 → smoke-test --version (杀软可能事后删/锁它, 失败就重下);
    未命中 → urlretrieve + 解压 + chmod + smoke-test。smoke-test 失败 (被杀软拦) 抛
    带"加白"引导的 RuntimeError。
    """
    import platform as _pf
    import subprocess as _sp
    import urllib.request as _ur

    url, suffix, binname = resolver(_pf.system(), _pf.machine())
    cache_dir = _bin_cache_dir(cache_subdir)
    bin_path = cache_dir / binname

    if bin_path.exists() and bin_path.stat().st_size > 1_000_000:
        try:
            _sp.run([str(bin_path), "--version"], capture_output=True,
                    timeout=smoke_timeout, check=False)
            return bin_path
        except (OSError, _sp.SubprocessError) as e:
            logger.warning(f"cached {label} at {bin_path} unusable ({e}); redownloading")
            try:
                bin_path.unlink()
            except OSError:
                pass

    logger.info(f"downloading {label} {binname} from {url}")
    # None / exe = 裸二进制 (exe 资产本身就是 .exe), 直接写到 bin_path; 否则下到 archive 再解压。
    if suffix in (None, "exe"):
        _ur.urlretrieve(url, bin_path)
    else:
        archive = cache_dir / f"{label}.{suffix}"
        _ur.urlretrieve(url, archive)
        _extract_release_archive(suffix, archive, bin_path, cache_dir, binname)
        archive.unlink(missing_ok=True)
    try:
        bin_path.chmod(0o755)
    except OSError:
        pass  # Windows 忽略

    # 下载后立刻 smoke-test: 杀软把它删了/拦了这里会失败。
    try:
        r = _sp.run([str(bin_path), "--version"], capture_output=True,
                    timeout=smoke_timeout, check=False)
    except OSError as e:
        raise RuntimeError(
            f"{label} 二进制下载后无法执行 ({e})。"
            f"很可能是杀软隔离了它 (反向隧道二进制高误报)。"
            f"请把 {bin_path} 加入杀软白名单后重试。"
        ) from e
    if r.returncode != 0 and not r.stdout:
        raise RuntimeError(
            f"{label} 跑不起来 (rc={r.returncode}, stderr={r.stderr[:200]!r})。"
            f"多半被杀软拦截, 请把 {bin_path} 加白后重试。"
        )
    return bin_path


# ---------------- chisel (沙箱→本地 反向隧道, issue #2 的成熟方案) ----------------
# chisel (jpillora/chisel): 单 Go 二进制, 原生反向隧道 (R: spec), 内建多路复用/重连/
# keepalive。对 claude-cli/Bun 等任意 HTTP 客户端完全透明 (无 --exit-on-eof 拆桥问题)。
# release: chisel_<ver>_<os>_<arch>.{gz|zip}  (linux/darwin→gz; windows→zip 含 .exe)

_CHISEL_VERSION = "1.11.8"
_CHISEL_REPO = "jpillora/chisel"

# (archive_suffix, binary_name) per normalized (os, arch); chisel 的 ARM release 叫 armv5
_CHISEL_ASSETS = {
    ("linux", "amd64"): ("gz", "chisel"), ("linux", "arm64"): ("gz", "chisel"),
    ("linux", "386"): ("gz", "chisel"), ("linux", "armv5"): ("gz", "chisel"),
    ("darwin", "amd64"): ("gz", "chisel"), ("darwin", "arm64"): ("gz", "chisel"),
    ("windows", "amd64"): ("zip", "chisel.exe"),
    ("windows", "386"): ("zip", "chisel.exe"),
    ("windows", "arm64"): ("zip", "chisel.exe"),
}


def chisel_release_asset(platform: str, arch: str) -> tuple[str, str, str]:
    """(download_url, archive_suffix, binary_name)。纯函数, 不联网。"""
    p, a = _normalize_platform_arch(platform, arch)
    # chisel 的 ARM 细分命名 (armv7l→armv5); 与 cloudflared 不同, 见 _normalize_platform_arch。
    if a in ("armv7l", "armv6l"):
        a = "armv5"
    key = (p, a)
    if key not in _CHISEL_ASSETS:
        raise ValueError(
            f"no chisel release asset for platform={platform!r} arch={arch!r} "
            f"(normalized {key}); supported: {sorted(_CHISEL_ASSETS)}")
    suffix, binname = _CHISEL_ASSETS[key]
    asset = f"chisel_{_CHISEL_VERSION}_{p}_{a}.{suffix}"
    url = (f"https://github.com/{_CHISEL_REPO}/releases/download/"
           f"v{_CHISEL_VERSION}/{asset}")
    return url, suffix, binname


def ensure_local_chisel():
    """本地有 chisel client 二进制则返回, 否则下载到 ~/.cache/managed_e2b/chisel/。"""
    return ensure_cached_binary(
        resolver=lambda pf, ar: chisel_release_asset(pf, ar),
        cache_subdir="chisel", label="chisel", smoke_timeout=10,
    )


# ---------------- cloudflared (本地→公网, quick + named tunnel) ----------------
# cloudflared (cloudflare/cloudflared): Cloudflare 官方签名二进制, 杀软基本不拦。
# 本地→公网 方向 —— 本地跑 quick tunnel 拿 trycloudflare.com 公网 URL, 或用 API token
# 走 named tunnel 绑自己的域名 (URL 稳定)。代价: 本地服务暴露到公网 (见 expose_local_cloudflare)。
# release: cloudflared-<os>-<arch>[.tgz|.exe]  (linux→裸; darwin→tgz; windows→exe)

_CFLARED_VERSION = "2026.7.3"
_CFLARED_REPO = "cloudflare/cloudflared"

# (archive_suffix|None, binary_name) per normalized (os, arch); None = 裸二进制直接下
# cloudflared 的 ARM release 叫 arm / armhf (不是 armv5/armv7)
_CFLARED_ASSETS = {
    ("linux", "amd64"): (None, "cloudflared"), ("linux", "386"): (None, "cloudflared"),
    ("linux", "arm64"): (None, "cloudflared"), ("linux", "arm"): (None, "cloudflared"),
    ("linux", "armhf"): (None, "cloudflared"),
    ("darwin", "amd64"): ("tgz", "cloudflared"), ("darwin", "arm64"): ("tgz", "cloudflared"),
    ("windows", "amd64"): ("exe", "cloudflared.exe"),
    ("windows", "386"): ("exe", "cloudflared.exe"),
    ("windows", "arm64"): ("exe", "cloudflared.exe"),
}


def cloudflared_release_asset(platform: str, arch: str) -> tuple[str, str, str]:
    """(download_url, archive_suffix|None, binary_name)。纯函数, 不联网。"""
    p, a = _normalize_platform_arch(platform, arch)
    # cloudflared 的 ARM 细分命名 (armv7l→arm, armv6l→armhf); 与 chisel 不同。
    a = {"armv7l": "arm", "armv6l": "armhf"}.get(a, a)
    key = (p, a)
    if key not in _CFLARED_ASSETS:
        raise ValueError(
            f"no cloudflared release asset for platform={platform!r} arch={arch!r} "
            f"(normalized {key}); supported: {sorted(_CFLARED_ASSETS)}")
    suffix, binname = _CFLARED_ASSETS[key]
    # None → 裸文件名 (cloudflared-linux-amd64); exe/tgz → 加后缀
    asset = f"cloudflared-{p}-{a}" if suffix is None else f"cloudflared-{p}-{a}.{suffix}"
    url = (f"https://github.com/{_CFLARED_REPO}/releases/download/"
           f"{_CFLARED_VERSION}/{asset}")
    return url, suffix, binname


def ensure_local_cloudflared():
    """本地有 cloudflared 二进制则返回, 否则下载到 ~/.cache/managed_e2b/cloudflared/。"""
    return ensure_cached_binary(
        resolver=lambda pf, ar: cloudflared_release_asset(pf, ar),
        cache_subdir="cloudflared", label="cloudflared", smoke_timeout=15,
    )


def parse_cloudflared_quick_url(stderr_text: str) -> str | None:
    """从 cloudflared quick tunnel 的 stderr 里解析 trycloudflare.com 公网 URL。

    cloudflared 把 URL 打成两行 (Visit it at: ... | https://xxx.trycloudflare.com |),
    这里直接抓 https://<...>.trycloudflare.com 即可。抓不到返回 None。
    """
    import re
    m = re.search(r"https://[a-z0-9-]+\.trycloudflare\.com", stderr_text)
    return m.group(0) if m else None


class StderrTail:
    """后台线程把一个进程的 stderr 边读边收进有界 deque, 避免无限增长 + pipe 堵塞。

    之前 quick tunnel 用闭包 `_drain` + 无界 list: 既 O(n²) (循环里 "".join 全量重扫),
    又把整个外层 frame 钉在内存里 (闭包捕获 proc/deadline 等); named tunnel 干脆没
    drain, cloudflared 日志一多就堵 stderr pipe (~64KB) 卡死隧道。本类只持 proc.stderr
    + 一个 maxlen deque, 每行可触发回调 (如 quick tunnel 在行里抓 URL), 不钉外层作用域。
    """

    def __init__(self, proc_stderr, *, maxlen: int = 200, on_line=None):
        import collections, threading
        self._buf = collections.deque(maxlen=maxlen)
        self._on_line = on_line
        self._thread = threading.Thread(target=self._run, args=(proc_stderr,), daemon=True)
        self._thread.start()

    def _run(self, proc_stderr):
        if proc_stderr is None:
            return  # 进程没开 stderr (理论上不会, 防御)
        for raw in proc_stderr:
            line = raw.decode(errors="replace")
            self._buf.append(line)
            if self._on_line is not None:
                try:
                    self._on_line(line)
                except Exception:
                    pass  # 回调失败不影响 drain

    def text(self) -> str:
        return "".join(self._buf)


# ---------------- cloudflared named tunnel (API token 模式, 稳定域名) ----------------
# 与 quick tunnel 不同: 用 CLOUDFLARE_API_TOKEN 走 REST API 建一个命名 tunnel 并绑到你
# 自己的域名 (如 adapter.yourdomain.com), URL 稳定可复用。流程 (全程非交互, 只靠环境变量):
#   1. POST   /accounts/{aid}/cfd_tunnel            {name, config_src:"cloudflare"}
#      → 返回 {id, token} (tunnel 已建)
#   2. PUT    /accounts/{aid}/cfd_tunnel/{id}/configurations
#      {config:{ingress:[{hostname, service:http://localhost:port}, {service:http_status:404}]}}
#   3. POST   /zones/{zid}/dns_records
#      {type:"CNAME", name:<hostname>, content:"<id>.cfargotunnel.com", proxied:true}
#   4. cloudflared tunnel run --token <token>      (本地常驻进程)
# 沙箱直接访问 https://<hostname> → 经 Cloudflare 边缘 → 本地 local_port。
# tunnel 复用: 同 (account, tunnel_name) 的 tunnel 已存在就 GET 列表里取, 不重建。

def _cf_named_env() -> dict | None:
    """读 named-tunnel 所需环境变量。齐全返回 dict, 否则 None (走 quick tunnel)。"""
    import os as _o
    token = _o.environ.get("CLOUDFLARE_API_TOKEN")
    account = _o.environ.get("CLOUDFLARE_ACCOUNT_ID")
    zone_name = _o.environ.get("CLOUDFLARE_ZONE_NAME")  # 如 yourdomain.com
    hostname = _o.environ.get("CLOUDFLARE_TUNNEL_HOSTNAME")  # 如 adapter.yourdomain.com
    if not (token and account and zone_name and hostname):
        return None
    return {
        "token": token, "account_id": account, "zone_name": zone_name,
        "hostname": hostname,
        "tunnel_name": _o.environ.get("CLOUDFLARE_TUNNEL_NAME", "managed-e2b"),
    }


def _cf_api(method: str, path: str, cfg: dict, *, json_body: dict | None = None):
    """调 Cloudflare REST API (api.cloudflare.com), 返回 JSON ``result`` (list|dict|str)。

    失败 (success=false 或 HTTP 错) 抛 RuntimeError。直接返回 result 原样, 不再 coerce 成 {},
    这样列表/字符串 result 能原样到达调用方 —— 之前的调用方各自 isinstance 重解包是因为
    旧 _cf_api 把 falsy result 变成了 {}。
    """
    import json as _j
    import urllib.request as _ur
    import urllib.error as _ue
    url = f"https://api.cloudflare.com/client/v4{path}"
    data = _j.dumps(json_body).encode() if json_body is not None else None
    req = _ur.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {cfg['token']}",
        "Content-Type": "application/json",
    })
    try:
        with _ur.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
    except _ue.HTTPError as e:
        body = e.read().decode(errors="replace")[:400]
        raise RuntimeError(f"Cloudflare API {method} {path} → HTTP {e.code}: {body}") from e
    payload = _j.loads(raw)
    if not payload.get("success", False):
        errs = payload.get("errors") or payload.get("messages") or []
        raise RuntimeError(f"Cloudflare API {method} {path} failed: {errs}")
    return payload.get("result")


def _cf_zone_id(cfg: dict) -> str:
    """查 zone id (按 zone_name)。带缓存 (一个 zone 名只查一次)。"""
    cache = _cf_named_state.setdefault("zone_ids", {})
    if cfg["zone_name"] in cache:
        return cache[cfg["zone_name"]]
    zones = _cf_api("GET", f"/zones?name={cfg['zone_name']}", cfg) or []
    if not zones:
        raise RuntimeError(f"Cloudflare zone {cfg['zone_name']!r} 未找到 (查 /zones?name=)")
    zid = zones[0]["id"]
    cache[cfg["zone_name"]] = zid
    return zid


# named tunnel 跨调用缓存: {(account, tunnel_name): {"tunnel_id","token"}}
_cf_named_state: dict = {}


def _cf_tunnel_token(cfg: dict, tunnel_id: str) -> str:
    """GET tunnel run token。token 端点返回的 result 可能是 str, 也可能被旧版包成 {result:"..."}。

    集中在这里处理那个 shape 不一致, 不在每个调用方重复 isinstance 解包。
    """
    token = _cf_api("GET", f"/accounts/{cfg['account_id']}/cfd_tunnel/{tunnel_id}/token", cfg)
    if isinstance(token, dict):
        token = token.get("result") or token.get("token") or ""
    return token or ""


def _cf_get_or_create_tunnel(cfg: dict) -> tuple[str, str]:
    """复用或新建命名 tunnel, 返回 (tunnel_id, run_token)。

    优先从 _cf_named_state 缓存取 (同进程多次 expose 复用); 没有就列表查同名 tunnel;
    再没有就 POST 新建。token: 新建时用返回的 token, 否则走 _cf_tunnel_token 单独 GET。
    缓存只在末尾写一处。
    """
    key = (cfg["account_id"], cfg["tunnel_name"])
    if key in _cf_named_state:
        c = _cf_named_state[key]
        return c["tunnel_id"], c["token"]

    base = f"/accounts/{cfg['account_id']}/cfd_tunnel"
    existing = _cf_api("GET", f"{base}?name={cfg['tunnel_name']}", cfg) or []
    if existing:
        tid = existing[0]["id"]
        token = _cf_tunnel_token(cfg, tid)
    else:
        r = _cf_api("POST", base, cfg, json_body={
            "name": cfg["tunnel_name"], "config_src": "cloudflare",
        })
        tid = r["id"]
        token = r.get("token") or _cf_tunnel_token(cfg, tid)

    _cf_named_state[key] = {"tunnel_id": tid, "token": token}
    return tid, token


def _cf_put_ingress(cfg: dict, tunnel_id: str, local_port: int) -> None:
    """PUT tunnel ingress 配置: hostname → http://localhost:port, 兜底 404。"""
    body = {
        "config": {
            "ingress": [
                {"hostname": cfg["hostname"], "service": f"http://localhost:{local_port}"},
                {"service": "http_status:404"},  # 必须的 terminating catch-all
            ]
        }
    }
    _cf_api("PUT", f"/accounts/{cfg['account_id']}/cfd_tunnel/{tunnel_id}/configurations",
            cfg, json_body=body)


def _cf_ensure_dns_cname(cfg: dict, tunnel_id: str) -> None:
    """确保 DNS CNAME: {hostname} → {tunnel_id}.cfargotunnel.com (proxied)。已存在则跳过。

    DNS 权限缺失 (HTTP 403 / code 10000) 不致命: token 可能只给了 Tunnel 权限而没给
    Zone:DNS:Edit。这时打 warning 给出需手动绑的 CNAME target, 然后继续 —— cloudflared
    仍会 run, 用户手动在 Cloudflare 面板绑好 CNAME 后沙箱即可访问。
    """
    zid = _cf_zone_id(cfg)
    target = f"{tunnel_id}.cfargotunnel.com"
    try:
        recs = _cf_api("GET", f"/zones/{zid}/dns_records?name={cfg['hostname']}", cfg) or []
        for r in recs:
            if r.get("type") == "CNAME" and r.get("content") == target:
                return  # 已存在, 跳过
        _cf_api("POST", f"/zones/{zid}/dns_records", cfg, json_body={
            "type": "CNAME", "name": cfg["hostname"], "content": target, "proxied": True,
        })
    except RuntimeError as e:
        msg = str(e)
        if "403" in msg or "10000" in msg or "Authentication error" in msg:
            logger.warning(
                f"cloudflared named tunnel: token 无 Zone:DNS 权限, 无法自动绑 CNAME。"
                f"请手动在 Cloudflare DNS 给 {cfg['hostname']} 绑一条 CNAME → {target} "
                f"(proxied 开)。绑好后沙箱即可访问 https://{cfg['hostname']}。"
                f"(原始错误: {msg[:160]})")
            return
        raise  # 其它错 (网络/5xx/参数) 仍抛


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
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id   TEXT PRIMARY KEY,
    name          TEXT,
    source_sid    TEXT,
    created_at    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS port_forwards (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    sandbox_id    TEXT NOT NULL,
    port          INTEGER NOT NULL,
    host          TEXT NOT NULL,
    url           TEXT NOT NULL,
    command       TEXT,
    created_at    INTEGER NOT NULL,
    FOREIGN KEY (sandbox_id) REFERENCES sandboxes(sandbox_id)
);
CREATE INDEX IF NOT EXISTS idx_pf_sandbox ON port_forwards(sandbox_id);
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
        """带状态机校验的状态转移: 非法转移(如 RUNNING→CLEANED 跳跃)抛 ValueError。
        状态副作用(running_since/last_heartbeat/killed_at)集中管理, 与 transition_to 一致。"""
        row = self.get(sid)
        if row is not None:
            cur = State(row["state"])
            if not cur.can_transition_to(state):
                raise StateTransitionError(f"非法状态转移: {cur.value} → {state.value} (sid={sid})")
        extra = {}
        if state == State.RUNNING:
            now = int(time.time())
            extra["running_since"] = now
            extra["last_heartbeat"] = now
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
            # 状态机校验 + CAS 原子性合一: 只允许 RUNNING→CLEANING 和 CLEANING→CLEANING
            # (崩溃残留重入)。拒绝 CLEANED→CLEANING 倒退(已死被当活)和其他非法转移。
            # 等价于 State.can_transition_to(CLEANING) 的 SQL 编码。
            cur = self._conn.execute(
                "UPDATE sandboxes SET state=?, killed_at=? "
                "WHERE sandbox_id=? AND state IN (?, ?)",
                (State.CLEANING.value, int(time.time()), sid,
                 State.RUNNING.value, State.CLEANING.value),
            )
            self._conn.commit()  # R1-1: 显式提交, 防 close/crash 回滚致 CLEANING claim 丢失
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

    # ---- snapshot 追踪 (持久资源, 单独表) ----
    def record_snapshot(self, snapshot_id: str, name: str = None, source_sid: str = None) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO snapshots (snapshot_id, name, source_sid, created_at) VALUES (?,?,?,?)",
                (snapshot_id, name, source_sid, int(time.time())),
            )
            self._conn.commit()

    def list_snapshots(self) -> list:
        with self._lock:
            return self._conn.execute("SELECT * FROM snapshots").fetchall()

    def forget_snapshot(self, snapshot_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
            self._conn.commit()

    # ---- 端口转发追踪 ----
    def record_port_forward(self, sandbox_id: str, port: int, host: str, url: str,
                           command: str = None) -> int:
        """记录一条端口转发, 返回自增 id。"""
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO port_forwards (sandbox_id, port, host, url, command, created_at) "
                "VALUES (?,?,?,?,?,?)",
                (sandbox_id, port, host, url, command, int(time.time())),
            )
            self._conn.commit()
            return cur.lastrowid

    def list_port_forwards(self, sandbox_id: str) -> list:
        """返回某沙箱的所有端口转发记录。"""
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM port_forwards WHERE sandbox_id=? ORDER BY port",
                (sandbox_id,),
            ).fetchall()

    def delete_port_forwards(self, sandbox_id: str) -> int:
        """删除某沙箱的所有端口转发记录, 返回删除行数。"""
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM port_forwards WHERE sandbox_id=?", (sandbox_id,),
            )
            self._conn.commit()
            return cur.rowcount

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
    lifecycle: "SandboxLifecycle" = None  # 所属 lifecycle (用于状态机/清理); fork/resume 的副本可空

    # ---- stage in/out: ephemeral 文件传输 (随沙箱销毁) ----
    def stage_in(self, files: dict, prefix: str = "/task") -> dict:
        """stage in: 把文件推进沙箱 (ephemeral)。
        files: {相对路径: content} 或 {绝对路径: content}
        prefix: 相对路径的根 (默认 /task)
        返回 {原路径: 沙箱内绝对路径}
        """
        paths = {}
        for name, content in files.items():
            dst = name if name.startswith("/") else f"{prefix}/{name}"
            parent = "/".join(dst.split("/")[:-1]) or "/"
            self.sandbox.files.make_dir(parent)
            self.sandbox.files.write(dst, content)
            paths[name] = dst
        return paths

    def stage_out(self, paths, prefix: str = "/task") -> dict:
        """stage out: 从沙箱取文件 (ephemeral)。返回 {路径: bytes}"""
        out = {}
        for p in paths:
            src = p if p.startswith("/") else f"{prefix}/{p}"
            out[p] = self.sandbox.files.read(src, format="bytes")
        return out

    def run_script(self, script: str, args: list = None, interpreter: str = None,
                   workdir: str = "/task", timeout: int = 60, env: dict = None) -> dict:
        """执行 stage in 推入的脚本。自动定位 + 推断解释器。
        返回 {stdout, stderr, exit_code}
        """
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
        r = self.sandbox.commands.run(f"cd {workdir} && {cmd}", timeout=timeout)
        return {"stdout": r.stdout or "", "stderr": r.stderr or "", "exit_code": r.exit_code}

    def run(self, command: str, timeout: int = 60) -> dict:
        """执行任意命令 (raw)。返回 {stdout, stderr, exit_code}"""
        r = self.sandbox.commands.run(command, timeout=timeout)
        return {"stdout": r.stdout or "", "stderr": r.stderr or "", "exit_code": r.exit_code}

    def mount_tos(self, bucket: str, mount_point: str = "/mnt/tos", region: str = "cn-beijing"):
        """挂载火山 TOS 桶为本地目录 (s3fs FUSE, virtual-host style)。
        需 E2B_TOS_AK / E2B_TOS_SK 环境变量。大文件/跨沙箱共享数据用。
        """
        ak = os.environ.get("E2B_TOS_AK") or os.environ.get("TOS_ACCESS_KEY")
        sk = os.environ.get("E2B_TOS_SK") or os.environ.get("TOS_SECRET_KEY")
        if not ak or not sk:
            raise TosError("mount_tos 需 E2B_TOS_AK/E2B_TOS_SK 环境变量")
        self.run("sudo apt-get update -qq >/dev/null 2>&1; sudo apt-get install -y -qq s3fs >/dev/null 2>&1", timeout=180)
        cred = f"{ak}:{sk}"
        self.run('echo ' + repr(cred) + ' | sudo tee /etc/passwd-s3fs >/dev/null; sudo chmod 600 /etc/passwd-s3fs; sudo mkdir -p ' + mount_point, timeout=30)
        self.run(f"sudo /usr/bin/s3fs {bucket} {mount_point} -o url=https://tos-s3-cn-beijing.volces.com -o endpoint={region} -o passwd_file=/etc/passwd-s3fs -o allow_other", timeout=30)

    # ---- 保存/复制/暂停 (snapshot/fork/pause) ----
    def save(self, name: str = None) -> str:
        """保存当前沙箱状态为快照 (持久, 跨沙箱可恢复)。返回 snapshot id。
        快照记入 sqlite (snapshots 表), 防 E2B 侧无限累积; cleanup_snapshots() 可清。"""
        info = self.sandbox.create_snapshot(name=name)
        snap_id = getattr(info, "id", None) or getattr(info, "snapshot_id", None)
        if snap_id and self.lifecycle:
            self.lifecycle.db.record_snapshot(snap_id, name=name, source_sid=self.sid)
        return snap_id

    def fork(self, count: int = 1, timeout: int = 60) -> list:
        """复制当前沙箱 (带状态) 为 count 个新沙箱。返回 SandboxHandle 列表。
        副本进 me2b 状态机 (写 RUNNING + _inflight), 防 atexit/reap 漏清。用完随 handle 走。"""
        forks = self.sandbox.fork(count=count, timeout=timeout)
        handles = []
        now = int(time.time())
        for f in forks:
            if isinstance(f, Exception):
                logger.warning(f"fork 失败: {f}")
                continue
            h = SandboxHandle(sid=f.sandbox_id, sandbox=f, template=self.template, lifecycle=self.lifecycle)
            if self.lifecycle:
                # 进状态机: 标 RUNNING + _inflight, atexit/reap 能清
                self.lifecycle.db.upsert(f.sandbox_id, state=State.RUNNING.value,
                                        template=self.template, created_at=now, last_heartbeat=now,
                                        metadata='{"forked_from":"' + self.sid + '"}')
                with self.lifecycle._inflight_lock:
                    self.lifecycle._inflight.add(f.sandbox_id)
            handles.append(h)
        return handles

    def pause(self, keep_memory: bool = True) -> bool:
        """暂停沙箱 (保内存), 后续 resume 恢复。火山端点可能禁用 pause。
        状态机: RUNNING→PAUSED (写 sqlite)。"""
        r = self.sandbox.pause(keep_memory=keep_memory)
        if r and self.lifecycle:
            self.lifecycle.db.set_state(self.sid, State.PAUSED)
        return r

    def resume(self, timeout: int = None) -> "SandboxHandle":
        """恢复已暂停的沙箱 (E2B 无独立 resume, 用 connect auto-resume)。
        状态机: PAUSED→RUNNING (写 sqlite + 重启心跳)。返回新 handle。"""
        Sandbox = type(self.sandbox)
        sbx = Sandbox.connect(self.sid, timeout=timeout)
        h = SandboxHandle(sid=self.sid, sandbox=sbx, template=self.template, lifecycle=self.lifecycle)
        if self.lifecycle:
            self.lifecycle.db.set_state(self.sid, State.RUNNING)
            # 重启心跳
            hb = _Heartbeat(self.lifecycle, self.sid, self.lifecycle._stale_timeout)
            hb.start()
        return h

    # ---- 端口转发: 获取沙箱端口的外部访问地址 ----
    def get_host(self, port: int) -> str:
        """获取沙箱端口的外部访问主机地址 (host:port 格式)。
        用此地址可从沙箱外部通过 HTTP/WebSocket 连接到沙箱内端口。"""
        return self.sandbox.get_host(port)

    def get_url(self, port: int, scheme: str = "https") -> str:
        """获取沙箱端口的外部访问完整 URL。
        scheme: http 或 https (默认 https)"""
        return f"{scheme}://{self.get_host(port)}"

    def expose_port(self, port: int, command: str = None, allow_public: bool = True):
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
            self.sandbox.commands.run(f"{command} &", background=True, timeout=5)
        if allow_public:
            try:
                from e2b import SandboxNetworkUpdate
                self.sandbox.update_network(
                    SandboxNetworkUpdate(allow_internet_access=True)
                )
            except Exception as e:
                logger.warning(f"update_network failed (non-fatal): {e}")
        host = self.get_host(port)
        pf = PortForward(
            port=port,
            host=host,
            url=f"https://{host}",
            command=command,
            sandbox_id=self.sid,
        )
        # 落盘: 端口转发生命周期追踪 (随沙箱销毁自动失效, 记录用于审计/查询)
        if self.lifecycle:
            self.lifecycle.db.record_port_forward(self.sid, port, host, pf.url, command)
        return pf

    def list_ports(self) -> list:
        """列出当前沙箱已暴露的端口 (从 sqlite 查)。返回 PortForward 列表。"""
        from managed_e2b.models import PortForward
        if not self.lifecycle:
            return []
        rows = self.lifecycle.db.list_port_forwards(self.sid)
        return [PortForward(
            port=r["port"], host=r["host"], url=r["url"],
            command=r["command"], sandbox_id=r["sandbox_id"],
        ) for r in rows]

    def close_port(self, port: int) -> bool:
        """关闭沙箱端口: kill 沙箱内监听该端口的进程 + 删除 sqlite 记录。
        返回是否找到并关闭了记录。"""
        if not self.lifecycle:
            return False
        rows = self.lifecycle.db.list_port_forwards(self.sid)
        found = any(r["port"] == port for r in rows)
        if not found:
            return False
        # kill 沙箱内监听该端口的进程 (best-effort, 失败不阻塞)
        try:
            self.sandbox.commands.run(
                f"sh -c 'fuser -k {port}/tcp 2>/dev/null; pkill -f :{port} 2>/dev/null; true'",
                timeout=10,
            )
        except Exception as e:
            logger.warning(f"close_port kill process on {port} failed (non-fatal): {e}")
        # 删 sqlite 记录
        with self.lifecycle.db._lock:
            self.lifecycle.db._conn.execute(
                "DELETE FROM port_forwards WHERE sandbox_id=? AND port=?",
                (self.sid, port),
            )
            self.lifecycle.db._conn.commit()
        return True

    # ---- 本地端口隧道: 让沙箱访问本地服务 ----
    def tunnel_local_http(self, tunnel_url: str, alias: str = "local-api"):
        """配置沙箱通过公网隧道 URL 访问本地服务 (cloudflared/ngrok 方案)。

        在沙箱内 /etc/hosts 添加 alias → tunnel_url 的映射, 使本地服务
        可以通过友好域名访问。tunnel_url 是本地 cloudflared/ngrok 输出的公网地址。

        Args:
            tunnel_url: 公网隧道 URL (如 https://xxx.trycloudflare.com)
            alias: 沙箱内 /etc/hosts 的别名 (默认 "local-api")

        Returns:
            dict: {alias, tunnel_url, sandbox_host} — sandbox_host 是沙箱内访问地址
        """
        # 提取 host (去掉 scheme)
        host = tunnel_url.replace("https://", "").replace("http://", "").rstrip("/")
        # 在沙箱内 /etc/hosts 添加映射 (best-effort, 失败不阻塞)
        try:
            self.sandbox.commands.run(
                f"echo '127.0.0.1 {alias}' | sudo tee -a /etc/hosts",
                timeout=10,
            )
        except Exception as e:
            logger.warning(f"tunnel_local_http /etc/hosts failed (non-fatal): {e}")
        return {
            "alias": alias,
            "tunnel_url": tunnel_url,
            "sandbox_host": host,
        }

    def expose_local(
        self,
        local_port: int,
        sandbox_port: int | None = None,
        *,
        chisel_port: int = 8082,
        chisel_token: str | None = None,
    ) -> "PortForward":
        """将本地服务反向转发到沙箱内 (沙箱内 ``http://127.0.0.1:sandbox_port``
        → 本地的 local_port)。沙箱无需公网 IP / 隧道。

        用 jpillora/chisel —— 单 Go 二进制, 原生反向隧道 (R: spec), 内建多路复用/
        重连/keepalive/auth token, 对 claude-cli/Bun 等任意 HTTP 客户端完全透明
        (issue #2 的旧 SSH+websocat 路径会被 ``--exit-on-eof`` 在 Bun 的 keep-alive
        POST body 读到一半时拆桥 → ``Connection lost``; chisel 无此问题)。

        - 沙箱内: 下载 linux chisel server (Linux 不被杀软拦), 跑
          ``chisel server --reverse --port <chisel_port> --auth e2b:<token>``;
          暴露 chisel_port (E2B 网关 TLS 终结 → 本地拨 wss://)。
        - 本地: ensure_local_chisel() 拿到本地 chisel client (下载到
          ~/.cache/managed_e2b/chisel/), 跑
          ``chisel client --auth e2b:<token> wss://<host> R:127.0.0.1:<sandbox_port>:127.0.0.1:<local_port>``。

        **Windows 上 chisel.exe 可能被杀软误报隔离** (Go 反向隧道二进制高误报)。
        ensure_local_chisel() 下载后会立刻 smoke-test ``--version``; 被拦时抛带加白
        引导的 RuntimeError —— 把 chisel.exe 加白后重试即可。

        前提: 本地+沙箱能从 github.com 下载 chisel 二进制。

        Args:
            local_port: 本地要暴露的端口号 (如 18080 = anyharness adapter)
            sandbox_port: 沙箱内映射端口 (默认 = local_port)
            chisel_port: chisel 在沙箱内监听的 WSS 端口 (默认 8082, 会自动暴露)
            chisel_token: chisel auth token (默认自动生成)

        Returns:
            PortForward: {port, host, url} — sandbox_port 是沙箱内访问端口
        """
        if sandbox_port is None:
            sandbox_port = local_port
        return self._expose_local_chisel(
            local_port, sandbox_port, chisel_port=chisel_port, chisel_token=chisel_token)

    def expose_local_cloudflare(self, local_port: int) -> "PortForward":
        """用 cloudflared 把本地服务暴露到一个公网 URL, 沙箱直接访问该 URL 即命中本地。

        与 expose_local (chisel, 沙箱内走 127.0.0.1:port) **方向不同**: cloudflared 是
        本地→公网 —— 本地跑 cloudflared 拿一个公网 URL, 沙箱 curl 该 URL 到达本地。
        优点: cloudflared 是 Cloudflare 官方签名二进制, 杀软基本不拦; 不需要沙箱侧装
        任何东西 (沙箱直接访问公网 URL)。

        两种模式 (按环境变量自动选):

        - **named tunnel** (有 key 时, 稳定域名): 设了以下 4 个环境变量就走它 ——
          ``CLOUDFLARE_API_TOKEN`` + ``CLOUDFLARE_ACCOUNT_ID`` +
          ``CLOUDFLARE_ZONE_NAME`` (如 yourdomain.com) +
          ``CLOUDFLARE_TUNNEL_HOSTNAME`` (如 adapter.yourdomain.com)。
          可选 ``CLOUDFLARE_TUNNEL_NAME`` (默认 "managed-e2b")。
          用 REST API 建/复用命名 tunnel、配 ingress (hostname → localhost:port)、
          建 DNS CNAME, 再本地 ``cloudflared tunnel run --token <token>``。
          URL 稳定不变, 进程重启后复用; 适合长期/反复评测。代价: 本地服务暴露到公网
          (建议在适配器侧加 token auth, 如 anyharness 的 ADAPTER_AUTH)。

        - **quick tunnel** (默认, 无 key): ``cloudflared tunnel --url http://localhost:port``,
          拿一个随机 trycloudflare.com URL。URL 每次随机、无 auth 裸奔, 仅适合临时/
          可信环境, 用完即停。

        本地需能从 github.com 下载 cloudflared 二进制; 沙箱需能访问公网 (E2B 默认允许)。

        Args:
            local_port: 本地要暴露的端口号 (如 18080 = anyharness adapter)

        Returns:
            PortForward: host/url 是公网地址 (沙箱直接访问); port 字段填 local_port
            仅供记录, 沙箱访问用的是 url。
        """
        cfg = _cf_named_env()
        if cfg is not None:
            return self._cf_named_tunnel(local_port, cfg)
        # 无 key → quick tunnel。issue #6: 它在真实 agent 负载 (claude keep-alive + 大 body)
        # 下不稳, 边缘→origin 连接会断 → HTTP 530 / tunnel_error 1033; 这里 warn 引导用
        # named tunnel (设下面 4 个环境变量) 拿稳定域名。持续自检 (_probe_url_sustained)
        # 能更早暴露 flaky, 但 trycloudflare 本质不可靠, 无法靠自检根治。
        logger.warning(
            "expose_local_cloudflare: 未设 CLOUDFLARE_API_TOKEN 等, 走 quick tunnel "
            "(随机 trycloudflare URL)。它无 auth 且在真实 agent 负载 (claude keep-alive "
            "+ 大 body) 下可能 HTTP 530 / tunnel_error 1033 (issue #6)。跑真实任务请设 "
            "CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID + CLOUDFLARE_ZONE_NAME + "
            "CLOUDFLARE_TUNNEL_HOSTNAME 走 named tunnel (稳定域名)。")
        return self._cf_quick_tunnel(local_port)

    def _probe_url_ready(self, url: str, *, proc=None, tries: int = 20,
                         sleep_s: float = 2.0, curl_timeout: int = 8,
                         extra_curl: str = "", fail_msg: str) -> None:
        """从沙箱 curl ``url`` 直到拿到非 000 的 HTTP 状态 (隧道通了), 否则抛 RuntimeError。

        chisel / cloudflared quick / cloudflared named 三条隧道启动后都用这套自检, 之前是
        三份几乎一样的循环。``proc`` 给了会在每轮先查进程是否已死 (死则带 stderr 尾抛错)。
        ``extra_curl`` 可追加 `-X POST -d '{}'` 等 (chisel 用)。
        """
        import time as _t
        curl = (
            f"curl -sS -m {curl_timeout} -o /dev/null -w 'HTTP%{{http_code}}' "
            f"{url}/ {extra_curl} 2>/dev/null || true"
        )
        for _ in range(tries):
            if proc is not None and proc.poll() is not None:
                err = (proc.stderr.read().decode(errors="replace")
                       if proc.stderr else "") if proc is not None else ""
                raise RuntimeError(f"{fail_msg}: 进程已退出 (rc={proc.returncode}); stderr: {err[:300]}")
            r = self.run(curl, timeout=curl_timeout + 12)
            out = (r.get("stdout", "") if r else "")
            if "HTTP" in out and "HTTP000" not in out:
                return
            _t.sleep(sleep_s)
        if proc is not None:
            proc.terminate()
        raise RuntimeError(fail_msg)

    def _probe_url_sustained(self, url: str, *, proc=None, need: int = 3,
                             interval_s: float = 3.0, curl_timeout: int = 8,
                             extra_curl: str = "", deadline_s: int = 40,
                             fail_msg: str) -> None:
        """严格自检: 在 deadline_s 内, 连续 ``need`` 次探针都拿到 2xx/3xx (非 000、非 5xx)
        才算隧道"持续可用", 否则抛 RuntimeError。

        issue #6: trycloudflare quick tunnel 的 edge→origin 连接在真实负载 (claude keep-alive
        + 大 body) 下会断, 但单次短 curl 能蹭到一个连通窗口骗过 _probe_url_ready → 返回 URL
        后几秒就 530/1033。本方法要求连续多次都通, flaky 隧道会在 expose_local_cloudflare
        阶段更快暴露 (仍非根治, trycloudflare 本质不可靠)。5xx (含 530 tunnel_error) 算失败。
        """
        import time as _t
        curl = (
            f"curl -sS -m {curl_timeout} -o /dev/null -w 'HTTP%{{http_code}}' "
            f"{url}/ {extra_curl} 2>/dev/null || true"
        )
        streak = 0
        deadline = _t.time() + deadline_s
        last_out = ""
        while _t.time() < deadline:
            if proc is not None and proc.poll() is not None:
                err = (proc.stderr.read().decode(errors="replace")
                       if proc.stderr else "") if proc is not None else ""
                raise RuntimeError(f"{fail_msg}: 进程已退出 (rc={proc.returncode}); stderr: {err[:300]}")
            r = self.run(curl, timeout=curl_timeout + 12)
            out = (r.get("stdout", "") if r else "")
            last_out = out
            # 隧道通 = 拿到任何非 000、非 530 的 HTTP 响应。000 = curl 连不上 (隧道没通/DNS 没解析);
            # 530 = Cloudflare 边缘到 cloudflared 断 (tunnel_error)。但 404/401/501/502 等说明
            # 请求到了适配器 (隧道是通的) —— 真实适配器 GET / 往往 404, 501 也只是方法不支持。
            if "HTTP" in out and "HTTP000" not in out and "HTTP530" not in out:
                streak += 1
                if streak >= need:
                    return
            else:
                streak = 0
            _t.sleep(interval_s)
        if proc is not None:
            proc.terminate()
        raise RuntimeError(
            f"{fail_msg}: 在 {deadline_s}s 内未连续 {need} 次探到非 000/非 5xx 响应 "
            f"(最后一次: {last_out[:60]!r})。trycloudflare quick tunnel 的边缘连接不稳, "
            f"真实 agent 负载下会 530; 建议设 CLOUDFLARE_API_TOKEN 等走 named tunnel。")

    @staticmethod
    def _wait_ha_connections(metrics_url: str, *, deadline_s: int = 30,
                              poll_s: float = 0.5) -> None:
        """轮询 cloudflared 的 --metrics 端点, 等 ``cloudflared_tunnel_ha_connections >= 1``
        再返回 —— 即 cloudflared 已向 Cloudflare 边缘注册了 HA (QUIC) 连接。

        issue #6 的根本修法: 530/tunnel_error 1033 的成因是 cloudflared→边缘的连接没建好
        (ha_connections=0), 而旧的 URL 自检探的是"边缘是否路由到 origin", 它能在 cloudflared
        还没注册时蹭到假连通窗口, 返回一个马上就死的 URL。先等 ha_connections≥1 (本地 HTTP 查,
        秒级、不耗公网) 再做 URL 自检, 从根上消除了假阳性。超时抛 RuntimeError。
        """
        import time as _t
        import urllib.request as _ur
        import re as _re
        deadline = _t.time() + deadline_s
        last = ""
        while _t.time() < deadline:
            try:
                with _ur.urlopen(metrics_url, timeout=2) as resp:
                    last = resp.read().decode(errors="replace")
                m = _re.search(r"^cloudflared_tunnel_ha_connections\s+(\d+)", last, _re.M)
                if m and int(m.group(1)) >= 1:
                    return
            except OSError:
                pass  # metrics 还没起 / cloudflared 没开 --metrics
            _t.sleep(poll_s)
        raise RuntimeError(
            f"cloudflared 在 {deadline_s}s 内未建立到边缘的 HA 连接 "
            f"(cloudflared_tunnel_ha_connections 一直为 0)。metrics: {metrics_url}。"
            f"last metrics: {last[:200]!r}")

    @staticmethod
    def _cf_edge_diag(stderr_text: str) -> str:
        """扫 cloudflared stderr, 若发现"连不上边缘"的错误, 返回针对性诊断提示。

        实测场景 (issue #6 真机验证): 本机装了 Cloudflare WARP / 其它 VPN 路由了
        198.18.0.0/16, 跟 cloudflared 抢边缘路由 → QUIC 拨号 timeout / TLS 握手 EOF,
        ha_connections gauge 仍报 1 (误导), 但公网 URL 530。这时给清晰诊断, 免得用户
        以为是代码 bug。无匹配返回空串。
        """
        t = stderr_text or ""
        if "Failed to dial" in t or "no recent network activity" in t:
            return (
                "  [诊断] cloudflared 拨不上 Cloudflare 边缘 (QUIC/UDP 7844)。"
                "常见原因: 本机的 Cloudflare WARP 或 VPN 路由了 198.18.0.0/16, 跟 "
                "cloudflared 抢边缘路由。试: 关掉 WARP/VPN, 或在 cloudflared 命令加 "
                "--protocol http2 走 TCP 443 回退。")
        if "TLS handshake with edge" in t:
            return (
                "  [诊断] cloudflared 到边缘的 TLS 握手失败 (EOF)。常见原因同上 "
                "(WARP/VPN 劫持 198.18.0.0/16, 或出站 443/7844 被防火墙挡)。"
                "试: 关 WARP/VPN, 或加 --protocol http2, 或放行出站 443 + UDP 7844。")
        return ""

    @staticmethod
    def _warn_if_warp_routes() -> None:
        """启动 cloudflared 前预检: 本机是否把 198.18.0.0/16 (Cloudflare WARP 虚拟段)
        路由到了某个网卡。若是, 提前 warning —— cloudflared 出站连 Cloudflare 边缘会走
        该虚拟网卡, 跟 WARP 抢路由, 导致 QUIC 拨号失败、公网 URL 530/000。

        代码层面 cloudflared 不支持选源网卡, 所以无法强制绕开 (那是 OS 路由层的事);
        只能提前检测 + 引导用户关掉 WARP/VPN 或在 Cloudflare WARP 设置里 split-tunnel 排除
        cloudflared 的边缘 IP。非 Windows / 查不到路由表则静默跳过。
        """
        import sys as _sys
        if not _sys.platform.startswith("win"):
            return  # 跨平台路由检测各自不同, 这里只覆盖最常见的 Windows + WARP 场景
        import subprocess as _sp
        try:
            r = _sp.run(
                ["powershell", "-NoProfile", "-Command",
                 "Get-NetRoute -DestinationPrefix '198.18.0.0/16' -ErrorAction SilentlyContinue | "
                 "Select-Object -ExpandProperty InterfaceAlias -Unique"],
                capture_output=True, timeout=5, check=False,
            )
            out = r.stdout.decode(errors="replace").strip()
        except (OSError, _sp.SubprocessError):
            return  # 没装 powershell / 沙箱环境: 跳过
        if out:
            logger.warning(
                f"expose_local_cloudflare: 检测到 198.18.0.0/16 路由走网卡 [{out.splitlines()[0]}] —— "
                f"这通常是本机的 Cloudflare WARP 或 VPN 虚拟网卡。cloudflared 出站连 Cloudflare 边缘 "
                f"会走该网卡并与之抢路由, 导致隧道 530/连不上。请先关掉 WARP/VPN, 或在 WARP 的 "
                f"Split Tunnel 设置里排除 Cloudflare 边缘 IP, 再重试。")

    def _cf_named_tunnel(self, local_port: int, cfg: dict) -> "PortForward":
        """named tunnel: API token 建隧道 + 绑定自定义域名, URL 稳定。见 expose_local_cloudflare。"""
        import subprocess as _sp

        cflared = ensure_local_cloudflared()
        self._warn_if_warp_routes()  # 预检: WARP/VPN 路由 198.18.0.0/16 会抢边缘路由
        # 1-3. 建/复用 tunnel + 配 ingress + DNS CNAME (REST API, 非交互)
        tunnel_id, run_token = _cf_get_or_create_tunnel(cfg)
        if not run_token:
            raise RuntimeError(
                f"无法从 Cloudflare API 拿到 tunnel {cfg['tunnel_name']!r} 的运行 token。"
                f"确认 CLOUDFLARE_API_TOKEN 有 'Cloudflare Tunnel' 写权限。")
        _cf_put_ingress(cfg, tunnel_id, local_port)
        _cf_ensure_dns_cname(cfg, tunnel_id)

        # 4. 本地常驻 cloudflared, 拨隧道; --metrics 查 ha_connections; StderrTail 防 pipe 堵
        import socket as _sock
        msock = _sock.socket(); msock.bind(("127.0.0.1", 0)); metrics_port = msock.getsockname()[1]
        msock.close()
        metrics_url = f"http://127.0.0.1:{metrics_port}/metrics"
        proc = _sp.Popen(
            [str(cflared), "tunnel", "--metrics", f"127.0.0.1:{metrics_port}",
             "run", "--token", run_token],
            stdin=_sp.DEVNULL, stdout=_sp.PIPE, stderr=_sp.PIPE,
        )
        _tail = StderrTail(proc.stderr)
        public_url = f"https://{cfg['hostname']}"

        # 根本自检: 等 cloudflared 向边缘注册 HA 连接 (ha_connections≥1), 再做 URL 自检。
        # 与 quick tunnel 同理 (issue #6): 避免 URL 自检蹭假窗口返回死 URL。
        self._wait_ha_connections(metrics_url, deadline_s=30)
        import time as _t; _t.sleep(1)

        # 兜底: 沙箱 curl 公网 URL (DNS 传播 + 边缘生效要几秒)
        try:
            self._probe_url_ready(
                public_url, proc=proc,
                fail_msg=f"cloudflared named tunnel {public_url} 在 40s 内未被沙箱访问通; DNS/边缘可能还在传播。"
                f" stderr: {_tail.text()[:300]}",
            )
        except RuntimeError as _e:
            diag = self._cf_edge_diag(_tail.text())
            if diag:
                raise RuntimeError(f"{_e}\n{diag}") from None
            raise

        from managed_e2b.models import PortForward
        return PortForward(
            port=local_port, host=cfg["hostname"], url=public_url, sandbox_id=self.sid,
        )

    def _cf_quick_tunnel(self, local_port: int) -> "PortForward":
        """quick tunnel: 无 key, 随机 trycloudflare URL。见 expose_local_cloudflare。"""
        import socket as _sock
        import subprocess as _sp

        cflared = ensure_local_cloudflared()
        self._warn_if_warp_routes()  # 预检: WARP/VPN 路由 198.18.0.0/16 会抢边缘路由
        # 选一个本地空闲端口给 cloudflared 的 --metrics (查 ha_connections 用, issue #6 根本修法)
        msock = _sock.socket(); msock.bind(("127.0.0.1", 0)); metrics_port = msock.getsockname()[1]
        msock.close()
        metrics_url = f"http://127.0.0.1:{metrics_port}/metrics"
        proc = _sp.Popen(
            [str(cflared), "tunnel", "--metrics", f"127.0.0.1:{metrics_port}",
             "--url", f"http://localhost:{local_port}"],
            stdin=_sp.DEVNULL, stdout=_sp.PIPE, stderr=_sp.PIPE,
        )
        # 边读 stderr 边按行抓 trycloudflare URL; StderrTail 有界 deque + 行回调, 不钉外层作用域,
        # 也不 O(n²) 重扫全量。URL 抓到用 Event 通知。
        import threading as _th
        found = _th.Event()
        holder: dict[str, str | None] = {"url": None}
        def _on_line(line):
            if not holder["url"]:
                u = parse_cloudflared_quick_url(line)
                if u:
                    holder["url"] = u
                    found.set()
        _tail = StderrTail(proc.stderr, on_line=_on_line)
        # 等 cloudflared 拿到 trycloudflare URL **且** 向边缘注册了 HA 连接 (ha_connections≥1)。
        # URL 抓到不代表隧道可用 —— issue #6: 530/1033 是 cloudflared→边缘连接没建好,
        # 必须等 ha_connections≥1 才算真的通了。两者并行等, 都满足才继续。
        import time as _t
        url_ok = found.wait(timeout=30)
        if not url_ok and proc.poll() is not None:
            raise RuntimeError(
                f"cloudflared quick tunnel 退出 (rc={proc.returncode})。stderr: {_tail.text()[:400]}")
        if not url_ok:
            proc.terminate()
            raise RuntimeError(f"cloudflared 30s 内没拿到 trycloudflare URL。stderr: {_tail.text()[:400]}")
        public_url = holder["url"]
        if proc.poll() is not None:
            raise RuntimeError(
                f"cloudflared quick tunnel 退出 (rc={proc.returncode})。stderr: {_tail.text()[:400]}")
        # 根本自检: 等 ha_connections≥1 (cloudflared 真连上边缘), 再做 URL 持续探针。
        # 这一步把"蹭假窗口返回死 URL"的假阳性从根上消掉 (issue #6 的治本, sustained 是治标)。
        self._wait_ha_connections(metrics_url, deadline_s=30)
        _t.sleep(1)  # 边缘把新连接加入路由表也要一两秒

        # 再做一次持续 URL 自检作为兜底 (ha_connections≥1 后仍可能短暂 530, 多探几下)。
        try:
            self._probe_url_sustained(
                public_url, proc=proc, need=3, interval_s=3.0, curl_timeout=8, deadline_s=40,
                fail_msg=f"cloudflared quick tunnel {public_url} 自检失败",
            )
        except RuntimeError as _e:
            diag = self._cf_edge_diag(_tail.text())
            if diag:
                raise RuntimeError(f"{_e}\n{diag}") from None
            raise

        from managed_e2b.models import PortForward
        host = public_url[len("https://"):] if public_url.startswith("https://") else public_url
        return PortForward(
            port=local_port, host=host, url=public_url, sandbox_id=self.sid,
        )

    def _expose_local_chisel(
        self, local_port: int, sandbox_port: int, *,
        chisel_port: int = 8082, chisel_token: str | None = None,
    ) -> "PortForward":
        """chisel 反向隧道 (expose_local 的唯一实现, issue #2 的成熟方案)。

        沙箱侧: 下载 chisel linux 二进制, 后台跑 ``chisel server --reverse --port
        chisel_port --auth e2b:<token>``; 暴露 chisel_port。
        本地侧: ensure_local_chisel() 拿到本地 chisel client, 后台跑
        ``chisel client --auth e2b:<token> wss://<host> R:127.0.0.1:<sandbox_port>:127.0.0.1:<local_port>``。
        自检: 沙箱内 curl sandbox_port 必须命中本地。
        """
        import subprocess as _sp
        import secrets as _sec

        token = chisel_token or _sec.token_hex(16)

        # 1. 沙箱内下载 + 启动 chisel server (--reverse 允许客户端提反向隧道)
        self.run(
            "set -e; "
            "mkdir -p /root/bin && cd /root/bin; "
            "if ! [ -x ./chisel ]; then "
            "  curl -sL https://github.com/jpillora/chisel/releases/download/"
            f"v{_CHISEL_VERSION}/chisel_{_CHISEL_VERSION}_linux_amd64.gz -o chisel.gz && "
            "  gunzip -f chisel.gz && chmod +x chisel; "
            "fi",
            timeout=60,
        )
        self.run(
            f"nohup /root/bin/chisel server --reverse --port {chisel_port} "
            f"--auth e2b:{token} > /tmp/chisel_server.log 2>&1 & echo PID=$!",
            timeout=5,
        )
        import time as _t; _t.sleep(2)

        # 2. 暴露 chisel_port (E2B 网关 TLS 终结 → 本地拨 wss://)
        self.expose_port(chisel_port)
        chisel_host = self.get_host(chisel_port)
        server_url = f"wss://{chisel_host}"

        # 3. 本地确保有 chisel client (下载到缓存; 杀软拦截会在这抛带引导的错)
        local_chisel = ensure_local_chisel()

        # 4. 本地后台启动 chisel client, 建 R: 反向隧道
        #    R:<sandbox 侧监听 iface>:<sandbox 侧监听 port>:<本地目标 host>:<本地目标 port>
        client_cmd = [
            str(local_chisel), "client",
            "--auth", f"e2b:{token}",
            server_url,
            f"R:127.0.0.1:{sandbox_port}:127.0.0.1:{local_port}",
        ]
        client_proc = _sp.Popen(
            client_cmd, stdin=_sp.DEVNULL,
            stdout=_sp.PIPE, stderr=_sp.PIPE,
        )
        import time as _t2; _t2.sleep(3)
        if client_proc.poll() is not None:
            err = client_proc.stderr.read().decode(errors="replace")
            if "not recognized" in err or "Access is denied" in err or not err:
                raise RuntimeError(
                    f"chisel client 启动失败 (rc={client_proc.returncode})。"
                    f"很可能是杀软隔离了 {local_chisel}。请把它加入杀软白名单后重试。"
                    f"stderr: {err[:300]}"
                )
            raise RuntimeError(f"chisel client failed to start: {err[:300]}")

        # 5. 自检: 沙箱内 curl sandbox_port 必须能命中本地 (隧道双向建立)
        try:
            self._probe_url_ready(
                f"http://127.0.0.1:{sandbox_port}", tries=10, sleep_s=1.0,
                curl_timeout=5, extra_curl="-X POST -d '{}'",
                fail_msg=f"chisel 隧道自检失败 (沙箱 127.0.0.1:{sandbox_port} 在 10s 内"
                         f"未把探测流量送到本地 :{local_port})",
            )
        except RuntimeError:
            log = self.run("tail -20 /tmp/chisel_server.log 2>&1", timeout=10)
            raise RuntimeError(
                f"chisel 隧道自检失败 (沙箱 127.0.0.1:{sandbox_port} 在 10s 内未把"
                f"探测流量送到本地 :{local_port})。sandbox chisel 日志: "
                f"{(log['stdout'] if log else '')[:300]}"
            )

        from managed_e2b.models import PortForward
        return PortForward(
            port=sandbox_port,
            host=f"127.0.0.1:{sandbox_port}",
            url=f"http://127.0.0.1:{sandbox_port}",
            sandbox_id=self.sid,
        )


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
        # pydantic 校验层: 参数集中校验 (ge/gt/stale>=10), 替代手写 raise
        cfg = SandboxConfig(db_path=db_path, max_concurrent=max_concurrent,
                            create_rate=create_rate, max_build_concurrency=max_build_concurrency,
                            stale_timeout=stale_timeout, reaper_max_iter=reaper_max_iter,
                            reaper_list_limit=reaper_list_limit, e2b_key=e2b_key, e2b_api_url=e2b_api_url)
        # 端点 + key 都下沉到 env (SDK 每次调用读 os.getenv):
        if e2b_api_url:
            os.environ["E2B_API_URL"] = e2b_api_url
        if e2b_key:
            os.environ["E2B_API_KEY"] = e2b_key
        self.db = SandboxDB(db_path)
        # 三个独立 limiter: 资源不同, 不能混用一把锁
        self._build_limiter = Limiter(max_build_concurrency)
        self._create_limiter = RateLimiter(create_rate)  # 按创建速率限流(#7)
        self._run_limiter = Limiter(max_concurrent)  # 同时 RUNNING 的沙箱
        self._key = e2b_key or os.environ.get("E2B_API_KEY")
        if not self._key:
            raise ConfigError("E2B_API_KEY 未设置")
        self.reaper_max_iter = reaper_max_iter
        self.reaper_list_limit = reaper_list_limit
        self._stale_timeout = stale_timeout  # 已由 SandboxConfig 校验 >=10
        self._reaper_lock = threading.Lock()
        # atexit 必须在构造时注册: 只用 acquire 不调 reap 的进程
        # 退出时也要清理 RUNNING 沙箱, 否则就是"孤儿进程危机"复现。
        atexit.register(self.shutdown)
        # template 预热: 去重 + 并发 build 锁
        self._template_ready: dict[str, bool] = {}      # name -> 已就绪
        self._template_alias: dict[str, str] = {}    # canonical -> renamed (R2-3: ERROR 换名后映射)
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
            raise ConfigError("prewarm 需要 image 或 dockerfile")
        name = self.template_name_for(image, dockerfile)
        # 内存级快路径: 本进程已确认就绪, 直接返回 (省一次 exists RPC)
        # R2-3: 若曾被 rename, 返回 renamed 名(_template_alias), 否则 canonical
        if self._template_ready.get(name):
            return self._template_alias.get(name, name)
        lock = self._template_lock(name)
        with lock:  # 同一 template 的 build 串行, 不同 template 不互斥
            # double-check: 拿到锁后可能已被别的线程 build 好
            if self._template_ready.get(name):
                return self._template_alias.get(name, name)
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
                        # R2-3: canonical 指向 renamed, 快路径返回 renamed 名(而非损坏的 canonical)
                        if name_to_use != name:
                            self._template_ready[name] = True
                            self._template_alias[name] = name_to_use
                        logger.info(f"template {name_to_use} build 完成")
                        return name_to_use
                    except Exception as e:
                        last_err = e
                        logger.warning(f"template {name_to_use} build 第{attempt+1}次失败: {e}")
                raise PrewarmError(f"prewarm build {name_to_use} 失败: {last_err}") from last_err

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
                raise PrewarmError(f"template {name} build 失败: {s}")
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
    def _create(self, template: str, timeout: int, metadata: dict,
                allow_internet_access: bool = True, network: dict = None) -> SandboxHandle:
        """create 沙箱并落盘。关键: 先 create 拿到真实 id, 再写 db;
        若 create 成功但写 db 失败, 仍要 kill 沙箱避免孤儿(见 acquire 的 except)。
        allow_internet_access/network 透传给 E2B create (控制沙箱网络)。"""
        Sandbox = self._sandbox_cls()
        sbx = Sandbox.create(template=template, timeout=timeout, metadata=metadata,
                             allow_internet_access=allow_internet_access, network=network)
        real_id = sbx.sandbox_id
        # 直接用真实 id 落盘 (不要临时 id + rename, 那套在并发下易竞态/漏字段)
        try:
            now = int(time.time())
            # 用 SandboxRecord 校验字段一致性(时间戳/类型), 再序列化落盘
            rec = SandboxRecord(sandbox_id=real_id, state=State.RUNNING,
                                template=template, created_at=now,
                                last_heartbeat=now, metadata=str(metadata))
            self.db.upsert(real_id, **rec.to_db_row())
        except Exception as e:
            # db 写失败但沙箱已建 → 必须 kill, 否则真孤儿
            logger.error(f"db 写入失败, 回收沙箱 {real_id}: {e}")
            try:
                sbx.kill()
            except Exception:
                pass
            raise
        return SandboxHandle(sid=real_id, sandbox=sbx, template=template, lifecycle=self)

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
                # 外部 sid 不在 DB: 用 SandboxRecord 校验后落盘 CLEANED
                now = int(time.time())
                rec = SandboxRecord(sandbox_id=sid, state=State.CLEANED,
                                    template="(foreign)", created_at=now, killed_at=now)
                self.db.upsert(sid, **rec.to_db_row())
            else:
                self.db.set_state(sid, State.CLEANED)
                # 清理该沙箱的端口转发记录 (沙箱已死, 端口自然失效)
                self.db.delete_port_forwards(sid)
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
                # R2-2: 也认 400: Invalid sandbox ID (格式无效/从未存在的 id, 如崩溃残留的 fake-id)
                if ("notfound" in en.lower() or "not found" in msg
                        or "sandboxnotfound" in en.lower()
                        or "invalid sandbox id" in msg or "400" in msg and "invalid" in msg):
                    return True
                # 网络/超时/5xx → 不确定, 重试
                logger.warning(f"_confirm_dead {sid} 瞬时错误(重试): {en}: {str(e)[:80]}")
            time.sleep(0.5)
        return False

    # ---- 上下文管理器: 完整生命周期 ----
    @contextmanager
    def acquire(self, image: Optional[str] = None, dockerfile: Optional[str] = None,
                template: Optional[str] = None, timeout: int = 1800,  # #6: 默认1800s(评测常>5min); E2B到点自动kill
                metadata: Optional[dict] = None,
                allow_internet_access: bool = True, network: dict = None):
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
        # pydantic 校验: image/dockerfile/template 三选一, timeout 有界, metadata 强类型
        req = AcquireRequest(image=image, dockerfile=dockerfile, template=template,
                             timeout=timeout, metadata=metadata or {},
                             allow_internet_access=allow_internet_access, network=network)
        image, dockerfile, template, timeout = req.image, req.dockerfile, req.template, req.timeout
        if not image and not dockerfile and not template:
            template = "base"
        md = dict(req.metadata)
        md.setdefault("managed_by", "sandbox_lifecycle")

        h: Optional[SandboxHandle] = None
        # prewarm (build if need) 在 run_limiter 之外: build 不占沙箱额度
        if image or dockerfile:
            template = self.prewarm(image=image, dockerfile=dockerfile)
        # create + yield + kill 全在一个 try: 任何中断(含 KeyboardInterrupt)
        # finally 都能拿到 h 并清理, 杜绝"create 成功但 h 未赋值就中断"的泄漏。
        try:
            with self._create_limiter.slot():
                h = self._create(template, timeout, md, req.allow_internet_access, req.network)
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

    # ---- 从快照/暂停恢复 ----
    @contextmanager
    def restore_from_snapshot(self, snapshot_id: str, timeout: int = 300,
                              metadata: Optional[dict] = None):
        """从快照起一个新沙箱 (E2B: Sandbox.create(template=snapshot_id))。
        带完整生命周期: 进 RUNNING、心跳、退出 kill + 清理。
        snapshot_id 由 h.save(name=) 返回。"""
        md = metadata or {}
        md.setdefault("managed_by", "sandbox_lifecycle")
        md["restored_from"] = snapshot_id
        with self._create_limiter.slot():
            h = self._create(snapshot_id, timeout, md)
        with self._inflight_lock:
            self._inflight.add(h.sid)
        hb = _Heartbeat(self, h.sid, self._stale_timeout)
        hb.start()
        try:
            with self._run_limiter.slot():
                yield h
        finally:
            if h is not None:
                self._kill_one(h.sid, h.sandbox)
                with self._inflight_lock:
                    self._inflight.discard(h.sid)

    def resume_sandbox(self, sid: str) -> SandboxHandle:
        """恢复一个已暂停的沙箱 (connect auto-resume)。纳入状态机:
        标 RUNNING + 进 _inflight, atexit/shutdown 兜底清理。
        注意: 若该沙箱已在 sqlite (me2b 创建过), 会刷新其状态; 若是外部沙箱, 新建一行。"""
        Sandbox = self._sandbox_cls()
        sbx = Sandbox.connect(sid)
        now = int(time.time())
        # 状态机: 标 RUNNING + 心跳 (upsert 幂等: 已存在则更新, 不存在则新建)
        self.db.upsert(sid, state=State.RUNNING.value, template="(resumed)",
                       created_at=now, last_heartbeat=now)
        with self._inflight_lock:
            self._inflight.add(sid)
        return SandboxHandle(sid=sid, sandbox=sbx, template="(resumed)", lifecycle=self)

    def _release(self, handle: SandboxHandle) -> None:
        """释放一个 handle (kill + db clean + 移出 _inflight)。
        供非 context-manager 场景 (如 Harbor stop) 用, 对应 acquire 的 finally。"""
        if handle is None:
            return
        self._kill_one(handle.sid, handle.sandbox)
        with self._inflight_lock:
            self._inflight.discard(handle.sid)

    # ---- 快照清理: 删自己追踪的快照 (防 E2B 侧累积) ----
    def cleanup_snapshots(self, keep: set = None) -> dict:
        """删除 sqlite 追踪的快照 (E2B delete_snapshot + sqlite forget)。
        keep: 保留的 snapshot_id 集合。返回 {deleted, failed}。"""
        Sandbox = self._sandbox_cls()
        keep = keep or set()
        result = {"deleted": 0, "failed": 0}
        for row in self.db.list_snapshots():
            sid = row["snapshot_id"]
            if sid in keep:
                continue
            try:
                Sandbox.delete_snapshot(sid)
                self.db.forget_snapshot(sid)
                result["deleted"] += 1
            except Exception as e:
                logger.warning(f"删快照 {sid} 失败: {e}")
                result["failed"] += 1
        return result

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
        # 走完整状态机(_kill_one): try_claim→CLEANING→confirm死→CLEANED。
        # 严格校验后不能直接 RUNNING→CLEANED 跳跃; _kill_one 对崩溃残留(E2B已杀)
        # 的 confirm 会判已死→CLEANED, 对活沙箱(不该出现, stale才进)会 kill。
        for row in self.db.list_stale_running(self._stale_timeout):
            if self._kill_one(row["sandbox_id"]):
                result["reconciled"] += 1
        for row in self.db.list_state(State.CLEANING):
            if self._kill_one(row["sandbox_id"]):
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
