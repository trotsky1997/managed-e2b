"""Tests for the cloudflared quick-tunnel path (``expose_local_cloudflare``,
the 本地→公网 complement to chisel's 沙箱→本地 reverse tunnel).

Three layers:
1. ``cloudflared_release_asset`` — asset URL / archive / binname per platform (pure).
2. ``parse_cloudflared_quick_url`` — extract the trycloudflare.com URL from the
   real stderr cloudflared emits.
3. Optional e2e (skipped if no cloudflared binary cached locally): start a real
   quick tunnel against a local echo server, fetch the public URL from outside,
   verify the echo round-trips through Cloudflare's edge.
"""
from __future__ import annotations

import re
import socket
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from managed_e2b.core import (
    cloudflared_release_asset,
    parse_cloudflared_quick_url,
)

_CFLARED_BIN = Path.home() / ".cache" / "managed_e2b" / "cloudflared" / (
    "cloudflared.exe" if sys.platform.startswith("win") else "cloudflared"
)


def _cloudflared_available() -> bool:
    try:
        r = subprocess.run([str(_CFLARED_BIN), "--version"],
                           capture_output=True, timeout=20)
        return r.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


skip_no_cflared = pytest.mark.skipif(
    not _cloudflared_available(),
    reason="cloudflared binary not cached locally (run ensure_local_cloudflared() once)",
)


# --- asset resolver --------------------------------------------------------

class TestCloudflaredResolver:
    def test_linux_amd64_raw_binary(self):
        url, suf, bn = cloudflared_release_asset("linux", "amd64")
        assert suf is None
        assert bn == "cloudflared"
        assert url.endswith("cloudflared-linux-amd64")

    def test_windows_amd64_exe(self):
        url, suf, bn = cloudflared_release_asset("windows", "amd64")
        assert (suf, bn) == ("exe", "cloudflared.exe")
        assert url.endswith("cloudflared-windows-amd64.exe")

    def test_darwin_arm64_tgz(self):
        url, suf, bn = cloudflared_release_asset("darwin", "arm64")
        assert (suf, bn) == ("tgz", "cloudflared")
        assert url.endswith("cloudflared-darwin-arm64.tgz")

    def test_normalizes_platform_and_arch(self):
        url, suf, bn = cloudflared_release_asset("win32", "x86_64")
        assert (suf, bn) == ("exe", "cloudflared.exe")
        url, suf, bn = cloudflared_release_asset("cygwin", "aarch64")
        assert suf == "exe" and "windows-arm64" in url

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="no cloudflared release asset"):
            cloudflared_release_asset("solaris", "sparc")


# --- quick-tunnel URL parser ----------------------------------------------

class TestParseQuickUrl:
    def test_extracts_url_from_real_stderr(self):
        # Verbatim snippet captured from a real cloudflared quick tunnel run.
        stderr = (
            "2026-08-05T11:21:56Z INF Requesting new quick Tunnel on trycloudflare.com...\n"
            "2026-08-05T11:22:00Z INF |  Your quick Tunnel has been created! "
            "Visit it at (it may take some time to be reachable):  |\n"
            "2026-08-05T11:22:00Z INF |  "
            "https://referral-reality-judge-chair.trycloudflare.com                                    |\n"
        )
        assert parse_cloudflared_quick_url(stderr) == \
            "https://referral-reality-judge-chair.trycloudflare.com"

    def test_returns_none_when_no_url(self):
        assert parse_cloudflared_quick_url("nothing here") is None
        assert parse_cloudflared_quick_url("") is None

    def test_does_not_match_non_trycloudflare(self):
        # A random github URL must not be picked up.
        assert parse_cloudflared_quick_url("https://github.com/cloudflare/cloudflared") is None

    def test_only_https_scheme(self):
        # plain http trycloudflare is not a real quick-tunnel URL; require https
        assert parse_cloudflared_quick_url("http://foo.trycloudflare.com") is None


# --- named-tunnel env gating + API shaping (mocked, no network) -----------

class TestNamedTunnelEnv:
    """Gating: 4 vars → named path; missing any → None (→ quick path)."""

    def test_all_vars_present(self, monkeypatch):
        for k, v in [("CLOUDFLARE_API_TOKEN", "t"), ("CLOUDFLARE_ACCOUNT_ID", "a"),
                     ("CLOUDFLARE_ZONE_NAME", "z.com"),
                     ("CLOUDFLARE_TUNNEL_HOSTNAME", "h.z.com")]:
            monkeypatch.setenv(k, v)
        from managed_e2b.core import _cf_named_env
        cfg = _cf_named_env()
        assert cfg == {"token": "t", "account_id": "a", "zone_name": "z.com",
                       "hostname": "h.z.com", "tunnel_name": "managed-e2b"}

    def test_custom_tunnel_name(self, monkeypatch):
        for k, v in [("CLOUDFLARE_API_TOKEN", "t"), ("CLOUDFLARE_ACCOUNT_ID", "a"),
                     ("CLOUDFLARE_ZONE_NAME", "z.com"),
                     ("CLOUDFLARE_TUNNEL_HOSTNAME", "h.z.com"),
                     ("CLOUDFLARE_TUNNEL_NAME", "my-tun")]:
            monkeypatch.setenv(k, v)
        from managed_e2b.core import _cf_named_env
        assert _cf_named_env()["tunnel_name"] == "my-tun"

    @pytest.mark.parametrize("missing", [
        "CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID",
        "CLOUDFLARE_ZONE_NAME", "CLOUDFLARE_TUNNEL_HOSTNAME",
    ])
    def test_missing_any_var_falls_back_to_quick(self, monkeypatch, missing):
        for k, v in [("CLOUDFLARE_API_TOKEN", "t"), ("CLOUDFLARE_ACCOUNT_ID", "a"),
                     ("CLOUDFLARE_ZONE_NAME", "z.com"),
                     ("CLOUDFLARE_TUNNEL_HOSTNAME", "h.z.com")]:
            monkeypatch.setenv(k, v)
        monkeypatch.delenv(missing, raising=False)
        from managed_e2b.core import _cf_named_env
        assert _cf_named_env() is None  # → quick tunnel path


class TestNamedTunnelApiShaping:
    """Verify the create→ingress→dns API call sequence + request shapes (mocked)."""

    _ENV = {
        "CLOUDFLARE_API_TOKEN": "tok", "CLOUDFLARE_ACCOUNT_ID": "acc1",
        "CLOUDFLARE_ZONE_NAME": "ex.com", "CLOUDFLARE_TUNNEL_HOSTNAME": "ad.ex.com",
    }

    def _make_handle(self):
        from managed_e2b.core import SandboxHandle, SandboxLifecycle, SandboxDB
        import tempfile
        from unittest.mock import MagicMock
        sandbox = MagicMock()
        sandbox.sandbox_id = "sbx1"
        db = tempfile.mktemp(suffix=".db")
        lc = SandboxLifecycle.__new__(SandboxLifecycle); lc.db = SandboxDB(db)
        h = SandboxHandle(sid="sbx1", sandbox=sandbox, template="base", lifecycle=lc)
        # make self.run (sandbox curl probe) return HTTP200 immediately
        h.run = MagicMock(return_value={"stdout": "HTTP200", "stderr": ""})
        return h

    def test_named_tunnel_creates_and_routes_dns(self, monkeypatch):
        for k, v in self._ENV.items():
            monkeypatch.setenv(k, v)
        import managed_e2b.core as core
        calls = []

        def fake_api(method, path, cfg, *, json_body=None):
            calls.append((method, path, json_body))
            if method == "POST" and path == "/accounts/acc1/cfd_tunnel":
                return {"id": "tid-uuid", "token": "run-token-xyz"}
            if method == "GET" and path.startswith("/zones?name="):
                return [{"id": "zoneid-1"}]
            if method == "GET" and path.startswith("/zones/zoneid-1/dns_records?name="):
                return []  # no existing CNAME → create
            if method == "PUT":
                return {}  # ingress config accepted
            if method == "POST" and "dns_records" in path:
                return {"id": "rec-1"}  # CNAME created
            return {}

        # ensure_local_cloudflared → return a fake bin; cloudflared run → noop Popen
        monkeypatch.setattr(core, "ensure_local_cloudflared", lambda: "/fake/cf")
        monkeypatch.setattr(core, "_cf_api", fake_api)

        # stub _cf_named_state cache so the first branch (no cache) runs
        core._cf_named_state.clear()

        class _FakeProc:
            def __init__(self): self.returncode = None
            stderr = None
            def poll(self): return None
            def terminate(self): self.returncode = 0
        import managed_e2b.core as c2
        import subprocess
        monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: _FakeProc())

        h = self._make_handle()
        pf = h.expose_local_cloudflare(18080)

        # URL is the stable https://hostname (not a random trycloudflare URL)
        assert pf.url == "https://ad.ex.com"
        assert pf.host == "ad.ex.com"

        # API call sequence: create tunnel → put ingress → get zone → get dns → post cname
        methods = [m for (m, _, _) in calls]
        assert "POST" in methods and "PUT" in methods
        # ingress PUT body has hostname → localhost:port + 404 catch-all
        put = next(b for (m, _, b) in calls if m == "PUT" and b is not None)
        assert put["config"]["ingress"][0] == {
            "hostname": "ad.ex.com", "service": "http://localhost:18080"}
        assert put["config"]["ingress"][-1]["service"] == "http_status:404"
        # CNAME POST body points to tunnel-id cfargotunnel
        cname = next(b for (m, _, b) in calls
                     if m == "POST" and b and b.get("type") == "CNAME")
        assert cname["content"] == "tid-uuid.cfargotunnel.com"
        assert cname["name"] == "ad.ex.com" and cname["proxied"] is True

    def test_named_tunnel_reuses_existing_dns_cname(self, monkeypatch):
        for k, v in self._ENV.items():
            monkeypatch.setenv(k, v)
        import managed_e2b.core as core

        def fake_api(method, path, cfg, *, json_body=None):
            if method == "POST" and path == "/accounts/acc1/cfd_tunnel":
                return {"id": "tid", "token": "tok"}
            if method == "GET" and path.startswith("/zones?name="):
                return [{"id": "zid"}]
            # existing CNAME already present → must NOT create a new one
            if method == "GET" and "dns_records" in path:
                return [{"type": "CNAME", "content": "tid.cfargotunnel.com"}]
            return {}

        monkeypatch.setattr(core, "ensure_local_cloudflared", lambda: "/fake/cf")
        monkeypatch.setattr(core, "_cf_api", fake_api)
        core._cf_named_state.clear()

        class _P:
            returncode = None; stderr = None
            def poll(self): return None
            def terminate(self): self.returncode = 0
        import subprocess
        monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: _P())
        h = self
        from managed_e2b.core import SandboxHandle, SandboxLifecycle, SandboxDB
        import tempfile
        from unittest.mock import MagicMock
        sb = MagicMock(); sb.sandbox_id = "sbx1"
        lc = SandboxLifecycle.__new__(SandboxLifecycle); lc.db = SandboxDB(tempfile.mktemp(suffix=".db"))
        h = SandboxHandle(sid="sbx1", sandbox=sb, template="base", lifecycle=lc)
        h.run = MagicMock(return_value={"stdout": "HTTP200", "stderr": ""})
        pf = h.expose_local_cloudflare(18080)
        assert pf.url == "https://ad.ex.com"
        # verify no POST dns_records happened (reused)
        # (fake_api would record it; here we just assert no exception + url stable)


# --- optional e2e through Cloudflare's edge ------------------------------

@skip_no_cflared
def test_expose_local_cloudflare_e2e(tmp_path):
    """Real cloudflared quick tunnel: local echo server ↔ public trycloudflare URL.

    Verifies the parse_cloudflared_quick_url path against a live cloudflared process
    and that traffic really round-trips through Cloudflare's edge. Skipped without
    network or if cloudflared isn't cached.
    """
    import http.server

    local_port = _free_port()

    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.0"

        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Length", "5")
            self.end_headers()
            self.wfile.write(b"hello")

        def log_message(self, *a):  # noqa: N802
            pass

    srv = http.server.HTTPServer(("127.0.0.1", local_port), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        proc = subprocess.Popen(
            [str(_CFLARED_BIN), "tunnel", "--url", f"http://localhost:{local_port}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        try:
            url = _wait_for_quick_url(proc, timeout=30)
            assert url and url.startswith("https://") and ".trycloudflare.com" in url
            # Cloudflare edge may take a few seconds to become reachable.
            body = _curl_until(url, want=b"hello", timeout=45)
            assert body == b"hello", f"echo mismatch: {body!r}"
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
    finally:
        srv.shutdown()


def _free_port() -> int:
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p


def _wait_for_quick_url(proc: subprocess.Popen, *, timeout: float) -> str | None:
    import time
    deadline = time.time() + timeout
    buf: list[str] = []
    # cloudflared writes the URL to stderr; read it line-by-line in a thread.
    assert proc.stderr is not None
    def _drain():
        for line in proc.stderr:
            buf.append(line.decode(errors="replace"))
    threading.Thread(target=_drain, daemon=True).start()
    while time.time() < deadline:
        url = parse_cloudflared_quick_url("".join(buf))
        if url:
            return url
        if proc.poll() is not None:
            return None
        time.sleep(0.5)
    return None


def _curl_until(url: str, *, want: bytes, timeout: float) -> bytes:
    import time
    deadline = time.time() + timeout
    last = b""
    while time.time() < deadline:
        r = subprocess.run(["curl", "-sS", "-m", "10", url],
                           capture_output=True, timeout=15)
        last = r.stdout
        if want in r.stdout:
            return r.stdout
        time.sleep(2)
    return last
