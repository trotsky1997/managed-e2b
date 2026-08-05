"""Tests for tunnel_local_http (cloudflared) + expose_local (chisel)."""
import pytest
from unittest.mock import MagicMock, AsyncMock


class TestTunnelLocalHttp:
    """Test tunnel_local_http + expose_local→chisel routing (sync)."""

    def _make_handle(self, sandbox_id="sbx_test123", sandbox_domain="e2b.app"):
        from managed_e2b.core import SandboxHandle, SandboxLifecycle, SandboxDB
        import tempfile
        sandbox = MagicMock()
        sandbox.sandbox_id = sandbox_id
        sandbox.sandbox_domain = sandbox_domain
        sandbox.get_host.return_value = f"8081-{sandbox_id}.{sandbox_domain}"
        db_path = tempfile.mktemp(suffix=".db")
        lc = SandboxLifecycle.__new__(SandboxLifecycle)
        lc.db = SandboxDB(db_path)
        h = SandboxHandle(sid=sandbox_id, sandbox=sandbox, template="base", lifecycle=lc)
        return h, sandbox

    def test_tunnel_local_http(self):
        h, sandbox = self._make_handle()
        result = h.tunnel_local_http("https://abc123.trycloudflare.com")
        assert result["alias"] == "local-api"
        assert result["tunnel_url"] == "https://abc123.trycloudflare.com"
        assert result["sandbox_host"] == "abc123.trycloudflare.com"
        # Should have tried to add /etc/hosts entry
        sandbox.commands.run.assert_called_once()
        cmd = sandbox.commands.run.call_args[0][0]
        assert "local-api" in cmd
        assert "/etc/hosts" in cmd

    def test_tunnel_local_http_custom_alias(self):
        h, sandbox = self._make_handle()
        result = h.tunnel_local_http("https://my-tunnel.ngrok.io", alias="my-api")
        assert result["alias"] == "my-api"
        cmd = sandbox.commands.run.call_args[0][0]
        assert "my-api" in cmd

    def test_tunnel_local_http_http_url(self):
        h, sandbox = self._make_handle()
        result = h.tunnel_local_http("http://localhost:4040")
        assert result["sandbox_host"] == "localhost:4040"

    def test_tunnel_local_http_hosts_failure_non_fatal(self):
        h, sandbox = self._make_handle()
        sandbox.commands.run.side_effect = Exception("permission denied")
        result = h.tunnel_local_http("https://abc.trycloudflare.com")
        assert result["alias"] == "local-api"
        assert result["sandbox_host"] == "abc.trycloudflare.com"

    def test_expose_local_dispatches_to_chisel(self, monkeypatch):
        """expose_local routes to _expose_local_chisel with the right args
        (issue #2: chisel is the only transport now)."""
        h, sandbox = self._make_handle()
        called = {}
        def fake_chisel(self_, local_port, sandbox_port, *, chisel_port, chisel_token):
            called["chisel"] = (local_port, sandbox_port, chisel_port, chisel_token)
            from managed_e2b.models import PortForward
            return PortForward(port=sandbox_port, host=f"127.0.0.1:{sandbox_port}",
                               url=f"http://127.0.0.1:{sandbox_port}", sandbox_id=self_.sid)
        monkeypatch.setattr(
            "managed_e2b.core.SandboxHandle._expose_local_chisel", fake_chisel)
        # default call → defaults flow through
        pf = h.expose_local(18080)
        assert called["chisel"] == (18080, 18080, 8082, None)
        assert pf.port == 18080
        sandbox.commands.run.assert_not_called()
        # explicit chisel_port + token
        pf2 = h.expose_local(18080, sandbox_port=19090, chisel_port=9090, chisel_token="tok")
        assert called["chisel"] == (18080, 19090, 9090, "tok")
        assert pf2.port == 19090


class TestChiselResolver:
    """chisel_release_asset: platform/arch → download URL + archive + binname."""

    def test_linux_amd64_gz(self):
        from managed_e2b.core import chisel_release_asset
        url, suf, bn = chisel_release_asset("linux", "amd64")
        assert suf == "gz"
        assert bn == "chisel"
        assert url.endswith("chisel_1.11.8_linux_amd64.gz")
        assert "github.com/jpillora/chisel/releases/download" in url

    def test_windows_amd64_zip(self):
        from managed_e2b.core import chisel_release_asset
        url, suf, bn = chisel_release_asset("windows", "amd64")
        assert suf == "zip"
        assert bn == "chisel.exe"
        assert url.endswith("chisel_1.11.8_windows_amd64.zip")

    def test_darwin_arm64_gz(self):
        from managed_e2b.core import chisel_release_asset
        url, suf, bn = chisel_release_asset("darwin", "arm64")
        assert (suf, bn) == ("gz", "chisel")
        assert url.endswith("chisel_1.11.8_darwin_arm64.gz")

    def test_normalizes_sys_platform_and_machine(self):
        from managed_e2b.core import chisel_release_asset
        # win32 + x86_64 → windows/amd64
        url, suf, bn = chisel_release_asset("win32", "x86_64")
        assert (suf, bn) == ("zip", "chisel.exe")
        # cygwin + aarch64 → windows/arm64
        url, suf, bn = chisel_release_asset("cygwin", "aarch64")
        assert (suf, bn) == ("zip", "chisel.exe")
        assert "windows_arm64" in url

    def test_unknown_raises(self):
        from managed_e2b.core import chisel_release_asset
        import pytest
        with pytest.raises(ValueError, match="no chisel release asset"):
            chisel_release_asset("solaris", "sparc")


class TestAsyncTunnelLocalHttp:
    """Test async tunnel_local_http."""

    def _make_handle(self, sandbox_id="sbx_async456", sandbox_domain="e2b.app"):
        from managed_e2b.async_core import AsyncSandboxHandle, AsyncSandboxLifecycle
        from managed_e2b.core import SandboxDB
        import tempfile
        sandbox = MagicMock()
        sandbox.sandbox_id = sandbox_id
        sandbox.sandbox_domain = sandbox_domain
        sandbox.get_host = MagicMock(return_value=f"8081-{sandbox_id}.{sandbox_domain}")
        sandbox.commands = MagicMock()
        sandbox.commands.run = AsyncMock()
        db_path = tempfile.mktemp(suffix=".db")
        lc = AsyncSandboxLifecycle.__new__(AsyncSandboxLifecycle)
        lc.db = SandboxDB(db_path)
        h = AsyncSandboxHandle(sid=sandbox_id, sandbox=sandbox, template="base", lifecycle=lc)
        return h, sandbox

    @pytest.mark.asyncio
    async def test_async_tunnel_local_http(self):
        h, sandbox = self._make_handle()
        result = await h.tunnel_local_http("https://abc123.trycloudflare.com")
        assert result["alias"] == "local-api"
        assert result["sandbox_host"] == "abc123.trycloudflare.com"
        sandbox.commands.run.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_tunnel_local_http_custom_alias(self):
        h, sandbox = self._make_handle()
        result = await h.tunnel_local_http("https://x.ngrok.io", alias="my-api")
        assert result["alias"] == "my-api"

    @pytest.mark.asyncio
    async def test_async_tunnel_local_http_failure_non_fatal(self):
        h, sandbox = self._make_handle()
        sandbox.commands.run.side_effect = Exception("error")
        result = await h.tunnel_local_http("https://abc.trycloudflare.com")
        assert result["sandbox_host"] == "abc.trycloudflare.com"

