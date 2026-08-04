"""Additional tests for tunnel_local_http and ssh_reverse_tunnel_cmd."""
import pytest
from unittest.mock import MagicMock, AsyncMock


class TestTunnelLocalHttp:
    """Test tunnel_local_http and ssh_reverse_tunnel_cmd (sync)."""

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

    def test_ssh_reverse_tunnel_cmd(self):
        h, sandbox = self._make_handle()
        cmd = h.ssh_reverse_tunnel_cmd(8000)
        assert "ssh -N -f" in cmd
        assert "ExitOnForwardFailure=yes" in cmd
        assert "websocat" in cmd
        assert "127.0.0.1:9000:127.0.0.1:8000" in cmd
        assert "user@" in cmd
        assert h.sid in cmd

    def test_ssh_reverse_tunnel_cmd_custom_port(self):
        h, sandbox = self._make_handle()
        cmd = h.ssh_reverse_tunnel_cmd(3000, sandbox_port=7000)
        assert "127.0.0.1:7000:127.0.0.1:3000" in cmd


class TestAsyncTunnelLocalHttp:
    """Test async tunnel_local_http and ssh_reverse_tunnel_cmd."""

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

    def test_async_ssh_reverse_tunnel_cmd(self):
        h, sandbox = self._make_handle()
        cmd = h.ssh_reverse_tunnel_cmd(8000)
        assert "ssh -N -f" in cmd
        assert "websocat" in cmd
        assert "127.0.0.1:9000:127.0.0.1:8000" in cmd
