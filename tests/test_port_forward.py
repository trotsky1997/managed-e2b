"""Tests for sandbox port forwarding (get_host, get_url, expose_port).

Unit tests with mocks — no real E2B API calls needed.
"""
import pytest
from unittest.mock import MagicMock, patch, AsyncMock


# ---- Sync tests ----

class TestSandboxHandlePortForward:
    """Test sync SandboxHandle port forwarding methods."""

    def _make_handle(self, sandbox_id="sbx_test123", sandbox_domain="e2b.app"):
        """Create a SandboxHandle with a mock sandbox."""
        from managed_e2b.core import SandboxHandle
        sandbox = MagicMock()
        sandbox.sandbox_id = sandbox_id
        sandbox.sandbox_domain = sandbox_domain
        sandbox.get_host.return_value = f"3000-{sandbox_id}.{sandbox_domain}"
        h = SandboxHandle(sid=sandbox_id, sandbox=sandbox, template="base")
        return h, sandbox

    def test_get_host(self):
        h, sandbox = self._make_handle()
        result = h.get_host(3000)
        sandbox.get_host.assert_called_once_with(3000)
        assert result == "3000-sbx_test123.e2b.app"

    def test_get_host_different_port(self):
        h, sandbox = self._make_handle()
        sandbox.get_host.return_value = "8080-sbx_test123.e2b.app"
        result = h.get_host(8080)
        sandbox.get_host.assert_called_once_with(8080)
        assert "8080" in result

    def test_get_url_default_https(self):
        h, sandbox = self._make_handle()
        url = h.get_url(3000)
        assert url == "https://3000-sbx_test123.e2b.app"

    def test_get_url_http(self):
        h, sandbox = self._make_handle()
        url = h.get_url(3000, scheme="http")
        assert url == "http://3000-sbx_test123.e2b.app"

    def test_expose_port_no_command(self):
        h, sandbox = self._make_handle()
        pf = h.expose_port(3000)
        assert pf.port == 3000
        assert pf.host == "3000-sbx_test123.e2b.app"
        assert pf.url == "https://3000-sbx_test123.e2b.app"
        assert pf.command is None
        assert pf.sandbox_id == "sbx_test123"
        # Should not have started a command
        sandbox.commands.run.assert_not_called()

    def test_expose_port_with_command(self):
        h, sandbox = self._make_handle()
        pf = h.expose_port(8080, command="python3 -m http.server 8080")
        assert pf.port == 8080
        assert pf.command == "python3 -m http.server 8080"
        # Should have started the command in background
        sandbox.commands.run.assert_called_once()
        call_args = sandbox.commands.run.call_args
        assert "python3 -m http.server 8080" in call_args[0][0]
        assert call_args[1].get("background") is True

    def test_expose_port_allow_public(self):
        h, sandbox = self._make_handle()
        pf = h.expose_port(3000, allow_public=True)
        # update_network should have been called
        sandbox.update_network.assert_called_once()

    def test_expose_port_no_public(self):
        h, sandbox = self._make_handle()
        pf = h.expose_port(3000, allow_public=False)
        # update_network should NOT have been called
        sandbox.update_network.assert_not_called()

    def test_expose_port_update_network_failure_non_fatal(self):
        h, sandbox = self._make_handle()
        sandbox.update_network.side_effect = Exception("network error")
        # Should not raise
        pf = h.expose_port(3000, allow_public=True)
        assert pf.port == 3000
        assert pf.host == "3000-sbx_test123.e2b.app"


# ---- Async tests ----

class TestAsyncSandboxHandlePortForward:
    """Test async AsyncSandboxHandle port forwarding methods."""

    def _make_handle(self, sandbox_id="sbx_async456", sandbox_domain="e2b.app"):
        """Create an AsyncSandboxHandle with a mock sandbox.
        get_host and update_network are sync on AsyncSandbox, so use MagicMock for those.
        commands.run is async, so use AsyncMock for it.
        """
        from managed_e2b.async_core import AsyncSandboxHandle
        sandbox = MagicMock()
        sandbox.sandbox_id = sandbox_id
        sandbox.sandbox_domain = sandbox_domain
        sandbox.get_host = MagicMock(return_value=f"3000-{sandbox_id}.{sandbox_domain}")
        sandbox.update_network = MagicMock()
        sandbox.commands = MagicMock()
        sandbox.commands.run = AsyncMock()
        h = AsyncSandboxHandle(sid=sandbox_id, sandbox=sandbox, template="base")
        return h, sandbox

    @pytest.mark.asyncio
    async def test_async_get_host(self):
        h, sandbox = self._make_handle()
        result = await h.get_host(3000)
        sandbox.get_host.assert_called_once_with(3000)
        assert result == "3000-sbx_async456.e2b.app"

    @pytest.mark.asyncio
    async def test_async_get_url(self):
        h, sandbox = self._make_handle()
        url = await h.get_url(3000)
        assert url == "https://3000-sbx_async456.e2b.app"

    @pytest.mark.asyncio
    async def test_async_get_url_http(self):
        h, sandbox = self._make_handle()
        url = await h.get_url(3000, scheme="http")
        assert url == "http://3000-sbx_async456.e2b.app"

    @pytest.mark.asyncio
    async def test_async_expose_port_no_command(self):
        h, sandbox = self._make_handle()
        pf = await h.expose_port(3000)
        assert pf.port == 3000
        assert pf.host == "3000-sbx_async456.e2b.app"
        assert pf.url == "https://3000-sbx_async456.e2b.app"
        assert pf.command is None
        assert pf.sandbox_id == "sbx_async456"

    @pytest.mark.asyncio
    async def test_async_expose_port_with_command(self):
        h, sandbox = self._make_handle()
        pf = await h.expose_port(8080, command="node server.js")
        assert pf.port == 8080
        assert pf.command == "node server.js"
        sandbox.commands.run.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_expose_port_no_public(self):
        h, sandbox = self._make_handle()
        pf = await h.expose_port(3000, allow_public=False)
        sandbox.update_network.assert_not_called()

    @pytest.mark.asyncio
    async def test_async_expose_port_update_network_failure_non_fatal(self):
        h, sandbox = self._make_handle()
        sandbox.update_network.side_effect = Exception("network error")
        pf = await h.expose_port(3000, allow_public=True)
        assert pf.port == 3000


# ---- Model tests ----

class TestPortForwardModel:
    """Test PortForward pydantic model."""

    def test_create_port_forward(self):
        from managed_e2b.models import PortForward
        pf = PortForward(
            port=3000,
            host="3000-sbx123.e2b.app",
            url="https://3000-sbx123.e2b.app",
        )
        assert pf.port == 3000
        assert pf.host == "3000-sbx123.e2b.app"
        assert pf.url == "https://3000-sbx123.e2b.app"
        assert pf.command is None
        assert pf.sandbox_id == ""

    def test_port_forward_with_all_fields(self):
        from managed_e2b.models import PortForward
        pf = PortForward(
            port=8080,
            host="8080-sbx456.e2b.app",
            url="https://8080-sbx456.e2b.app",
            command="python3 -m http.server 8080",
            sandbox_id="sbx456",
        )
        assert pf.port == 8080
        assert pf.command == "python3 -m http.server 8080"
        assert pf.sandbox_id == "sbx456"

    def test_port_forward_validation_port_range(self):
        from managed_e2b.models import PortForward
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            PortForward(port=0, host="h", url="u")
        with pytest.raises(ValidationError):
            PortForward(port=70000, host="h", url="u")

    def test_port_forward_extra_forbidden(self):
        from managed_e2b.models import PortForward
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            PortForward(port=3000, host="h", url="u", extra_field="bad")


# ---- Import tests ----

class TestImports:
    """Test that PortForward is properly exported."""

    def test_import_port_forward_from_package(self):
        from managed_e2b import PortForward
        assert PortForward is not None

    def test_import_port_forward_from_models(self):
        from managed_e2b.models import PortForward
        assert PortForward is not None

    def test_port_forward_in_all(self):
        import managed_e2b
        assert "PortForward" in managed_e2b.__all__
