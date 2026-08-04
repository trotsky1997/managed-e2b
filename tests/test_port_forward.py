"""Tests for sandbox port forwarding (get_host, get_url, expose_port, list_ports, close_port).

Unit tests with mocks — no real E2B API calls needed.
"""
import pytest
from unittest.mock import MagicMock, patch, AsyncMock


# ---- Sync tests ----

class TestSandboxHandlePortForward:
    """Test sync SandboxHandle port forwarding methods."""

    def _make_handle(self, sandbox_id="sbx_test123", sandbox_domain="e2b.app"):
        """Create a SandboxHandle with a mock sandbox and lifecycle."""
        from managed_e2b.core import SandboxHandle, SandboxLifecycle
        sandbox = MagicMock()
        sandbox.sandbox_id = sandbox_id
        sandbox.sandbox_domain = sandbox_domain
        sandbox.get_host.return_value = f"3000-{sandbox_id}.{sandbox_domain}"
        # Create a real lifecycle with in-memory db for DB tracking
        import os, tempfile
        db_path = tempfile.mktemp(suffix=".db")
        lc = SandboxLifecycle.__new__(SandboxLifecycle)
        from managed_e2b.core import SandboxDB
        lc.db = SandboxDB(db_path)
        h = SandboxHandle(sid=sandbox_id, sandbox=sandbox, template="base", lifecycle=lc)
        return h, sandbox, lc

    def test_get_host(self):
        h, sandbox, _ = self._make_handle()
        result = h.get_host(3000)
        sandbox.get_host.assert_called_once_with(3000)
        assert result == "3000-sbx_test123.e2b.app"

    def test_get_host_different_port(self):
        h, sandbox, _ = self._make_handle()
        sandbox.get_host.return_value = "8080-sbx_test123.e2b.app"
        result = h.get_host(8080)
        sandbox.get_host.assert_called_once_with(8080)
        assert "8080" in result

    def test_get_url_default_https(self):
        h, sandbox, _ = self._make_handle()
        url = h.get_url(3000)
        assert url == "https://3000-sbx_test123.e2b.app"

    def test_get_url_http(self):
        h, sandbox, _ = self._make_handle()
        url = h.get_url(3000, scheme="http")
        assert url == "http://3000-sbx_test123.e2b.app"

    def test_expose_port_no_command(self):
        h, sandbox, _ = self._make_handle()
        pf = h.expose_port(3000)
        assert pf.port == 3000
        assert pf.host == "3000-sbx_test123.e2b.app"
        assert pf.url == "https://3000-sbx_test123.e2b.app"
        assert pf.command is None
        assert pf.sandbox_id == "sbx_test123"
        sandbox.commands.run.assert_not_called()

    def test_expose_port_with_command(self):
        h, sandbox, _ = self._make_handle()
        pf = h.expose_port(8080, command="python3 -m http.server 8080")
        assert pf.port == 8080
        assert pf.command == "python3 -m http.server 8080"
        sandbox.commands.run.assert_called_once()
        call_args = sandbox.commands.run.call_args
        assert "python3 -m http.server 8080" in call_args[0][0]
        assert call_args[1].get("background") is True

    def test_expose_port_allow_public(self):
        h, sandbox, _ = self._make_handle()
        pf = h.expose_port(3000, allow_public=True)
        sandbox.update_network.assert_called_once()

    def test_expose_port_no_public(self):
        h, sandbox, _ = self._make_handle()
        pf = h.expose_port(3000, allow_public=False)
        sandbox.update_network.assert_not_called()

    def test_expose_port_update_network_failure_non_fatal(self):
        h, sandbox, _ = self._make_handle()
        sandbox.update_network.side_effect = Exception("network error")
        pf = h.expose_port(3000, allow_public=True)
        assert pf.port == 3000
        assert pf.host == "3000-sbx_test123.e2b.app"

    def test_expose_port_records_in_db(self):
        h, sandbox, lc = self._make_handle()
        pf = h.expose_port(3000)
        rows = lc.db.list_port_forwards(h.sid)
        assert len(rows) == 1
        assert rows[0]["port"] == 3000
        assert rows[0]["host"] == "3000-sbx_test123.e2b.app"
        assert rows[0]["url"] == "https://3000-sbx_test123.e2b.app"

    def test_expose_port_multiple_records(self):
        h, sandbox, lc = self._make_handle()
        h.expose_port(3000)
        h.expose_port(8080, command="node server.js")
        rows = lc.db.list_port_forwards(h.sid)
        assert len(rows) == 2

    def test_list_ports_empty(self):
        h, sandbox, lc = self._make_handle()
        ports = h.list_ports()
        assert ports == []

    def test_list_ports_after_expose(self):
        h, sandbox, lc = self._make_handle()
        h.expose_port(3000)
        h.expose_port(8080, command="node server.js")
        ports = h.list_ports()
        assert len(ports) == 2
        port_nums = sorted(p.port for p in ports)
        assert port_nums == [3000, 8080]
        # Check PortForward fields
        p8080 = [p for p in ports if p.port == 8080][0]
        assert p8080.command == "node server.js"
        assert p8080.sandbox_id == "sbx_test123"

    def test_close_port(self):
        h, sandbox, lc = self._make_handle()
        h.expose_port(3000, command="python3 -m http.server 3000")
        assert len(h.list_ports()) == 1
        result = h.close_port(3000)
        assert result is True
        assert len(h.list_ports()) == 0
        # Should have tried to kill the process
        sandbox.commands.run.assert_called()

    def test_close_port_not_found(self):
        h, sandbox, lc = self._make_handle()
        result = h.close_port(9999)
        assert result is False

    def test_close_port_kill_failure_non_fatal(self):
        h, sandbox, lc = self._make_handle()
        h.expose_port(3000)
        sandbox.commands.run.side_effect = Exception("kill failed")
        # Should still delete the record
        result = h.close_port(3000)
        assert result is True
        assert len(h.list_ports()) == 0

    def test_port_forwards_cleaned_on_sandbox_kill(self):
        h, sandbox, lc = self._make_handle()
        h.expose_port(3000)
        h.expose_port(8080)
        assert len(h.list_ports()) == 2
        # Simulate kill cleanup
        lc.db.delete_port_forwards(h.sid)
        assert len(h.list_ports()) == 0


# ---- Async tests ----

class TestAsyncSandboxHandlePortForward:
    """Test async AsyncSandboxHandle port forwarding methods."""

    def _make_handle(self, sandbox_id="sbx_async456", sandbox_domain="e2b.app"):
        """Create an AsyncSandboxHandle with a mock sandbox.
        get_host and update_network are sync on AsyncSandbox, so use MagicMock for those.
        commands.run is async, so use AsyncMock for it.
        """
        from managed_e2b.async_core import AsyncSandboxHandle, AsyncSandboxLifecycle
        from managed_e2b.core import SandboxDB
        import tempfile
        sandbox = MagicMock()
        sandbox.sandbox_id = sandbox_id
        sandbox.sandbox_domain = sandbox_domain
        sandbox.get_host = MagicMock(return_value=f"3000-{sandbox_id}.{sandbox_domain}")
        sandbox.update_network = MagicMock()
        sandbox.commands = MagicMock()
        sandbox.commands.run = AsyncMock()
        db_path = tempfile.mktemp(suffix=".db")
        lc = AsyncSandboxLifecycle.__new__(AsyncSandboxLifecycle)
        lc.db = SandboxDB(db_path)
        h = AsyncSandboxHandle(sid=sandbox_id, sandbox=sandbox, template="base", lifecycle=lc)
        return h, sandbox, lc

    @pytest.mark.asyncio
    async def test_async_get_host(self):
        h, sandbox, _ = self._make_handle()
        result = await h.get_host(3000)
        sandbox.get_host.assert_called_once_with(3000)
        assert result == "3000-sbx_async456.e2b.app"

    @pytest.mark.asyncio
    async def test_async_get_url(self):
        h, sandbox, _ = self._make_handle()
        url = await h.get_url(3000)
        assert url == "https://3000-sbx_async456.e2b.app"

    @pytest.mark.asyncio
    async def test_async_get_url_http(self):
        h, sandbox, _ = self._make_handle()
        url = await h.get_url(3000, scheme="http")
        assert url == "http://3000-sbx_async456.e2b.app"

    @pytest.mark.asyncio
    async def test_async_expose_port_no_command(self):
        h, sandbox, lc = self._make_handle()
        pf = await h.expose_port(3000)
        assert pf.port == 3000
        assert pf.host == "3000-sbx_async456.e2b.app"
        assert pf.url == "https://3000-sbx_async456.e2b.app"
        assert pf.command is None
        assert pf.sandbox_id == "sbx_async456"
        # Check DB recording
        rows = lc.db.list_port_forwards(h.sid)
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_async_expose_port_with_command(self):
        h, sandbox, lc = self._make_handle()
        pf = await h.expose_port(8080, command="node server.js")
        assert pf.port == 8080
        assert pf.command == "node server.js"
        sandbox.commands.run.assert_called_once()
        rows = lc.db.list_port_forwards(h.sid)
        assert len(rows) == 1
        assert rows[0]["command"] == "node server.js"

    @pytest.mark.asyncio
    async def test_async_expose_port_no_public(self):
        h, sandbox, _ = self._make_handle()
        await h.expose_port(3000, allow_public=False)
        sandbox.update_network.assert_not_called()

    @pytest.mark.asyncio
    async def test_async_expose_port_update_network_failure_non_fatal(self):
        h, sandbox, _ = self._make_handle()
        sandbox.update_network.side_effect = Exception("network error")
        pf = await h.expose_port(3000, allow_public=True)
        assert pf.port == 3000

    @pytest.mark.asyncio
    async def test_async_list_ports_empty(self):
        h, sandbox, _ = self._make_handle()
        ports = await h.list_ports()
        assert ports == []

    @pytest.mark.asyncio
    async def test_async_list_ports_after_expose(self):
        h, sandbox, _ = self._make_handle()
        await h.expose_port(3000)
        await h.expose_port(8080, command="node server.js")
        ports = await h.list_ports()
        assert len(ports) == 2

    @pytest.mark.asyncio
    async def test_async_close_port(self):
        h, sandbox, lc = self._make_handle()
        await h.expose_port(3000, command="node server.js")
        assert len(await h.list_ports()) == 1
        result = await h.close_port(3000)
        assert result is True
        assert len(await h.list_ports()) == 0

    @pytest.mark.asyncio
    async def test_async_close_port_not_found(self):
        h, sandbox, _ = self._make_handle()
        result = await h.close_port(9999)
        assert result is False

    @pytest.mark.asyncio
    async def test_async_port_forwards_cleaned_on_kill(self):
        h, sandbox, lc = self._make_handle()
        await h.expose_port(3000)
        await h.expose_port(8080)
        assert len(await h.list_ports()) == 2
        lc.db.delete_port_forwards(h.sid)
        assert len(await h.list_ports()) == 0


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
