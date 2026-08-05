"""End-to-end tests for the chisel reverse-tunnel transport
(``expose_local(transport="chisel")``, the default path for issue #2).

These run chisel server + client on loopback (no real E2B sandbox) and drive
a real HTTP client through the reverse tunnel — proving the chisel path
carries claude/Bun-style traffic (large bodies, sequential requests) that the
old SSH/EOF bridge dropped mid-body.

Skipped automatically if the chisel binary isn't present locally
(``ensure_local_chisel()`` would download it; we don't want tests to hit the
network, so we skip instead).
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from managed_e2b.core import ensure_local_chisel

_CHISEL_BIN = Path.home() / ".cache" / "managed_e2b" / "chisel" / (
    "chisel.exe" if sys.platform.startswith("win") else "chisel"
)


def _chisel_available() -> bool:
    try:
        r = subprocess.run([str(_CHISEL_BIN), "--version"],
                           capture_output=True, timeout=10)
        return r.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


skip_no_chisel = pytest.mark.skipif(
    not _chisel_available(),
    reason="chisel binary not cached locally (run ensure_local_chisel() once to enable)",
)


def _free_port() -> int:
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p


def _start_echo_http(port: int) -> threading.Thread:
    """HTTP/1.0 echo server on `port` (Connection: close per request)."""
    import http.server

    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.0"

        def do_POST(self):  # noqa: N802
            n = int(self.headers.get("Content-Length", 0)); b = self.rfile.read(n)
            self.send_response(200)
            self.send_header("Content-Length", str(len(b) + 6))
            self.end_headers()
            self.wfile.write(b"echo:" + b)

        def log_message(self, *a):  # noqa: N802
            pass

    srv = http.server.HTTPServer(("127.0.0.1", port), H)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    th._srv = srv  # type: ignore[attr-defined]
    return th


@skip_no_chisel
def test_chisel_reverse_tunnel_small_and_large_body(tmp_path):
    """chisel server + client R: reverse tunnel carries POST bodies (the
    issue #2 case: claude/Bun POST body dropped through the SSH/EOF bridge)."""
    local_port = _free_port()        # the echo "adapter"
    server_port = _free_port()       # chisel server (plain ws on loopback)
    tunneled_port = _free_port()     # the port the R: rule opens on the server side
    http_thread = _start_echo_http(local_port)
    try:
        server = subprocess.Popen(
            [str(_CHISEL_BIN), "server", "--reverse",
             "--port", str(server_port), "--auth", "e2b:testtok"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(2)
        client = subprocess.Popen(
            [str(_CHISEL_BIN), "client", "--auth", "e2b:testtok",
             f"http://127.0.0.1:{server_port}",
             f"R:127.0.0.1:{tunneled_port}:127.0.0.1:{local_port}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            time.sleep(2)
            # small body
            r = subprocess.run(
                ["curl", "-sS", "-m", "10", "-X", "POST", "-d", "ping",
                 f"http://127.0.0.1:{tunneled_port}/"],
                capture_output=True, timeout=15)
            assert b"echo:ping" in r.stdout, f"small body failed: {r.stdout!r} {r.stderr!r}"
            # large body (150KB)
            big = tmp_path / "big.bin"; big.write_bytes(b"X" * 150_000)
            r = subprocess.run(
                ["curl", "-sS", "-m", "20", "-X", "POST", "--data-binary", f"@{big}",
                 f"http://127.0.0.1:{tunneled_port}/", "-o", str(tmp_path / "resp.bin")],
                capture_output=True, timeout=25)
            resp = (tmp_path / "resp.bin").read_bytes()
            assert resp == b"echo:" + b"X" * 150_000, (
                f"150KB body mismatch: got {len(resp)}B, want 150006B")
        finally:
            client.terminate(); client.wait(timeout=5)
            server.terminate(); server.wait(timeout=5)
    finally:
        getattr(http_thread, "_srv", None) and http_thread._srv.shutdown()  # type: ignore[attr-defined]
