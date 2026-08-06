"""Shared test helpers for managed_e2b tests (avoid per-file duplication)."""
from __future__ import annotations

import socket


def free_port() -> int:
    """An ephemeral free localhost TCP port (bind 0, read port, close)."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class FakeProc:
    """A minimal stand-in for subprocess.Popen for probe/tunnel tests.

    - poll() always returns None (process "alive") until terminate() sets 0.
    - stderr is None by default; tests that drain stderr set it.
    """

    def __init__(self) -> None:
        self.returncode = None
        self.stderr = None

    def poll(self):  # noqa: D401
        return None

    def terminate(self) -> None:
        self.returncode = 0
