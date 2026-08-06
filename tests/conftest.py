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
