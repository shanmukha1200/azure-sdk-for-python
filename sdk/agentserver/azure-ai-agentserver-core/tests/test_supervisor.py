# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------
"""Tests for the in-process supervisor / reverse proxy (Option C).

Covers:
* mode/port/command resolution helpers,
* the raw reverse proxy (readiness answered locally, 503 when the worker is
  down, transparent tunnel when it is up),
* an end-to-end crash -> restart cycle where ``/readiness`` stays 200 the
  whole time and the worker comes back automatically.
"""
import asyncio  # pylint: disable=do-not-import-asyncio
import contextlib
import os
import sys
import textwrap
from typing import Optional, Tuple

import pytest

from azure.ai.agentserver.core import _supervisor


# ======================================================================
# Pure resolution helpers
# ======================================================================
def test_should_run_as_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AGENTSERVER_SUPERVISED_WORKER", raising=False)
    assert _supervisor.should_run_as_worker() is False
    monkeypatch.setenv("AGENTSERVER_SUPERVISED_WORKER", "1")
    assert _supervisor.should_run_as_worker() is True
    monkeypatch.setenv("AGENTSERVER_SUPERVISED_WORKER", "0")
    assert _supervisor.should_run_as_worker() is False


def test_resolve_supervision_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AGENTSERVER_SUPERVISOR", raising=False)
    # Default is opt-in (off) — no public param, env-driven only.
    assert _supervisor.resolve_supervision() is False
    # Internal explicit override still wins over env.
    monkeypatch.setenv("AGENTSERVER_SUPERVISOR", "1")
    assert _supervisor.resolve_supervision(False) is False
    assert _supervisor.resolve_supervision() is True
    monkeypatch.setenv("AGENTSERVER_SUPERVISOR", "off")
    assert _supervisor.resolve_supervision() is False


def test_resolve_internal_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AGENTSERVER_SUPERVISOR_WORKER_PORT", raising=False)
    assert _supervisor.resolve_internal_port(8088) == 8089
    monkeypatch.setenv("AGENTSERVER_SUPERVISOR_WORKER_PORT", "9999")
    assert _supervisor.resolve_internal_port(8088) == 9999
    # Collision with external port is rejected.
    monkeypatch.setenv("AGENTSERVER_SUPERVISOR_WORKER_PORT", "8088")
    with pytest.raises(ValueError):
        _supervisor.resolve_internal_port(8088)


def test_resolve_worker_command_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENTSERVER_WORKER_COMMAND", "python -m foo.bar")
    assert _supervisor.resolve_worker_command() == ["python", "-m", "foo.bar"]
    monkeypatch.delenv("AGENTSERVER_WORKER_COMMAND", raising=False)
    cmd = _supervisor.resolve_worker_command()
    assert isinstance(cmd, list) and cmd  # non-empty


# ======================================================================
# Raw reverse proxy (no real worker subprocess)
# ======================================================================
async def _http_get(
    host: str, port: int, path: str, timeout: float = 5.0
) -> Tuple[int, bytes]:
    """Issue a minimal HTTP/1.1 GET and return (status_code, body)."""
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(
        f"GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".encode()
    )
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(), timeout=timeout)
    writer.close()
    head, _, body = raw.partition(b"\r\n\r\n")
    status = int(head.split(b"\r\n", 1)[0].split(b" ")[1])
    return status, body


async def _start_proxy(
    internal_port: int,
    readiness_probe: bool = True,
    readiness_timeout: float = 2.0,
) -> Tuple[asyncio.AbstractServer, int]:
    server = await asyncio.start_server(
        lambda r, w: _supervisor._handle_client(  # noqa: SLF001
            r, w, internal_port, readiness_probe, readiness_timeout
        ),
        host="127.0.0.1",
        port=0,
    )
    proxy_port = server.sockets[0].getsockname()[1]
    return server, proxy_port


async def _readiness_backend(status_line: bytes):
    """Start a backend that answers any GET with the given status line."""
    async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.readuntil(b"\r\n\r\n")
        except asyncio.IncompleteReadError:
            return
        writer.write(status_line + b"Content-Length: 0\r\nConnection: close\r\n\r\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(_handler, host="127.0.0.1", port=0)
    return server, server.sockets[0].getsockname()[1]


@pytest.mark.asyncio
async def test_proxy_readiness_424_when_worker_down() -> None:
    """/readiness returns 424 when the worker entry point is unresponsive."""
    # Point at a port with nothing listening.
    server, proxy_port = await _start_proxy(internal_port=1)
    try:
        status, body = await _http_get("127.0.0.1", proxy_port, "/readiness")
        assert status == 424
        assert b"worker_unresponsive" in body
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_proxy_readiness_200_when_worker_healthy() -> None:
    """/readiness returns 200 when the worker's own readiness is 2xx."""
    backend, backend_port = await _readiness_backend(b"HTTP/1.1 200 OK\r\n")
    server, proxy_port = await _start_proxy(internal_port=backend_port)
    try:
        status, body = await _http_get("127.0.0.1", proxy_port, "/readiness")
        assert status == 200
        assert body == b'{"status":"healthy"}'
    finally:
        server.close()
        await server.wait_closed()
        backend.close()
        await backend.wait_closed()


@pytest.mark.asyncio
async def test_proxy_readiness_424_when_worker_unhealthy() -> None:
    """/readiness returns 424 when the worker answers a non-2xx status."""
    backend, backend_port = await _readiness_backend(b"HTTP/1.1 503 Service Unavailable\r\n")
    server, proxy_port = await _start_proxy(internal_port=backend_port)
    try:
        status, _ = await _http_get("127.0.0.1", proxy_port, "/readiness")
        assert status == 424
    finally:
        server.close()
        await server.wait_closed()
        backend.close()
        await backend.wait_closed()


@pytest.mark.asyncio
async def test_proxy_readiness_always_200_when_probe_disabled() -> None:
    """With probing disabled, /readiness is always 200 (container-alive only)."""
    server, proxy_port = await _start_proxy(internal_port=1, readiness_probe=False)
    try:
        status, body = await _http_get("127.0.0.1", proxy_port, "/readiness")
        assert status == 200
        assert body == b'{"status":"healthy"}'
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_proxy_503_when_worker_down() -> None:
    """Non-readiness paths get 503 when the worker is unreachable."""
    server, proxy_port = await _start_proxy(internal_port=1)
    try:
        status, _ = await _http_get("127.0.0.1", proxy_port, "/invocations")
        assert status == 503
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_proxy_tunnels_to_worker() -> None:
    """A live backend is reached transparently through the tunnel."""
    async def _backend(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello"
        )
        await writer.drain()
        writer.close()

    backend = await asyncio.start_server(_backend, host="127.0.0.1", port=0)
    backend_port = backend.sockets[0].getsockname()[1]
    server, proxy_port = await _start_proxy(internal_port=backend_port)
    try:
        status, body = await _http_get("127.0.0.1", proxy_port, "/invocations")
        assert status == 200
        assert body == b"hello"
    finally:
        server.close()
        await server.wait_closed()
        backend.close()
        await backend.wait_closed()


# ======================================================================
# End-to-end crash -> restart (real worker subprocess)
# ======================================================================
_WORKER_SCRIPT = textwrap.dedent(
    """
    import os
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass
        def do_GET(self):
            if self.path == "/crash":
                os._exit(7)
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

    port = int(os.environ["PORT"])
    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
    """
)


async def _wait_for(coro_factory, predicate, timeout: float = 10.0):
    deadline = asyncio.get_event_loop().time() + timeout
    last: Optional[Tuple[int, bytes]] = None
    while asyncio.get_event_loop().time() < deadline:
        try:
            last = await coro_factory()
            if predicate(last):
                return last
        except OSError:
            pass
        await asyncio.sleep(0.1)
    raise AssertionError(f"condition not met within {timeout}s; last={last!r}")


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="subprocess signal/event-loop semantics differ on Windows; Linux-first feature",
)
@pytest.mark.asyncio
async def test_end_to_end_crash_auto_recovers(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Worker crash -> supervisor auto-restarts; readiness reflects the worker."""
    script = tmp_path / "worker.py"
    script.write_text(_WORKER_SCRIPT)
    monkeypatch.setenv("AGENTSERVER_WORKER_COMMAND", f"{sys.executable} {script}")
    # Keep the worker from inheriting a stale sentinel/port.
    monkeypatch.delenv("AGENTSERVER_SUPERVISED_WORKER", raising=False)

    sup = _supervisor.Supervisor(host="127.0.0.1", external_port=0, internal_port=0)
    # Bind the external listener on an ephemeral port and pick a free internal one.
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        internal_port = s.getsockname()[1]
    sup._internal_port = internal_port  # noqa: SLF001

    # Start server + monitor by hand so we can read the chosen external port.
    server = await asyncio.start_server(
        lambda r, w: _supervisor._handle_client(  # noqa: SLF001
            r, w, internal_port, True, 2.0
        ),
        host="127.0.0.1",
        port=0,
    )
    ext_port = server.sockets[0].getsockname()[1]
    monitor = asyncio.ensure_future(sup._monitor())  # noqa: SLF001
    try:
        # Once the worker is reachable, readiness reflects it as 200.
        await _wait_for(
            lambda: _http_get("127.0.0.1", ext_port, "/"),
            lambda r: r[0] == 200 and r[1] == b"ok",
        )
        status, body = await _http_get("127.0.0.1", ext_port, "/readiness")
        assert status == 200
        assert body == b'{"status":"healthy"}'

        # Crash the worker.
        with pytest.raises((OSError, asyncio.IncompleteReadError, ValueError)):
            await _http_get("127.0.0.1", ext_port, "/crash", timeout=2.0)

        # Worker is respawned and serving again, and readiness goes back to 200.
        await _wait_for(
            lambda: _http_get("127.0.0.1", ext_port, "/"),
            lambda r: r[0] == 200 and r[1] == b"ok",
            timeout=15.0,
        )
        await _wait_for(
            lambda: _http_get("127.0.0.1", ext_port, "/readiness"),
            lambda r: r[0] == 200,
            timeout=15.0,
        )
    finally:
        sup._shutting_down = True  # noqa: SLF001
        await sup._terminate_worker()  # noqa: SLF001
        monitor.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await monitor
        server.close()
        await server.wait_closed()
