# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------
"""In-process supervisor ("nanny") for AgentServerHost.

This module lets ``AgentServerHost.run()`` fork and supervise the real
server as a child process so a durable-task container recovers from a
worker crash automatically — with **no customer code**. The customer keeps
writing ``app.run()``; when supervision is enabled the parent process:

* binds the platform-facing port and answers ``GET /readiness`` with ``200``
  directly (so the platform never evicts the container while the worker is
  restarting),
* reverse-proxies every other request to the worker on an internal port via
  a dependency-free raw-asyncio byte tunnel (SSE / chunked friendly),
* respawns the worker on crash with a crash-loop cap, and forwards
  ``SIGTERM``/``SIGINT`` for a clean shutdown.

The worker is the same program re-executed with the sentinel env var
``AGENTSERVER_SUPERVISED_WORKER=1`` and ``PORT`` pointed at the internal
port; on the worker side the durable ``TaskManager.startup()`` reclaims
stale leases and resumes tasks from their last checkpoint — which is what
makes recovery invisible.

Dependency constraint: ``azure-ai-agentserver-core`` only depends on
starlette / hypercorn / azure-core / opentelemetry — **no httpx/aiohttp** —
so the proxy is built on raw ``asyncio`` streams only.

This is intentionally opt-in (env ``AGENTSERVER_SUPERVISOR``) so default
behaviour is unchanged. The public ``run()`` surface exposes no supervision
parameter; ``resolve_supervision()`` accepts an internal-only override.
Best-effort on Windows; designed Linux-first for containers.
"""
import asyncio  # pylint: disable=do-not-import-asyncio
import contextlib
import logging
import os
import shlex
import signal
import sys
import time
from collections import deque
from typing import TYPE_CHECKING, Deque, List, Optional, Tuple

if TYPE_CHECKING:
    from asyncio.subprocess import Process as _WorkerProcess

logger = logging.getLogger("azure.ai.agentserver.supervisor")

# Environment variables --------------------------------------------------
_ENV_SUPERVISOR = "AGENTSERVER_SUPERVISOR"
_ENV_WORKER_SENTINEL = "AGENTSERVER_SUPERVISED_WORKER"
_ENV_WORKER_PORT = "AGENTSERVER_SUPERVISOR_WORKER_PORT"
_ENV_WORKER_COMMAND = "AGENTSERVER_WORKER_COMMAND"
_ENV_MAX_RESTARTS = "AGENTSERVER_SUPERVISOR_MAX_RESTARTS"
_ENV_RESTART_WINDOW = "AGENTSERVER_SUPERVISOR_RESTART_WINDOW_SECONDS"
_ENV_READINESS_PROBE = "AGENTSERVER_SUPERVISOR_READINESS_PROBE"
_ENV_READINESS_TIMEOUT = "AGENTSERVER_SUPERVISOR_READINESS_TIMEOUT_SECONDS"
_ENV_PORT = "PORT"

# Defaults ---------------------------------------------------------------
_DEFAULT_MAX_RESTARTS = 5
_DEFAULT_RESTART_WINDOW_SECONDS = 60.0
_RESTART_BACKOFF_BASE_SECONDS = 0.5
_RESTART_BACKOFF_CAP_SECONDS = 10.0
_SHUTDOWN_GRACE_SECONDS = 10.0
_HEADER_READ_TIMEOUT_SECONDS = 30.0
_DEFAULT_READINESS_TIMEOUT_SECONDS = 5.0
_MAX_HEADER_BYTES = 64 * 1024
_PIPE_CHUNK = 64 * 1024

_READINESS_PATH = "/readiness"

# Pre-built raw HTTP/1.1 responses the parent answers itself. Each forces
# ``Connection: close`` so a health prober's keep-alive can't strand a
# connection on the parent that should have reached the worker.
_READY_RESPONSE = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Type: application/json\r\n"
    b"Content-Length: 20\r\n"
    b"Connection: close\r\n"
    b"\r\n"
    b'{"status":"healthy"}'
)
_UNAVAILABLE_BODY = b'{"status":"unavailable"}'
_UNAVAILABLE_RESPONSE = (
    b"HTTP/1.1 503 Service Unavailable\r\n"
    b"Content-Type: application/json\r\n"
    b"Content-Length: " + str(len(_UNAVAILABLE_BODY)).encode("ascii") + b"\r\n"
    b"Connection: close\r\n"
    b"\r\n" + _UNAVAILABLE_BODY
)
# Returned by ``GET /readiness`` when probing the worker (customer entry
# point) fails — it is unreachable or did not answer in time. ``424 Failed
# Dependency`` signals that the supervisor itself is up but the dependency it
# fronts is not healthy.
_FAILED_DEPENDENCY_BODY = b'{"status":"unavailable","reason":"worker_unresponsive"}'
_FAILED_DEPENDENCY_RESPONSE = (
    b"HTTP/1.1 424 Failed Dependency\r\n"
    b"Content-Type: application/json\r\n"
    b"Content-Length: " + str(len(_FAILED_DEPENDENCY_BODY)).encode("ascii") + b"\r\n"
    b"Connection: close\r\n"
    b"\r\n" + _FAILED_DEPENDENCY_BODY
)


# ======================================================================
# Mode resolution
# ======================================================================
def _truthy(value: Optional[str]) -> Optional[bool]:
    """Parse a tri-state boolean env string.

    :param value: Raw environment value or None.
    :type value: Optional[str]
    :return: True/False when recognised, else None (unset/empty).
    :rtype: Optional[bool]
    """
    if value is None:
        return None
    norm = value.strip().lower()
    if norm == "":
        return None
    if norm in ("1", "true", "yes", "on"):
        return True
    if norm in ("0", "false", "no", "off"):
        return False
    return None


def should_run_as_worker() -> bool:
    """Return True if this process is the supervised worker child.

    The sentinel always wins over any supervisor request so a worker can
    never re-enter supervision and fork infinitely.

    :return: Whether the worker sentinel env var is set.
    :rtype: bool
    """
    return _truthy(os.environ.get(_ENV_WORKER_SENTINEL)) is True


def resolve_supervision(supervised: Optional[bool] = None) -> bool:
    """Resolve whether ``run()`` should supervise a worker subprocess.

    Resolution order: explicit ``supervised`` override (internal callers only)
    → ``AGENTSERVER_SUPERVISOR`` env var → default ``False`` (opt-in). The
    public ``run()`` surface deliberately exposes no parameter; supervision is
    driven entirely by the platform-set ``AGENTSERVER_SUPERVISOR`` env var so
    customer code stays unaware of the supervisor.

    :param supervised: Explicit override or None (env-driven).
    :type supervised: Optional[bool]
    :return: True to run as supervisor, False to serve directly.
    :rtype: bool
    """
    if supervised is not None:
        return supervised
    env = _truthy(os.environ.get(_ENV_SUPERVISOR))
    if env is not None:
        return env
    return False


def resolve_internal_port(external_port: int) -> int:
    """Resolve the internal port the worker binds.

    Resolution order: ``AGENTSERVER_SUPERVISOR_WORKER_PORT`` env var →
    ``external_port + 1``.

    :param external_port: The platform-facing port the parent owns.
    :type external_port: int
    :return: Port the worker listens on (loopback only).
    :rtype: int
    :raises ValueError: If the override is not a valid 1-65535 port or
        collides with the external port.
    """
    raw = os.environ.get(_ENV_WORKER_PORT)
    if raw is not None and raw.strip() != "":
        try:
            port = int(raw)
        except ValueError as exc:
            raise ValueError(f"Invalid {_ENV_WORKER_PORT}: {raw!r}") from exc
        if not 1 <= port <= 65535:
            raise ValueError(f"Invalid {_ENV_WORKER_PORT}: {port} (expected 1-65535)")
        if port == external_port:
            raise ValueError(
                f"{_ENV_WORKER_PORT} ({port}) must differ from the external port"
            )
        return port
    if external_port >= 65535:
        raise ValueError(
            f"Cannot derive internal port from external port {external_port}; "
            f"set {_ENV_WORKER_PORT} explicitly"
        )
    return external_port + 1


def resolve_worker_command() -> List[str]:
    """Resolve the argv used to re-exec the worker.

    Resolution order: ``AGENTSERVER_WORKER_COMMAND`` (shell-split) →
    ``sys.orig_argv`` (full interpreter + flags, Python 3.10+) →
    ``[sys.executable] + sys.argv``.

    Re-exec is inherently best-effort across launch styles
    (``python app.py``, ``python -m pkg``, console-scripts, hypercorn CLI).
    Set ``AGENTSERVER_WORKER_COMMAND`` to be explicit when in doubt.

    :return: Command list suitable for asyncio subprocess exec.
    :rtype: List[str]
    """
    override = os.environ.get(_ENV_WORKER_COMMAND)
    if override is not None and override.strip() != "":
        return shlex.split(override, posix=os.name != "nt")
    orig = getattr(sys, "orig_argv", None)
    if orig:
        cmd = list(orig)
        # When launched through a virtual environment, ``orig_argv[0]`` can point
        # at the *base* interpreter (the venv ``python`` is a thin redirector),
        # which would re-exec the worker without the venv's site-packages. Pin the
        # interpreter to ``sys.executable`` (always the active env) when argv[0]
        # is a python launcher; leave console-script launches untouched.
        if cmd and os.path.basename(cmd[0]).lower().startswith("python") and cmd[0] != sys.executable:
            cmd[0] = sys.executable
        return cmd
    return [sys.executable] + list(sys.argv)


# ======================================================================
# Raw reverse proxy
# ======================================================================
async def _read_header_block(
    reader: asyncio.StreamReader,
) -> Tuple[bytes, bytes]:
    """Read bytes up to and including the end-of-headers (CRLFCRLF).

    :param reader: Client stream reader.
    :type reader: asyncio.StreamReader
    :return: Tuple of (full buffered bytes, request path bytes).
    :rtype: Tuple[bytes, bytes]
    :raises ValueError: If headers exceed the size cap or are malformed.
    """
    buf = await asyncio.wait_for(
        reader.readuntil(b"\r\n\r\n"), timeout=_HEADER_READ_TIMEOUT_SECONDS
    )
    if len(buf) > _MAX_HEADER_BYTES:
        raise ValueError("request header block too large")
    request_line = buf.split(b"\r\n", 1)[0]
    parts = request_line.split(b" ")
    if len(parts) < 2:
        raise ValueError("malformed request line")
    target = parts[1]
    path = target.split(b"?", 1)[0]
    return buf, path


async def _pipe(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """Copy one direction of a tunnel until EOF, then half-close.

    :param reader: Source stream.
    :type reader: asyncio.StreamReader
    :param writer: Destination stream.
    :type writer: asyncio.StreamWriter
    """
    try:
        while True:
            data = await reader.read(_PIPE_CHUNK)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        with contextlib.suppress(Exception):
            if writer.can_write_eof():
                writer.write_eof()


async def _probe_worker_readiness(internal_port: int, timeout: float) -> bool:
    """Probe the worker's own ``GET /readiness`` endpoint.

    Opens a short-lived loopback connection to the worker and issues a
    minimal HTTP/1.1 readiness request. Returns True only when the worker
    answers with a 2xx status within ``timeout`` seconds. Any connection
    error, timeout, malformed response, or non-2xx status is treated as
    "unresponsive" and returns False.

    :param internal_port: Loopback port the worker listens on.
    :type internal_port: int
    :param timeout: Per-operation timeout in seconds.
    :type timeout: float
    :return: True if the worker entry point is responsive and healthy.
    :rtype: bool
    """
    writer: Optional[asyncio.StreamWriter] = None
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", internal_port), timeout=timeout
        )
        writer.write(
            b"GET " + _READINESS_PATH.encode("ascii") + b" HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Connection: close\r\n\r\n"
        )
        await asyncio.wait_for(writer.drain(), timeout=timeout)
        status_line = await asyncio.wait_for(
            reader.readuntil(b"\r\n"), timeout=timeout
        )
        parts = status_line.split(b" ")
        if len(parts) < 2:
            return False
        code = int(parts[1])
        return 200 <= code < 300
    except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError, ValueError):
        return False
    finally:
        if writer is not None:
            with contextlib.suppress(Exception):
                writer.close()


# Tracks the last readiness outcome so we log only on transitions
# (healthy <-> unresponsive) instead of on every platform probe.
_readiness_state = {"healthy": True}


def _log_readiness_transition(healthy: bool) -> None:
    """Log an INFO line only when the readiness outcome changes.

    Keeps the signal clear during a worker crash/restart window: one line
    when readiness degrades to 424 and one line when it recovers to 200,
    without spamming on every periodic platform probe.

    :param healthy: Current readiness probe outcome.
    :type healthy: bool
    """
    if healthy == _readiness_state["healthy"]:
        return
    if healthy:
        logger.info(
            "Readiness recovered: worker entry point responsive again; "
            "answering platform probe with 200."
        )
    else:
        logger.info(
            "Readiness degraded: worker entry point unresponsive "
            "(crash/restart window); answering platform probe with "
            "424 Failed Dependency."
        )
    _readiness_state["healthy"] = healthy


async def _handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    internal_port: int,
    readiness_probe: bool = True,
    readiness_timeout: float = _DEFAULT_READINESS_TIMEOUT_SECONDS,
) -> None:
    """Handle one inbound connection: answer readiness or tunnel to worker.

    For ``GET /readiness`` the supervisor (when ``readiness_probe`` is True)
    probes the worker's own readiness endpoint and returns ``200`` when the
    worker is responsive or ``424 Failed Dependency`` when it is not. When
    ``readiness_probe`` is False it always returns ``200`` (the worker is
    never probed — useful if you want readiness to mean only "container
    alive"). All other paths are tunnelled to the worker.

    :param client_reader: Inbound client reader.
    :type client_reader: asyncio.StreamReader
    :param client_writer: Inbound client writer.
    :type client_writer: asyncio.StreamWriter
    :param internal_port: Loopback port the worker listens on.
    :type internal_port: int
    :param readiness_probe: Whether to reflect worker readiness (424 on fail).
    :type readiness_probe: bool
    :param readiness_timeout: Per-operation probe timeout in seconds.
    :type readiness_timeout: float
    """
    try:
        header, path = await _read_header_block(client_reader)
    except Exception:  # pylint: disable=broad-exception-caught
        # Malformed / oversized / timed-out request — drop it.
        with contextlib.suppress(Exception):
            client_writer.close()
        return

    if path == _READINESS_PATH.encode("ascii"):
        response = _READY_RESPONSE
        if readiness_probe:
            healthy = await _probe_worker_readiness(internal_port, readiness_timeout)
            _log_readiness_transition(healthy)
            response = _READY_RESPONSE if healthy else _FAILED_DEPENDENCY_RESPONSE
        with contextlib.suppress(Exception):
            client_writer.write(response)
            await client_writer.drain()
        with contextlib.suppress(Exception):
            client_writer.close()
        return

    try:
        worker_reader, worker_writer = await asyncio.open_connection(
            "127.0.0.1", internal_port
        )
    except OSError:
        # Worker down / restarting.
        with contextlib.suppress(Exception):
            client_writer.write(_UNAVAILABLE_RESPONSE)
            await client_writer.drain()
        with contextlib.suppress(Exception):
            client_writer.close()
        return

    # Replay the already-buffered first-request bytes, then become a dumb
    # bidirectional tunnel for the lifetime of this connection.
    try:
        worker_writer.write(header)
        await worker_writer.drain()
    except Exception:  # pylint: disable=broad-exception-caught
        with contextlib.suppress(Exception):
            worker_writer.close()
        with contextlib.suppress(Exception):
            client_writer.close()
        return

    await asyncio.gather(
        _pipe(client_reader, worker_writer),
        _pipe(worker_reader, client_writer),
    )
    for w in (worker_writer, client_writer):
        with contextlib.suppress(Exception):
            w.close()


# ======================================================================
# Supervisor
# ======================================================================
class Supervisor:  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """Parent process: owns the platform port, supervises a worker child.

    :param host: Network interface for the platform-facing listener.
    :type host: str
    :param external_port: Platform-facing port (parent binds this).
    :type external_port: int
    :param internal_port: Loopback port the worker binds.
    :type internal_port: int
    """

    def __init__(self, host: str, external_port: int, internal_port: int) -> None:
        self._host = host
        self._external_port = external_port
        self._internal_port = internal_port
        self._command = resolve_worker_command()
        self._max_restarts = self._read_int(
            _ENV_MAX_RESTARTS, _DEFAULT_MAX_RESTARTS
        )
        self._restart_window = self._read_float(
            _ENV_RESTART_WINDOW, _DEFAULT_RESTART_WINDOW_SECONDS
        )
        probe = _truthy(os.environ.get(_ENV_READINESS_PROBE))
        self._readiness_probe = True if probe is None else probe
        self._readiness_timeout = self._read_float(
            _ENV_READINESS_TIMEOUT, _DEFAULT_READINESS_TIMEOUT_SECONDS
        )
        self._shutting_down = False
        self._proc: "Optional[_WorkerProcess]" = None
        self._exit_code = 0
        self._crash_times: Deque[float] = deque()

    @staticmethod
    def _read_int(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if raw is None or raw.strip() == "":
            return default
        try:
            return int(raw)
        except ValueError:
            logger.warning("Invalid %s=%r; using %d", name, raw, default)
            return default

    @staticmethod
    def _read_float(name: str, default: float) -> float:
        raw = os.environ.get(name)
        if raw is None or raw.strip() == "":
            return default
        try:
            return float(raw)
        except ValueError:
            logger.warning("Invalid %s=%r; using %s", name, raw, default)
            return default

    def _worker_env(self) -> dict:
        env = dict(os.environ)
        env[_ENV_WORKER_SENTINEL] = "1"
        env[_ENV_PORT] = str(self._internal_port)
        # Belt-and-suspenders: prevent the child from re-supervising even if
        # the sentinel check were ever bypassed.
        env[_ENV_SUPERVISOR] = "0"
        return env

    async def _spawn_worker(self) -> "_WorkerProcess":
        logger.info("Supervisor spawning worker: %s", " ".join(self._command))
        return await asyncio.create_subprocess_exec(
            *self._command,
            env=self._worker_env(),
        )

    def _record_crash_and_should_stop(self) -> bool:
        """Record a crash and report whether the crash-loop cap is exceeded.

        :return: True if too many crashes occurred inside the window.
        :rtype: bool
        """
        now = time.monotonic()
        self._crash_times.append(now)
        while self._crash_times and now - self._crash_times[0] > self._restart_window:
            self._crash_times.popleft()
        return len(self._crash_times) >= self._max_restarts

    def _backoff_seconds(self) -> float:
        n = max(0, len(self._crash_times) - 1)
        return min(
            _RESTART_BACKOFF_CAP_SECONDS,
            _RESTART_BACKOFF_BASE_SECONDS * (2 ** n),
        )

    async def _monitor(self) -> None:
        """Spawn, watch, and restart the worker until clean exit / cap."""
        while not self._shutting_down:
            self._proc = await self._spawn_worker()
            returncode = await self._proc.wait()

            if self._shutting_down:
                break
            if returncode == 0:
                logger.info("Worker exited cleanly (0); supervisor stopping.")
                self._exit_code = 0
                break

            logger.warning("Worker exited with code %s.", returncode)
            if self._record_crash_and_should_stop():
                logger.error(
                    "Worker crashed %d times within %.0fs; supervisor giving up "
                    "so the platform recreates the container.",
                    self._max_restarts,
                    self._restart_window,
                )
                self._exit_code = returncode or 1
                break

            delay = self._backoff_seconds()
            logger.info("Restarting worker in %.1fs.", delay)
            await asyncio.sleep(delay)

    async def _terminate_worker(self) -> None:
        """Forward termination to the worker, grace, then kill."""
        proc = self._proc
        if proc is None or proc.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=_SHUTDOWN_GRACE_SECONDS)
        except asyncio.TimeoutError:
            logger.warning("Worker did not exit in grace; killing.")
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()

    def _install_signal_handlers(
        self, loop: asyncio.AbstractEventLoop, stop: asyncio.Event
    ) -> None:
        def _on_signal() -> None:
            if not self._shutting_down:
                logger.info("Supervisor received shutdown signal; draining.")
            self._shutting_down = True
            stop.set()

        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            if not hasattr(signal, name):
                continue
            try:
                loop.add_signal_handler(getattr(signal, name), _on_signal)
            except NotImplementedError:
                # Windows fallback.
                signal.signal(getattr(signal, name), lambda *_: _on_signal())

    async def run(self) -> int:
        """Run the supervisor until the worker is done or a signal arrives.

        :return: Process exit code for the parent to propagate.
        :rtype: int
        """
        loop = asyncio.get_event_loop()
        stop = asyncio.Event()
        self._install_signal_handlers(loop, stop)

        logger.info(
            "Supervisor active: external %s:%d -> worker 127.0.0.1:%d "
            "(max_restarts=%d/%.0fs, readiness_probe=%s)",
            self._host,
            self._external_port,
            self._internal_port,
            self._max_restarts,
            self._restart_window,
            self._readiness_probe,
        )

        server = await asyncio.start_server(
            lambda r, w: _handle_client(
                r,
                w,
                self._internal_port,
                self._readiness_probe,
                self._readiness_timeout,
            ),
            host=self._host,
            port=self._external_port,
        )

        monitor_task = asyncio.ensure_future(self._monitor())
        stop_task = asyncio.ensure_future(stop.wait())

        async with server:
            await asyncio.wait(
                {monitor_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

        self._shutting_down = True
        await self._terminate_worker()

        monitor_task.cancel()
        stop_task.cancel()
        for t in (monitor_task, stop_task):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t

        return self._exit_code


def run_supervisor(host: str, external_port: int, internal_port: int) -> int:
    """Blocking entry point: run the supervisor event loop to completion.

    :param host: Network interface for the platform-facing listener.
    :type host: str
    :param external_port: Platform-facing port.
    :type external_port: int
    :param internal_port: Loopback port the worker binds.
    :type internal_port: int
    :return: Parent process exit code.
    :rtype: int
    """
    return asyncio.run(Supervisor(host, external_port, internal_port).run())
