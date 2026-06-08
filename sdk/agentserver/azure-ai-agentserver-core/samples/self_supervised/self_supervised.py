"""Self-supervising agent server (crash auto-recovery, no extra code).

This sample shows the in-process supervisor ("nanny") absorbed into the
SDK. The customer writes the **same** ``app.run()`` they always do — the
only difference is the platform (or you, locally) sets
``AGENTSERVER_SUPERVISOR=1`` to turn supervision on. There is no public
API parameter; the supervisor is fully transparent to customer code.

When supervision is on, ``run()`` becomes a thin parent process that:

* owns the platform-facing port and answers ``GET /readiness`` by probing the
  worker's own readiness endpoint — returning ``200`` when the customer entry
  point is responsive and ``424 Failed Dependency`` when it is unresponsive,
* runs the real server in a worker subprocess and respawns it on crash
  (with a crash-loop cap), and
* reverse-proxies every other request to the worker.

On restart, the durable ``TaskManager`` automatically reclaims stale leases
and resumes in-flight tasks from their last checkpoint — so recovery is
invisible to callers. **No ``supervisor.py``, no Dockerfile changes.**

Usage::

    pip install azure-ai-agentserver-core

    # Either pass the flag ...
    python self_supervised.py

    # ... or keep app.run() untouched and flip the env var:
    AGENTSERVER_SUPERVISOR=1 python self_supervised.py

Then, in another shell, prove the worker-aware readiness::

    curl -i localhost:8088/readiness    # 200 {"status":"healthy"} while worker is up
    curl localhost:8088/boom            # worker exits; supervisor restarts it
    curl -i localhost:8088/readiness    # 424 Failed Dependency during the down window
                                        # then 200 again once the worker is back

Relevant environment variables (all optional)::

    AGENTSERVER_SUPERVISOR=1                       # enable supervision
    AGENTSERVER_SUPERVISOR_WORKER_PORT=8089        # internal worker port (default external+1)
    AGENTSERVER_WORKER_COMMAND="python self_supervised.py"  # explicit re-exec command
    AGENTSERVER_SUPERVISOR_MAX_RESTARTS=5          # crashes allowed within the window
    AGENTSERVER_SUPERVISOR_RESTART_WINDOW_SECONDS=60
    AGENTSERVER_SUPERVISOR_READINESS_PROBE=1       # probe worker readiness (424 on fail); 0 = always 200
    AGENTSERVER_SUPERVISOR_READINESS_TIMEOUT_SECONDS=5

.. note::

    Supervision is **opt-in** so default behaviour is unchanged. The worker
    child is detected via the ``AGENTSERVER_SUPERVISED_WORKER`` sentinel and
    always serves directly, so it can never re-supervise itself.
"""

from __future__ import annotations

import logging
import os

from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from azure.ai.agentserver.core import AgentServerHost

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = AgentServerHost()


async def _boom(request) -> Response:  # pragma: no cover - demo endpoint
    """Crash the worker process to demonstrate auto-recovery."""
    logger.warning("Simulating a fatal worker crash (os._exit).")
    # Hard-exit so the supervisor (parent) observes a non-zero child exit and
    # respawns the worker. A normal exception would NOT take the process down.
    os._exit(1)


async def _hello(request) -> Response:
    return JSONResponse({"message": "hello from the worker", "pid": os.getpid()})


app.router.routes.append(Route("/boom", _boom, methods=["GET"]))
app.router.routes.append(Route("/hello", _hello, methods=["GET"]))


if __name__ == "__main__":
    # The supervisor is fully transparent: customer code just calls app.run().
    # Set AGENTSERVER_SUPERVISOR=1 in the environment to turn on the nanny.
    app.run()
