# Copyright (c) Microsoft. All rights reserved.

"""Self-supervising durable test app — with switches to bring down the worker.

This is a trimmed, **locally runnable** cousin of the ``durable-agent-demo``
(no Foundry / LLM dependency). Its purpose is to exercise the in-process
supervisor absorbed into ``AgentServerHost.run()``:

* It runs a durable ``slow_research`` task that checkpoints after every stage
  and resumes from the last checkpoint after a crash (``ctx.metadata`` +
  ``ctx.metadata.flush()`` — exactly like the demo).
* It exposes **control endpoints to bring down the customer entry point** so
  you can watch the supervisor's worker-aware ``/readiness`` flip to ``424``
  and the worker auto-recover:

  =========================  =========================================================
  Endpoint                   Effect on the worker (customer entry point)
  =========================  =========================================================
  ``GET /control/whoami``    Returns the worker PID (watch it change after a restart).
  ``GET /control/crash``     Hard-exit the worker process (``os._exit(1)``).
                             → worker dies; external ``/readiness`` → 424 (connection
                               refused); supervisor restarts it; durable task resumes;
                               ``/readiness`` → 200 again with a NEW pid.
  ``GET /control/hang``      Block the worker event loop for ``?seconds=N`` (default 15)
                             using a blocking ``time.sleep`` — the process stays ALIVE
                             but stops answering. → external ``/readiness`` → 424
                             (readiness probe times out); the supervisor does NOT
                             restart it (it's alive); it recovers on its own after N.
  ``GET /control/exit``      Graceful ``os._exit(0)`` → supervisor stops with exit 0.
  =========================  =========================================================

  The demo-style trigger ``POST /invocations`` with ``{"message": "crash"}``
  also still crashes the worker.

Run it (supervised)::

    pip install -r requirements.txt

    # turn the nanny on with the flag ...
    python app.py
    # ... or keep app.run() untouched and flip the env var:
    AGENTSERVER_SUPERVISOR=1 python app.py

Then drive it from another shell (see ``drive.sh`` / ``drive.ps1``)::

    curl localhost:8088/control/whoami        # {"pid": 1234}
    curl localhost:8088/readiness             # 200 healthy
    curl localhost:8088/control/crash         # worker exits
    curl localhost:8088/readiness             # 424 briefly, then 200
    curl localhost:8088/control/whoami        # {"pid": 1240}  <- new pid

.. note::

    File-based task/stream stores are used for simplicity. In production a
    proper persistence store must be used.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from azure.ai.agentserver.core.durable import (
    TaskCancelled,
    TaskConflictError,
    TaskContext,
    TaskFailed,
    task,
)
from azure.ai.agentserver.invocations import InvocationAgentServerHost

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Number of stages and per-stage delay — long enough that you can crash the
# worker mid-task and watch it resume from the last checkpoint.
STAGES_COUNT = int(os.environ.get("STAGES_COUNT", "8"))
STAGE_DURATION = float(os.environ.get("STAGE_DURATION", "3"))

_STREAM_DIR = Path.home() / ".durable-tasks" / "_streams"


# ── File-backed stream handler (crash-resilient replay) ───────────────────────
class FileStreamHandler:
    """Persist stream items to disk so a consumer can replay after a crash."""

    def __init__(self, task_id: str) -> None:
        self._dir = _STREAM_DIR / task_id
        self._dir.mkdir(parents=True, exist_ok=True)
        self._file = self._dir / "stream.jsonl"
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._sentinel = object()
        if self._file.exists():
            for line in self._file.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    data = json.loads(line)
                    if "__done__" not in data:
                        self._queue.put_nowait(data)

    async def put(self, item: Any) -> None:
        """Persist an item to disk and enqueue it for the live consumer."""
        with open(self._file, "a", encoding="utf-8") as f:
            f.write(json.dumps(item) + "\n")
        await self._queue.put(item)

    async def get(self) -> Any:
        """Return the next streamed item (raises StopAsyncIteration at end)."""
        item = await self._queue.get()
        if item is self._sentinel:
            raise StopAsyncIteration
        return item

    async def close(self) -> None:
        """Mark the stream complete and unblock the consumer."""
        with open(self._file, "a", encoding="utf-8") as f:
            f.write(json.dumps({"__done__": True}) + "\n")
        await self._queue.put(self._sentinel)


def file_stream_factory(task_id: str) -> FileStreamHandler:
    """Factory that creates a file-backed stream handler for a task."""
    return FileStreamHandler(task_id)


# ── The durable task ──────────────────────────────────────────────────────────
@task(name="slow_research", stream_handler_factory=file_stream_factory)
async def slow_research(ctx: TaskContext[dict]) -> dict[str, Any]:
    """A long-running, crash-resilient task that checkpoints each stage."""
    topic: str = ctx.input.get("topic", "")
    completed: int = ctx.metadata.get("completed_stages", 0)
    total = STAGES_COUNT

    if ctx.entry_mode == "recovered":
        logger.warning("⚡ Recovered! Resuming from stage %d/%d", completed + 1, total)
        await ctx.stream(json.dumps({
            "type": "token",
            "content": f"\n⚡ Recovered from crash — resuming at stage {completed + 1}/{total}\n",
        }))

    for stage_idx in range(completed, total):
        if ctx.cancel.is_set():
            await ctx.stream(json.dumps({"type": "token", "content": "\n🛑 Cancelled.\n"}))
            return {"topic": topic, "stages_completed": stage_idx, "cancelled": True}

        await ctx.stream(json.dumps({
            "type": "token",
            "content": f"\n[Stage {stage_idx + 1}/{total}] working on {topic!r}...\n",
        }))
        await asyncio.sleep(STAGE_DURATION)

        # ── CHECKPOINT ── crash-recovery boundary ─────
        ctx.metadata["completed_stages"] = stage_idx + 1
        await ctx.metadata.flush()
        await ctx.stream(json.dumps({
            "type": "token",
            "content": f"✅ Stage {stage_idx + 1}/{total} complete (checkpointed).\n",
        }))

    await ctx.stream(json.dumps({"type": "token", "content": "\n✅ Research complete!\n"}))
    return {"topic": topic, "stages_completed": total}


# ── Host + invocation handlers (mirrors the demo) ─────────────────────────────
app = InvocationAgentServerHost()


@app.invoke_handler
async def handle_invoke(request: Request) -> Response:
    """Start a research task (fire-and-forget). ``message: crash`` crashes."""
    body = await request.body()
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {"message": body.decode("utf-8", errors="replace").strip()}

    topic = (data.get("message") or "").strip()
    if not topic and isinstance(data.get("input"), str):
        topic = data["input"].strip()

    if topic.lower() in ("crash", "kill", "💥"):
        logger.critical("💥 CRASH triggered via invoke — worker will exit shortly")
        _schedule_exit(137, delay=0.3)
        return JSONResponse({"status": "crashing"}, status_code=202)

    if not topic:
        return JSONResponse({"error": "Provide a 'message' field"}, status_code=400)

    invocation_id: str = request.state.invocation_id
    session_id: str = request.state.session_id
    task_id = invocation_id
    status = "started"
    try:
        await slow_research.start(
            task_id=task_id,
            input={"topic": topic, "invocation_id": invocation_id},
            session_id=session_id,
        )
    except TaskConflictError:
        status = "in_progress"
    return JSONResponse(
        {"status": status, "invocation_id": invocation_id, "session_id": session_id},
        status_code=202,
    )


@app.get_invocation_handler
async def handle_get(request: Request) -> Response:
    """Stream SSE from the active task, or replay from the persisted file."""
    invocation_id = request.state.invocation_id
    task_id = invocation_id
    last_event_id = request.query_params.get("last_event_id", "")
    skip_count = int(last_event_id) if last_event_id.isdigit() else 0

    run = await slow_research.get_active_run(task_id)
    if run is not None:
        async def live_stream():
            event_id = 0
            try:
                async for chunk in run:
                    event_id += 1
                    if event_id <= skip_count:
                        continue
                    yield f"id: {event_id}\ndata: {chunk}\n\n"
                result = await run.result()
                event_id += 1
                done = {"type": "done", "result": result.output}
                yield f"id: {event_id}\ndata: {json.dumps(done)}\n\n"
            except (TaskCancelled,):
                event_id += 1
                done = {"type": "done", "result": "[cancelled]"}
                yield f"id: {event_id}\ndata: {json.dumps(done)}\n\n"
            except TaskFailed as exc:
                event_id += 1
                done = {"type": "done", "result": f"[error: {exc}]"}
                yield f"id: {event_id}\ndata: {json.dumps(done)}\n\n"

        return StreamingResponse(
            live_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"}
        )

    stream_file = _STREAM_DIR / task_id / "stream.jsonl"
    if not stream_file.exists():
        return JSONResponse({"status": "not_found", "message": "No active task or history."})

    async def file_stream():
        event_id = 0
        for line in stream_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            if "__done__" in data:
                event_id += 1
                yield f"id: {event_id}\ndata: {json.dumps({'type': 'done', 'result': ''})}\n\n"
                return
            event_id += 1
            if event_id <= skip_count:
                continue
            yield f"id: {event_id}\ndata: {json.dumps(data)}\n\n"

    return StreamingResponse(
        file_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"}
    )


@app.cancel_invocation_handler
async def handle_cancel(request: Request) -> Response:
    """Cancel the running research task."""
    task_id = request.state.invocation_id
    run = await slow_research.get_active_run(task_id)
    if run is None:
        return JSONResponse({"status": "not_found", "message": "No active task to cancel."})
    await run.cancel()
    return JSONResponse({"status": "cancelled"})


# ── Control endpoints: ways to bring down the customer entry point ────────────
def _schedule_exit(code: int, delay: float = 0.3) -> None:
    """Exit the worker process after a short delay so the HTTP response flushes."""
    async def _do_exit() -> None:
        await asyncio.sleep(delay)
        logger.critical("Worker exiting now with code %d", code)
        os._exit(code)

    asyncio.get_event_loop().create_task(_do_exit())


async def _whoami(request: Request) -> Response:  # pylint: disable=unused-argument
    """Return the worker PID so a restart is visible as a changed pid."""
    return JSONResponse({"pid": os.getpid(), "stages_count": STAGES_COUNT})


async def _crash(request: Request) -> Response:  # pylint: disable=unused-argument
    """Hard-crash the worker (process dies → supervisor restarts it)."""
    logger.critical("💥 /control/crash — worker will hard-exit shortly")
    _schedule_exit(1, delay=0.3)
    return JSONResponse({"status": "crashing", "pid": os.getpid()}, status_code=202)


async def _hang(request: Request) -> Response:
    """Block the worker event loop so it is ALIVE but unresponsive.

    This makes the supervisor's readiness probe time out → external
    ``/readiness`` returns 424, while the supervisor leaves the (still-alive)
    worker running. After ``seconds`` the worker recovers on its own.
    """
    seconds = float(request.query_params.get("seconds", "15"))
    logger.critical(
        "🥶 /control/hang — blocking event loop for %.1fs (pid=%d)", seconds, os.getpid()
    )
    time.sleep(seconds)  # intentional blocking sleep — freezes this worker
    return JSONResponse(
        {"status": "unfroze", "blocked_seconds": seconds, "pid": os.getpid()}
    )


async def _graceful_exit(request: Request) -> Response:  # pylint: disable=unused-argument
    """Exit the worker cleanly (code 0) → supervisor stops with exit 0."""
    logger.warning("👋 /control/exit — worker will exit cleanly (supervisor stops)")
    _schedule_exit(0, delay=0.3)
    return JSONResponse({"status": "exiting", "pid": os.getpid()}, status_code=202)


for _route in (
    Route("/control/whoami", _whoami, methods=["GET"]),
    Route("/control/crash", _crash, methods=["GET"]),
    Route("/control/hang", _hang, methods=["GET"]),
    Route("/control/exit", _graceful_exit, methods=["GET"]),
):
    app.router.routes.append(_route)


if __name__ == "__main__":
    # The supervisor is fully transparent: customer code just calls app.run().
    # In a hosted container the platform sets AGENTSERVER_SUPERVISOR=1 to turn
    # on the in-process nanny; locally, export AGENTSERVER_SUPERVISOR=1 first.
    app.run()
