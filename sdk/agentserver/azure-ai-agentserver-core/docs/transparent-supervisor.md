# Transparent Worker Supervisor ("nanny") for `AgentServerHost`

> Status: prototype validated end‑to‑end on Azure AI Foundry **hosted agents** in
> `westus2`. Module: `azure.ai.agentserver.core._supervisor` (private). Integration
> point: `AgentServerHost.run()`.

## 1. Problem

Durable‑task agents run long, checkpointed work inside a single hosted container.
If the **worker process crashes** (unhandled exception, OOM in a child library,
`os._exit`, segfault in a native dep), the whole container goes down. The platform
then has to recreate the container, the readiness gate flaps, and any in‑flight
durable task only resumes once a brand‑new container has cold‑started.

We wanted **invisible auto‑recovery**: when the worker dies, something restarts it
*in place* — same container, same network identity, sub‑second — and the durable
`TaskManager` resumes the task from its last checkpoint. Crucially, this must
require **no customer code**: the customer keeps writing `app.run()`.

## 2. Design overview ("Option C": absorb the nanny into the SDK)

Instead of shipping an external supervisor process (a separate `supervisor.py` +
`entrypoint.sh` the customer has to wire into their Dockerfile), the supervisor is
**absorbed into the SDK** and gated entirely on a platform‑set environment
variable. The public API surface is unchanged.

```
            ┌─────────────────────────── container ───────────────────────────┐
            │                                                                  │
 platform ─▶│  PARENT (supervisor)            CHILD (worker)                   │
 :8088      │  ┌───────────────────┐  loopback ┌────────────────────────────┐ │
            │  │ owns :8088        │  :8089    │ real AgentServerHost        │ │
            │  │ answers /readiness │──probe──▶ │ serves /readiness, /invoc… │ │
            │  │ raw‑asyncio proxy ─┼──tunnel──▶│ durable TaskManager         │ │
            │  │ respawns on crash  │           │ (resumes from checkpoint)   │ │
            │  └───────────────────┘           └────────────────────────────┘ │
            └──────────────────────────────────────────────────────────────────┘
```

* **Parent (supervisor)** binds the platform‑facing port (`PORT`, default `8088`),
  answers `GET /readiness` itself, reverse‑proxies everything else to the worker,
  and respawns the worker on crash.
* **Child (worker)** is the *same program* re‑executed with a sentinel env var and
  `PORT` pointed at an internal loopback port. It runs the normal
  `AgentServerHost` server, including the durable `TaskManager` which reclaims
  stale leases and resumes tasks on startup — this is what makes recovery
  invisible.

### 2.1 Why it lives in the SDK and not a sidecar

* **No customer code / Dockerfile wiring.** The customer image just runs
  `python app.py`; the SDK decides whether to supervise based on the env var.
* **Dependency constraint honored.** `azure-ai-agentserver-core` only depends on
  `starlette` / `hypercorn` / `azure-core` / `opentelemetry` — *no* `httpx` /
  `aiohttp`. The reverse proxy is therefore built on **raw `asyncio` streams**
  only (a dumb bidirectional byte tunnel), which is also SSE‑ / chunked‑friendly.
* **Opt‑in, default unchanged.** With the env var unset, `run()` serves directly
  exactly as before. There is **no public `supervised=` parameter** — supervision
  is invisible to customer code.

## 3. Activation & control (environment variables)

| Variable | Meaning | Default |
| --- | --- | --- |
| `AGENTSERVER_SUPERVISOR` | Master switch. `1/true/on` ⇒ this process becomes the supervisor. | `false` (opt‑in) |
| `AGENTSERVER_SUPERVISED_WORKER` | **Internal sentinel.** Set by the parent on the child so the worker serves directly and can never re‑supervise (no fork bomb). | set on child |
| `PORT` | Platform‑facing port the parent owns; the parent sets this to the internal port on the child. | `8088` |
| `AGENTSERVER_SUPERVISOR_WORKER_PORT` | Override the worker's loopback port. | `external_port + 1` |
| `AGENTSERVER_WORKER_COMMAND` | Override the argv used to re‑exec the worker (shell‑split). | auto‑derived |
| `AGENTSERVER_SUPERVISOR_MAX_RESTARTS` | Crash‑loop cap within the window. | `5` |
| `AGENTSERVER_SUPERVISOR_RESTART_WINDOW_SECONDS` | Sliding window for the cap. | `60` |
| `AGENTSERVER_SUPERVISOR_READINESS_PROBE` | If `true`, `/readiness` reflects the *worker's* health (probes it, returns `424` on failure). If `false`, `/readiness` always returns `200` (means only "container alive"). | `true` |
| `AGENTSERVER_SUPERVISOR_READINESS_TIMEOUT_SECONDS` | Per‑probe timeout. | `5.0` |

A typical hosted Dockerfile sets only:

```dockerfile
ENV AGENTSERVER_SUPERVISOR=1
CMD ["python", "app.py"]
```

## 4. Control flow in `AgentServerHost.run()`

`run(self, host="0.0.0.0", port=None)` (no supervision‑related parameter):

1. Resolve the port (`PORT` env or `8088`).
2. If this process is **not** the worker sentinel **and** `AGENTSERVER_SUPERVISOR`
   is truthy ⇒ derive the internal port and call `run_supervisor(...)`; propagate
   its exit code as `SystemExit`.
3. Otherwise (default, or we *are* the worker child) ⇒ `_run_direct(...)`, the
   normal hypercorn serve path — unchanged behavior.

The worker sentinel **always wins** over a supervisor request, so a child can
never re‑enter supervision.

### 4.1 Worker re‑exec (`resolve_worker_command`)

Resolution order: `AGENTSERVER_WORKER_COMMAND` → `sys.orig_argv` (full interpreter
+ flags, Python 3.10+) → `[sys.executable] + sys.argv`.

A **virtualenv hardening** step pins `argv[0]` to `sys.executable` when it is a
`python*` launcher that differs from the active interpreter — otherwise a venv's
thin `python` redirector can re‑exec the worker *without* the venv's
site‑packages. (Confirmed in‑container: the worker re‑execs
`/usr/local/bin/python app.py`.)

## 5. Readiness semantics (the key availability property)

The parent and worker are **separate processes**. The platform's `GET /readiness`
always reaches the **parent**, which (when `readiness_probe=true`) opens a
short‑lived loopback connection and issues a minimal HTTP/1.1 readiness request to
the worker:

| Worker state | Parent answers the platform |
| --- | --- |
| Up & responsive (2xx) | **`200`** `{"status":"healthy"}` |
| Booting / crashed / restarting / event‑loop frozen | **`424 Failed Dependency`** `{"status":"unavailable","reason":"worker_unresponsive"}` |

For **non‑readiness** requests, if the worker socket can't be reached the parent
returns **`503 Service Unavailable`** (so an in‑flight invoke during the restart
gap gets a clean 503, not a hang).

Because the **parent never dies**, the container stays up across a worker crash;
the platform just sees readiness flip `424 → 200` instead of losing the container.
The `424` is what protects the container from eviction *if* a probe lands during
the brief worker‑down window.

Each readiness outcome is logged **only on transition** (degraded ↔ recovered) at
`INFO`, to make the crash/restart window observable in container logs without
spamming on every periodic probe.

## 6. Crash handling & restart policy

* `_monitor()` spawns the worker, `await`s its exit, and restarts it.
* **Clean exit (`0`)** ⇒ supervisor stops too (intentional shutdown).
* **Non‑zero exit** ⇒ record the crash; if `>= max_restarts` within
  `restart_window` ⇒ give up and exit non‑zero **so the platform recreates the
  container** (avoids a hidden hot crash loop). Otherwise back off and respawn.
* **Backoff:** exponential `0.5s · 2^n`, capped at `10s`.
* **Shutdown:** `SIGINT`/`SIGTERM`/`SIGBREAK` are caught, `_shutting_down` is set,
  the worker is `terminate()`d with a `10s` grace then `kill()`ed.

## 7. Threat/limitation notes

* **Linux‑first.** Designed for containers; Windows signal handling is
  best‑effort.
* **`run_async()` does not supervise** (known gap) — only the synchronous `run()`
  path forks a supervisor.
* **Re‑exec is best‑effort across launch styles** (`python app.py`, `python -m`,
  console‑scripts, hypercorn CLI). Set `AGENTSERVER_WORKER_COMMAND` to be explicit.
* The reverse proxy is a **byte tunnel**, not an HTTP parser — it only special‑cases
  `GET /readiness`; everything else is streamed verbatim (correct for SSE/chunked).

---

# Tests performed

Two layers: deterministic local/unit behavior, and a full **hosted** validation on
Azure AI Foundry in `westus2`.

## A. Local / unit

* `tests/test_supervisor.py` — `resolve_supervision()` precedence (override → env →
  default `False`), worker‑sentinel resolution, readiness `424` when the probed
  port is dead, port derivation/validation.
* Live local run (Python 3.12 venv): `run()` with `AGENTSERVER_SUPERVISOR=1`
  forks a worker; `/readiness` returns `200` when up; a killed worker is respawned
  and `/readiness` returns `424` during the gap.

## B. Hosted validation (Azure AI Foundry, `westus2`)

**Environment.** Subscription `921496dc‑…`, RG `agents-e2e-tests-westus2`, project
`e2e-tests-westus2`, ACR `crdyt765he4tmsy.azurecr.io`, model `gpt-4.1-mini`.
Deployed with `azd deploy` (remote ACR build, `host: azure.ai.agent`,
`remoteBuild: true`). Image sets `AGENTSERVER_SUPERVISOR=1` + `CMD python app.py`.

A small test app (`durable-research-agent`) exposes triggers reachable through the
hosted **invocations** protocol:

* `{"message":"crash"}` ⇒ worker `os._exit(137)` (tests crash → restart).
* `{"message":"hang"}` / `hang:N` ⇒ freeze the worker event loop for `N`s, worker
  stays alive (tests "alive but unresponsive" → `424`, **no** restart).

> Note: the platform only routes the invocation verbs externally; the container's
> `/readiness` and `/control/*` are internal. So readiness behavior is observed via
> the container logs (the platform's own `/readiness` probes + our transition logs),
> not by curling `/readiness` from outside.

### B.1 Supervisor runs invisibly in‑container ✅

```
Supervisor active: external 0.0.0.0:8088 -> worker 127.0.0.1:8089 (max_restarts=5/60s, readiness_probe=True)
Supervisor spawning worker: /usr/local/bin/python app.py
AgentServerHost starting on 0.0.0.0:8089 ... is_hosted=True ... HostedTaskProvider
```

No customer code — just `app.run()` + the env var baked into the image.

### B.2 Crash → automatic in‑place recovery ✅

`POST {"message":"crash"}` ⇒ `202 {"status":"crashing"}`, then from the logs:

```
04:03:15.882 [CRITICAL] 💥 CRASH triggered via invoke — worker will exit shortly
04:03:16.183 [CRITICAL] Worker exiting now with code 137
04:03:16.187 [WARNING]  Worker exited with code 137.
04:03:16.187 [INFO]     Restarting worker in 0.5s.
04:03:16.689 [INFO]     Supervisor spawning worker: /usr/local/bin/python app.py
04:03:17.784 [INFO]     AgentServerHost started            ← new worker pid, ~1.6s down‑window
```

Worker **pid 30 → crash → pid 54** in ~6s end‑to‑end, **same container** (no
platform pod recreate). A second `crash` on the **same session** returned `202`
again, proving the recovered worker serves live traffic.

### B.3 Readiness during the crash/restart window ✅

Readiness transition logs across a worker boot/restart:

```
04:03:14.028 [INFO] Readiness degraded: worker entry point unresponsive (crash/restart window); answering platform probe with 424 Failed Dependency.
04:03:15.449  GET /readiness 200 ; Readiness recovered: worker entry point responsive again; answering platform probe with 200.
```

So while the worker is not up, the parent answers the platform **`424`**; once the
worker is up it answers **`200`** — exactly the intended gate, with the container
staying alive throughout.

### B.4 Readiness during a frozen ("hung") worker ✅ / observation

`POST {"message":"hang:25"}` ⇒ `202 {"status":"hanging","blocked_seconds":25.0}`:

```
04:00:44.552 [INFO] Readiness degraded ... 424 Failed Dependency      (boot window)
04:00:46.790 [INFO] Readiness recovered ... 200
04:00:47     [CRITICAL] 🥶 Worker freezing event loop now for 25.0s
04:01:12     [CRITICAL] Worker event loop unfroze after 25.0s          (no restart — worker never exited)
```

**Observation:** the platform polls `/readiness` *infrequently* after the initial
readiness gate — it did **not** probe during the 25s freeze or the ~1.6s restart
gap, so no `424` was recorded *then*. The boot‑window `424 → 200` (captured in
every run) exercises the **same code path** and is the authoritative proof. Net
effect: brief worker blips are usually invisible to the platform (good — no churn),
and the `424` protects the container only if a probe lands mid‑outage.

## C. Known issue surfaced during hosted testing (NOT a supervisor bug)

The durable *research* path returns `500` because the agent's **managed identity
cannot obtain a token for the platform Task Storage API**
(`ManagedIdentityCredential: No token received` → `Bad Request` →
`_recover_stale_tasks` fails). This is an RBAC/identity gap (the instance identity
needs a role assignment, typically created by `azd provision`/`azd up`) and is
**independent of the supervisor**, which is fully functional.

## D. Files

* `azure/ai/agentserver/core/_supervisor.py` — the feature (private module).
* `azure/ai/agentserver/core/_base.py` — `run()` integration (env‑gated).
* `tests/test_supervisor.py` — unit tests.
* `../azure-ai-agentserver-invocations/samples/durable-agent-demo/` — hosted test
  app + `azd` deployment scaffold (Dockerfile with `AGENTSERVER_SUPERVISOR=1`,
  `crash`/`hang` triggers).
