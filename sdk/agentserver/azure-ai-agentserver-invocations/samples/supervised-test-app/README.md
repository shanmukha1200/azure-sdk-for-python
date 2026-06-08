# Supervised durable test app

A small, **locally runnable** test app (no Foundry / LLM needed) for exercising
the in-process supervisor absorbed into `AgentServerHost.run()`. It is a trimmed
cousin of the `durable-agent-demo` and lets you **bring down the customer entry
point** on demand to watch the supervisor's worker-aware `/readiness` and the
automatic crash recovery.

## What it does

- Runs a durable `slow_research` task that **checkpoints after every stage** and
  **resumes from the last checkpoint after a crash** (`ctx.metadata` +
  `await ctx.metadata.flush()`).
- Enables supervision via `app.run(supervised=True)`. The parent process owns the
  platform port (`8088`), runs the real app as a worker subprocess on `8089`,
  reverse-proxies traffic to it, and answers `GET /readiness` by **probing the
  worker** — `200` when the worker is responsive, **`424 Failed Dependency`** when
  it is not.

## Run

```bash
pip install -r requirements.txt

# Turn the nanny on with the flag ...
python app.py
# ... or keep app.run() untouched and flip the env var instead:
AGENTSERVER_SUPERVISOR=1 python app.py
```

Tunables (all optional):

| Env var | Meaning | Default |
| --- | --- | --- |
| `AGENTSERVER_SUPERVISOR` | Enable supervision (alternative to `supervised=True`) | off |
| `AGENTSERVER_SUPERVISOR_WORKER_PORT` | Internal worker port | external+1 (8089) |
| `AGENTSERVER_SUPERVISOR_READINESS_PROBE` | Probe worker for readiness (`424` on fail); `0` = always `200` | on |
| `AGENTSERVER_SUPERVISOR_READINESS_TIMEOUT_SECONDS` | Readiness probe timeout | 5 |
| `AGENTSERVER_SUPERVISOR_MAX_RESTARTS` / `..._RESTART_WINDOW_SECONDS` | Crash-loop cap | 5 / 60 |
| `STAGES_COUNT` / `STAGE_DURATION` | Durable task length | 8 / 3s |

## Ways to bring down the customer entry point

All requests go to the supervisor on `localhost:8088`; it tunnels them to the worker.

| Endpoint | What happens |
| --- | --- |
| `GET /control/whoami` | Returns the worker PID — watch it change after a restart. |
| `GET /control/crash` | Worker hard-exits (`os._exit(1)`). The supervisor restarts it. `/readiness` is `424` (connection refused) during the gap, then `200` with a **new pid**. A running durable task resumes from its last checkpoint. |
| `GET /control/hang?seconds=N` | Worker blocks its event loop for `N` seconds (default 15) — **alive but unresponsive**. `/readiness` returns `424` (probe times out). The supervisor does **not** restart it (it's alive); it recovers on its own after `N`. |
| `GET /control/exit` | Worker exits cleanly (`os._exit(0)`). The supervisor stops too, with exit code 0. |
| `POST /invocations` `{"message":"crash"}` | Demo-style crash trigger (same as `/control/crash`). |

## Drive it

In one shell, run the app (above). In another:

```bash
./drive.sh            # bash
# or
pwsh ./drive.ps1      # PowerShell
```

Or by hand:

```bash
curl localhost:8088/control/whoami      # {"pid": 1234, ...}
curl -i localhost:8088/readiness        # 200 {"status":"healthy"}

curl localhost:8088/control/crash       # worker exits
curl -i localhost:8088/readiness        # 424 briefly ...
curl -i localhost:8088/readiness        # ... then 200
curl localhost:8088/control/whoami      # {"pid": 1240}  <- new pid

# Unresponsive-but-alive (no restart):
curl "localhost:8088/control/hang?seconds=12" &
curl -i localhost:8088/readiness        # 424 (probe timeout) while frozen
```

> Note: under auto-restart the worker-down window is sub-second, so a platform
> readiness probe should use `failureThreshold >= 2`. The `424` is best read as
> "worker persistently unresponsive."
