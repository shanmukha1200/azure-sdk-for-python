FROM python:3.12-slim

WORKDIR /app

# Local wheels carry the supervisor-modified azure-ai-agentserver-core and
# azure-ai-agentserver-invocations (built by build.sh before the image build).
COPY wheels/ /tmp/wheels/

# Install the local wheels and let their dependencies resolve. PyPI (default
# index) supplies starlette/hypercorn/azure-core/opentelemetry; the azure-sdk
# public feed (extra index) supplies the internal microsoft-opentelemetry distro.
RUN pip install --no-cache-dir --no-input \
    --extra-index-url https://pkgs.dev.azure.com/azure-sdk/public/_packaging/azure-sdk-for-python/pypi/simple/ \
    /tmp/wheels/*.whl \
    && rm -rf /tmp/wheels

COPY app.py ./

# Turn on the SDK's built-in, fully transparent supervisor ("nanny"). No
# external supervisor.py / entrypoint.sh: the customer entry point is just
# `python app.py`, and run() becomes a thin parent that owns the platform
# port, answers /readiness (424 when the worker is unresponsive), and respawns
# the worker on crash.
ENV AGENTSERVER_SUPERVISOR=1
# Durable task knobs for the demo workload.
ENV STAGE_DURATION=5
ENV STAGES_COUNT=8
# Persist task/stream state across worker restarts.
ENV FOUNDRY_TASK_API_ENABLED=1

EXPOSE 8088

CMD ["python", "app.py"]
