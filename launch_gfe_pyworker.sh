#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_DIR="${WORKSPACE_DIR:-/workspace}"
APP_DIR="${GFE_APP_DIR:-$WORKSPACE_DIR/gfe-sweet}"
ADAPTER_DIR="$APP_DIR/renderer/musetalk-adapter"
WORKER_DIR="${SERVER_DIR:-$WORKSPACE_DIR/gfe-pyworker}"
ADAPTER_LOG="${MODEL_LOG:-/var/log/gfe-musetalk-adapter.log}"
ADAPTER_PORT="${MUSETALK_ADAPTER_PORT:-18000}"

mkdir -p "$WORKER_DIR"

cat > "$WORKER_DIR/requirements.txt" <<'REQ'
aiohttp
REQ

cat > "$WORKER_DIR/worker.py" <<'PY'
import asyncio
import base64
import json
import os
from urllib.parse import urlparse

import aiohttp
from aiohttp import web
from vastai import BenchmarkConfig, HandlerConfig, LogActionConfig, Worker, WorkerConfig

MODEL_SERVER_URL = "http://127.0.0.1"
MODEL_SERVER_PORT = int(os.environ.get("MUSETALK_ADAPTER_PORT", "18000"))
MODEL_LOG_FILE = os.environ.get("MODEL_LOG", "/var/log/gfe-musetalk-adapter.log")
MAX_WAIT_SECONDS = int(os.environ.get("SERVERLESS_RENDER_WAIT_SECONDS", "1800"))
POLL_SECONDS = float(os.environ.get("SERVERLESS_RENDER_POLL_SECONDS", "5"))


def unwrap_payload(request_payload):
    return request_payload.get("input") or request_payload.get("payload") or request_payload


def workload(payload):
    body = unwrap_payload(payload)
    config = ((body.get("session") or {}).get("config") or {}) if isinstance(body, dict) else {}
    seconds = config.get("clipSeconds") or os.environ.get("MUSETALK_CLIP_SECONDS", "30")
    try:
        return max(float(seconds), 1.0)
    except (TypeError, ValueError):
        return 30.0


def benchmark_payload():
    return {"ok": True}


async def benchmark_ready(**_params):
    return {"ok": True}


async def json_or_text(response):
    text = await response.text()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"status": "failed", "error": text or f"HTTP {response.status}"}


async def read_result_data_url(session, output_url):
    if not output_url:
        return None

    parsed = urlparse(output_url)
    result_name = os.path.basename(parsed.path)
    if not result_name:
        return None

    result_url = f"{MODEL_SERVER_URL}:{MODEL_SERVER_PORT}/results/{result_name}"
    async with session.get(result_url) as response:
        if response.status >= 400:
            return None
        content = await response.read()

    encoded = base64.b64encode(content).decode("ascii")
    return f"data:video/mp4;base64,{encoded}"


async def jobs_response_generator(client_request, model_response):
    async with aiohttp.ClientSession() as session:
        data = await json_or_text(model_response)
        if model_response.status >= 400:
            return web.json_response(data, status=model_response.status)

        job_id = data.get("jobId") or data.get("id")
        if not job_id:
            return web.json_response(data, status=model_response.status)

        deadline = asyncio.get_event_loop().time() + MAX_WAIT_SECONDS
        latest = data
        status_url = f"{MODEL_SERVER_URL}:{MODEL_SERVER_PORT}/jobs/{job_id}"

        while asyncio.get_event_loop().time() < deadline:
            async with session.get(status_url) as status_response:
                latest = await json_or_text(status_response)
                if status_response.status >= 400:
                    return web.json_response(latest, status=status_response.status)

            status = str(latest.get("status", "")).lower()
            if status in {"completed", "complete", "ready", "failed", "error"}:
                break
            await asyncio.sleep(POLL_SECONDS)
        else:
            latest = {
                "status": "failed",
                "jobId": job_id,
                "error": f"MuseTalk serverless render timed out after {MAX_WAIT_SECONDS}s.",
            }

        if str(latest.get("status", "")).lower() in {"completed", "complete", "ready"}:
            data_url = await read_result_data_url(session, latest.get("outputUrl") or latest.get("videoUrl"))
            if data_url:
                latest["outputUrl"] = data_url
                latest["videoUrl"] = data_url
                latest["status"] = "completed"

        return web.json_response(latest)


def logs_request_parser(payload):
    return {}


worker_config = WorkerConfig(
    model_server_url=MODEL_SERVER_URL,
    model_server_port=MODEL_SERVER_PORT,
    model_log_file=MODEL_LOG_FILE,
    model_healthcheck_url="/health",
    benchmark_route="/health",
    handlers=[
        HandlerConfig(
            route="/jobs",
            healthcheck="/health",
            allow_parallel_requests=False,
            max_queue_time=float(os.environ.get("SERVERLESS_MAX_QUEUE_TIME", "1800")),
            request_parser=unwrap_payload,
            response_generator=jobs_response_generator,
            workload_calculator=workload,
        ),
        HandlerConfig(
            route="/benchmark-ready",
            allow_parallel_requests=True,
            remote_function=benchmark_ready,
            benchmark_config=BenchmarkConfig(
                generator=benchmark_payload,
                runs=1,
                concurrency=1,
                do_warmup=False,
            ),
            workload_calculator=lambda _: 1.0,
        ),
        HandlerConfig(
            route="/logs",
            healthcheck="/health",
            allow_parallel_requests=True,
            request_parser=logs_request_parser,
            workload_calculator=lambda _: 1.0,
        ),
    ],
    log_action_config=LogActionConfig(
        on_load=["Application startup complete."],
        on_error=["Traceback (most recent call last):", "RuntimeError:", "Application startup failed"],
        on_info=["MuseTalk", "renderer"],
    ),
)

Worker(worker_config).run()
PY

if [[ ! -d "$ADAPTER_DIR" ]]; then
  echo "Missing MuseTalk adapter directory: $ADAPTER_DIR" >&2
  exit 1
fi

if command -v supervisorctl >/dev/null 2>&1 && supervisorctl status gfe-musetalk-adapter >/dev/null 2>&1; then
  supervisorctl restart gfe-musetalk-adapter || supervisorctl start gfe-musetalk-adapter || true
else
  cd "$ADAPTER_DIR"
  if [[ -f .env.renderer ]]; then
    set -a
    # shellcheck disable=SC1091
    . ./.env.renderer
    set +a
  fi
  "${ADAPTER_DIR}/.runtime-adapter-live/bin/uvicorn" app:app --host 127.0.0.1 --port "$ADAPTER_PORT" >> "$ADAPTER_LOG" 2>&1 &
fi

for _ in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:${ADAPTER_PORT}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

export SERVER_DIR="$WORKER_DIR"
export WORKSPACE_DIR="$WORKSPACE_DIR"
# Vast templates may inject BACKEND, which makes start_server.sh look for a
# built-in worker and require HF_TOKEN. This launcher provides worker.py itself.
unset BACKEND
export WORKER_PORT="${WORKER_PORT:-3000}"
export MODEL_LOG="$ADAPTER_LOG"
export SERVERLESS_RENDER_WAIT_SECONDS="${SERVERLESS_RENDER_WAIT_SECONDS:-${MUSETALK_MAX_SECONDS:-1800}}"

curl -fsSL https://raw.githubusercontent.com/vast-ai/pyworker/main/start_server.sh | bash
