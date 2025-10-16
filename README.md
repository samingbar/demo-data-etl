# Temporal Python SDK Project Template

![GitHub CI](https://github.com/kawofong/temporal-python-template/actions/workflows/ci.yml/badge.svg)
[![Code Coverage](https://img.shields.io/codecov/c/github/kawofong/temporal-python-template.svg?maxAge=86400)](https://codecov.io/github/kawofong/temporal-python-template?branch=master)
[![GitHub License](https://img.shields.io/github/license/kawofong/temporal-python-template)](https://github.com/kawofong/temporal-python-template/blob/main/LICENSE)

## Introduction

A modern, production-ready template for building Temporal applications using [Temporal Python SDK](https://docs.temporal.io/dev-guide/python). This template provides a solid foundation for developing Workflow-based applications with comprehensive testing, linting, and modern Python tooling.

### What's Included

- Complete testing setup (pytest) with async support
- Pre-configured development tooling (e.g. ruff, pre-commit) and CI
- Comprehensive documentation and guides
- [AGENTS.md](https://agents.md/) to provide the context and instructions to help AI coding agents work on your project

## Getting Started

### Prerequisites

- [uv](https://docs.astral.sh/uv/)
- [Temporal CLI](https://docs.temporal.io/cli#install)

### Quick Start

1. **Clone and setup the project:**

   ```bash
   git clone https://github.com/kawofong/temporal-python-template.git
   cd temporal-python-template
   uv sync --dev
   ```

1. **Install development hooks:**

   ```bash
   uv run poe pre-commit-install
   ```

1. **Run tests:**

   ```bash
   uv run poe test
   ```

1. **Start Temporal Server**:

   ```bash
   temporal server start-dev
   ```

1. **Run the example workflow** (in a separate terminal):

   ```bash
   # Start the worker
   uv run -m src.workflows.http.worker

   # In another terminal, execute a workflow
   uv run -m src.workflows.http.http_workflow
   ```

### Next Steps

- Check out some [example prompts](./docs/example-prompts.md) to generate Temporal Workflows using your favorite tool.
- After you have built your first Temporal Workflow, read [DEVELOPERS.md](./DEVELOPERS.md) to learn about development tips & tricks using this template.
- See [`docs/temporal-patterns.md`](./docs/temporal-patterns.md) for advanced Temporal patterns
- Check [`docs/testing.md`](./docs/testing.md) for Temporal testing best practices

## Temporal Durable ETL Demo

Demonstrate Temporal's strengths with a resilient extract→transform→load pipeline that survives rate limits, automates retries, and keeps customer data sources under their control.

```
+---------------------------+        +--------------------+        +-------------------+
| HTTP API (429/503 faults) | -----> | DurableEtlWorkflow | -----> | Customer SQLite    |
| Object store batch files  |        |  signals / queries |        | warehouse (sink)   |
+---------------------------+        +--------------------+        +-------------------+
           ^                                  |
           |                                  v
           +------------------- extract / transform / load activities -------------------+
```

### Why it matters

- **Durable orchestration:** Temporal tracks progress for every batch, heartbeats the sink writes, and resumes safely after worker restarts.
- **Customer-owned resources:** Sources (mock HTTP API/object store) and the SQLite warehouse live outside Temporal, reflecting real ownership boundaries.
- **Retry intelligence:** Activities declare retry/backoff policies; logs highlight automatic retries for 429/503/timeouts without manual glue code.
- **Operational control:** Pause/resume signals, live progress queries, and idempotent writes let operators manage backpressure confidently.

### Run it (<2 minutes)

1. Ensure the [Temporal CLI](https://docs.temporal.io/cli#install) is installed and on your `PATH`.
2. From the repo root run: `make demo`
   - Spawns Temporal dev server, mock faulting API, worker, launches workflow, and streams progress.
3. From another terminal run `make demo.pause` → watch fan-out halt while in-flight loads finish.
4. Resume with `make demo.resume` to continue ingestion.
5. Inspect live state any time with `make demo.status`.

Tip: Temporal Web UI is available at `http://localhost:8233` when dev server is running. Open it to visualize the workflow, activity retries, and history. The demo intentionally sleeps briefly before fan-out to give you time to open the UI.

### Flags and customization

You can fully drive the demo via `scripts/demo.py` (the Makefile wraps these). Use `uv run python -m scripts.demo <subcommand> [flags]`.

- `start` subcommand (launch stack and begin a run):
  - `--source {http|object}`: choose source type (default `http`).
  - `--api-port <int>`: mock HTTP API port (default `8081`).
  - `--page-size <int>`: records per HTTP page (default `40`).
  - `--max-pages <int>`: number of pages to ingest (default `5`).
  - `--max-concurrency <int>`: concurrent batches (default `3`).
  - `--interval <int>`: progress poll interval in seconds (default `10`).
  - `--sink-path <path>`: SQLite sink path (default `data/warehouse/warehouse.db`).
  - `--enable-failures` / `--disable-failures`: toggle simulated API faults (timeouts/429/503). If not provided, the first run defaults to disabled; subsequent runs default to enabled.

- Control subcommands (operate on the active run recorded under `.demo/state.json`):
  - `pause`: send the pause signal (halts new batch fan-out; in-flight continues).
  - `resume`: send the resume signal (continues fan-out).
  - `status`: print the progress snapshot from the workflow query.
  - `clean`: stop services and delete artifacts under `.demo/` and the default sink.

- Mock API controls (HTTP source only):
  - `mock offline [--duration <seconds>]`: simulate upstream outage; optional bounded duration.
  - `mock online`: restore availability immediately.

Examples:

```bash
# Minimal demo with defaults
uv run python -m scripts.demo start

# Larger pages, more fan-out, faster status polling
uv run python -m scripts.demo start --page-size 80 --max-pages 8 --max-concurrency 5 --interval 5

# Write sink to a custom location
uv run python -m scripts.demo start --sink-path /tmp/warehouse.db

# Object storage mode (reads local NDJSON under data/source_batches)
uv run python -m scripts.demo start --source object

# Simulate an outage for 30s, then restore
uv run python -m scripts.demo mock offline --duration 30
uv run python -m scripts.demo mock online
```

Make targets for convenience:

- `make demo` → `start`
- `make demo.pause` → `pause`
- `make demo.resume` → `resume`
- `make demo.status` → `status`
- `make demo.clean` → `clean`
- `make demo.mock-offline` → `mock offline`
- `make demo.mock-online` → `mock online`

### Framing the demo

Audience: data/platform engineers and SREs evaluating reliability and control for ingestion pipelines.

Key takeaways to state up front:
- Temporal makes ETL progress durable across process crashes and redeploys.
- Retries/backoff are declared once and handled uniformly across activities.
- Operators have a safe control surface (signals, queries) to manage backpressure.
- Sources and sinks remain customer-owned; Temporal orchestrates, doesn’t store your data.

Environment prep (before you start talking):
- Close stray Temporal dev servers; run `make demo.clean` if you’ve demoed recently.
- Ensure ports are free: `7233` (Temporal), `8233` (Temporal UI), `8081` (mock API) or adjust flags.
- Have `http://localhost:8233` ready in a browser tab.
- Optionally remove `data/warehouse/warehouse.db` if you want a fresh sink.

### Live demo run-of-show (5–8 minutes)

1. **Kickoff (0:00–1:00):** Run `make demo`, call out customer-owned API/object storage + warehouse, and show the ASCII architecture.
2. **Reliability (1:00–2:30):** Point to logs showing automatic retries/backoff when the mock API throws 429/503 or timeouts; highlight that Temporal replays deterministically.
3. **Control surface (2:30–4:00):** Send `make demo.pause`; explain signals pause new batch fan-out while heartbeats keep in-flight loads resumable. Resume and show workflow picks up instantly.
4. **Observability (4:00–5:30):** Run `make demo.status`; share progress metrics (batches/items/retries/last_error). Open Temporal UI to show workflow history, retries, and signals.
5. **Safety nets (5:30–7:00):** Kill the worker process (Ctrl+C in the worker pane) and relaunch the demo worker (re-run `make demo` or `uv run -m scripts.demo start` if needed). Show it resumes exactly where it left off because of activity heartbeats and idempotent sink writes.
6. **Optional outage (7:00–7:30):** `make demo.mock-offline` (or `uv run python -m scripts.demo mock offline --duration 20`) to simulate upstream downtime. Note backoff, retries, and no manual glue code.
7. **Wrap (7:30–8:00):** Summarize: zero lost work, automatic recovery, customer resource ownership, and precise operator controls.

### Triggering failure modes

- The mock API can inject 429/503 responses and an occasional forced timeout per page. By default, these are disabled on the very first run to ease setup, and enabled on subsequent runs. Override anytime with `--enable-failures` or `--disable-failures` on `start`.
- The load activity heartbeats each record; kill the worker and restart to prove checkpointed resume.
- Run with `make demo.pause` during high retry counts to show backpressure controls.
 - Force an upstream outage with `make demo.mock-offline` (restore with `make demo.mock-online`). Use `--duration` via the CLI if you want a bounded outage.

### Object storage mode

- Instead of HTTP, ingest from local NDJSON batches under `data/source_batches`.
- Start with `uv run python -m scripts.demo start --source object`.
- Customize file enumeration by changing `file_glob`/`root_path` in `src/workflows/etl/models.py` or by adding files under `data/source_batches/`.

### Inspecting results

- The default sink is a SQLite database at `data/warehouse/warehouse.db`.
- If you have `sqlite3` installed, you can quickly inspect counts:

```bash
sqlite3 data/warehouse/warehouse.db \
  "SELECT COUNT(1) AS rows, SUM(status='completed') AS completed FROM etl_records;"
```

### Cleanup

- `make demo.clean` stops background services and removes `.demo/` artifacts plus the demo warehouse database.
- Remove generated SQLite output manually if you changed `--sink-path` during the demo.

## License

[MIT License](LICENSE).
