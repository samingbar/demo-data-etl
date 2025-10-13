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

### Live demo talk track (5–8 minutes)

1. **Kickoff (0:00–1:00):** Run `make demo`, call out customer-owned API/object storage + warehouse, and show the ASCII architecture.
2. **Reliability (1:00–2:30):** Point to logs showing automatic retries/backoff when the mock API throws 429/503 or timeouts; highlight that Temporal replays deterministically.
3. **Control surface (2:30–4:00):** Send `make demo.pause`; explain signals pause new batch fan-out while heartbeats keep in-flight loads resumable. Resume and show workflow picks up instantly.
4. **Observability (4:00–5:30):** Run `make demo.status`; share progress metrics (batches/items/retries/last error). Mention queries work even mid-flight.
5. **Safety nets (5:30–7:00):** Restart the worker (Ctrl+C + rerun `make demo` if desired) to illustrate heartbeats/idempotent sink preventing duplicates. Emphasize customer infra stays untouched.
6. **Wrap (7:00–8:00):** Summarize benefits: zero lost work, automatic recoveries, customer resource ownership, and precise operational control.

### Triggering failure modes

- The mock API intentionally injects 429/503 responses and an occasional forced timeout per page.
- The load activity heartbeats each record; kill the worker and restart to prove checkpointed resume.
- Run with `make demo.pause` during high retry counts to show backpressure controls.

### Cleanup

- `make demo.clean` stops background services and removes `.demo/` artifacts plus the demo warehouse database.
- Remove generated SQLite output manually if you changed `--sink-path` during the demo.

## License

[MIT License](LICENSE).
