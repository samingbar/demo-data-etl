#
.PHONY: demo demo.pause demo.resume demo.status demo.clean demo.mock-offline demo.mock-online \
        worker worker.kill workers.kill temporal.worker.kill temporal.workers.kill

# Launch the full Temporal Durable ETL demo stack
demo:
	uv run python -m scripts.demo start

# Pause the running demo workflow to simulate backpressure controls
demo.pause:
	uv run python -m scripts.demo pause

# Resume the paused demo workflow
demo.resume:
	uv run python -m scripts.demo resume

# Show the live progress snapshot
demo.status:
	uv run python -m scripts.demo status

# Stop background services and remove generated artifacts
demo.clean:
	uv run python -m scripts.demo clean

# Take the mock HTTP API offline to simulate upstream downtime
demo.mock-offline:
	uv run python -m scripts.demo mock offline

# Restore the mock HTTP API availability after an outage
demo.mock-online:
	uv run python -m scripts.demo mock online

# Start the ETL worker (foreground; stop with Ctrl+C or `make worker.kill`)
worker:
	uv run -m src.workflows.etl.worker

# Kill any running ETL worker processes
worker.kill:
	@echo "Killing ETL worker processes..."
	@pids=$$(pgrep -f "python.*-m[[:space:]]*src\\.workflows\\.etl\\.worker" || true); \
	 [ -z "$$pids" ] || kill -TERM $$pids 2>/dev/null || true; \
	 pids=$$(pgrep -f "uv.*-m[[:space:]]*src\\.workflows\\.etl\\.worker" || true); \
	 [ -z "$$pids" ] || kill -TERM $$pids 2>/dev/null || true; \
	 pids=$$(pgrep -f "src/workflows/etl/worker\\.py" || true); \
	 [ -z "$$pids" ] || kill -TERM $$pids 2>/dev/null || true; \
	 sleep 0.3; \
	 pids=$$(pgrep -f "python.*-m[[:space:]]*src\\.workflows\\.etl\\.worker" || true); \
	 [ -z "$$pids" ] || kill -KILL $$pids 2>/dev/null || true; \
	 pids=$$(pgrep -f "uv.*-m[[:space:]]*src\\.workflows\\.etl\\.worker" || true); \
	 [ -z "$$pids" ] || kill -KILL $$pids 2>/dev/null || true; \
	 pids=$$(pgrep -f "src/workflows/etl/worker\\.py" || true); \
	 [ -z "$$pids" ] || kill -KILL $$pids 2>/dev/null || true; \
	 echo "ETL worker kill complete."

# Kill all workflow workers across packages
workers.kill:
	@echo "Killing all workflow worker processes..."
	@for pat in \
		"python.*-m[[:space:]]*src\\.workflows\\\..*\\.worker" \
		"uv.*-m[[:space:]]*src\\.workflows\\\..*\\.worker" \
		"src/workflows/.*/worker\\.py"; \
	 do \
	   pids=$$(pgrep -f "$$pat" || true); \
	   [ -z "$$pids" ] || kill -TERM $$pids 2>/dev/null || true; \
	 done; \
	 sleep 0.3; \
	 for pat in \
		"python.*-m[[:space:]]*src\\.workflows\\\..*\\.worker" \
		"uv.*-m[[:space:]]*src\\.workflows\\\..*\\.worker" \
		"src/workflows/.*/worker\\.py"; \
	 do \
	   pids=$$(pgrep -f "$$pat" || true); \
	   [ -z "$$pids" ] || kill -KILL $$pids 2>/dev/null || true; \
	 done; \
	 echo "Worker kill complete."

# Aliases scoped explicitly as Temporal workers
temporal.worker.kill: worker.kill
temporal.workers.kill: workers.kill
