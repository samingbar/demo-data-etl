#
# Generated Makefile for Temporal ETL demo
.PHONY: demo demo.pause demo.resume demo.status demo.clean

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
