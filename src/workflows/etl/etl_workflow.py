"""Durable ETL workflow orchestrating extract, transform, and load stages."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import List

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .activities import (
        enumerate_batches,
        fetch_http_batch,
        fetch_object_batch,
        load_batch,
        transform_batch,
    )

    from .models import (
        EnumerateBatchesInput,
        EtlWorkflowInput,
        HttpSourceConfig,
        HttpBatchRequest,
        ObjectBatchRequest,
        LoadBatchRequest,
        ProgressSnapshot,
        SourceBatch,
        SourceType,
        TransformBatchRequest,
    )
#Handle invocations from the CLI
async def main() -> None:  # pragma: no cover
    """Manual entry point for executing the workflow directly."""
    from temporalio.client import Client  # noqa: PLC0415
    from temporalio.contrib.pydantic import pydantic_data_converter  # noqa: PLC0415

    client = await Client.connect(
        "localhost:7233",
        data_converter=pydantic_data_converter,
    )
    input_data = EtlWorkflowInput(
        run_id="cli-run",
        source_type=SourceType.HTTP_API,
        http_source=HttpSourceConfig(base_url="http://localhost:8081"),
    )
    result = await client.execute_workflow(
        DurableEtlWorkflow.run,
        input_data,
        id="etl-demo-cli",
        task_queue="etl-task-queue",
    )
    print(result.model_dump_json(indent=2))  # noqa: T201

### Define the Durable ETL Workflow ###
@workflow.defn
class DurableEtlWorkflow:

    ### We establish a class and constructor to track custom state information about the overall job ###
    def __init__(self) -> None:
        self._input: EtlWorkflowInput | None = None
        self._paused = False
        self._progress: ProgressSnapshot | None = None
        self._retry_count = 0
        self._semaphore: asyncio.Semaphore | None = None

    ### Here we define the logic of the worklfow ####
    @workflow.run
    async def run(self, input_data: EtlWorkflowInput) -> ProgressSnapshot:
        
        await asyncio.sleep(30) # Insert a wait time to launch the UI
        
        workflow.logger.info(
            "Starting ETL workflow run_id=%s source=%s",
            input_data.run_id,
            input_data.source_type.value,
        )

        #Set initial state
        self._input = input_data
        self._progress = ProgressSnapshot(
            run_id=input_data.run_id,
            total_batches=0,
            batches_completed=0,
            total_items=0,
            items_processed=0,
            retry_count=0,
            paused=False,
            last_error=None,
            last_updated=workflow.now(),
        )

        #Generate batches from dataset
        batches = await workflow.execute_activity(
            enumerate_batches,
            EnumerateBatchesInput(workflow_input=input_data),
            start_to_close_timeout=timedelta(seconds=20),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=2),
                backoff_coefficient=2.0,
            ),
        )

        #Update workflow state
        self._progress.total_batches = len(batches)
        self._update_progress()
        self._semaphore = asyncio.Semaphore(input_data.max_concurrent_batches)
        

        #Parcel out batches into independent tasks
        async with asyncio.TaskGroup() as tg:
            await self._wait_if_paused()
            tasks = [tg.create_task(self._process_batch(batch)) for batch in batches]
        
        results = [await t for t in tasks]
        

        workflow.logger.info(
            "ETL workflow %s finished (%s/%s batches, %s items)",
            input_data.run_id,
            self._progress.batches_completed,
            self._progress.total_batches,
            self._progress.items_processed,
        )
        self._update_progress()
        return self._progress
    #Pause the process mid-stream
    @workflow.signal
    async def pause(self) -> None:
        """Pause processing of new batches (in-flight work continues)."""
        if not self._paused:
            workflow.logger.warning("Pause signal received; throttling new batch fan-out.")
        self._paused = True
        # Signals let operators apply backpressure without losing deterministic state
        self._update_progress()
    #Resume the process if paused
    @workflow.signal
    async def resume(self) -> None:
        """Resume processing after a pause."""
        if self._paused:
            workflow.logger.warning("Resume signal received; continuing batch fan-out.")
        self._paused = False
        self._update_progress()

    @workflow.query
    def progress(self) -> ProgressSnapshot:
        """Return the most recent progress snapshot."""
        if self._progress is None:
            return ProgressSnapshot(
                run_id="initializing",
                total_batches=0,
                batches_completed=0,
                total_items=0,
                items_processed=0,
                retry_count=self._retry_count,
                paused=self._paused,
                last_error=None,
                last_updated=workflow.now(),
            )
        snapshot = self._progress.copy()
        snapshot.last_updated = workflow.now()
        snapshot.paused = self._paused
        snapshot.retry_count = self._retry_count
        return snapshot
    
    async def _process_batch(self, batch: SourceBatch) -> None:
        """Process a single batch through extract, transform, and load activities."""
        assert self._input is not None
        assert self._semaphore is not None
        assert self._progress is not None
        await self._semaphore.acquire()
        try:
            await self._wait_if_paused()
            extract_result = await self._run_extract(batch)
            self._increment_retries(extract_result.attempt)
            self._progress.total_items += len(extract_result.records)
            self._update_progress()

            transform_result = await self._run_transform(batch, extract_result.records)
            self._increment_retries(transform_result.attempt)


            load_result = await self._run_load(batch, transform_result.records)
            self._increment_retries(load_result.attempt)
            items_processed = len(transform_result.records)
            self._progress.items_processed += items_processed
            self._progress.batches_completed += 1
            workflow.logger.info(
                "Completed batch %s: processed=%s inserted=%s skipped=%s",
                batch.batch_id,
                items_processed,
                load_result.inserted_count,
                load_result.skipped_count,
            )
            self._update_progress(last_error=None)
        except Exception as exc:  # pragma: no cover - defensive
            workflow.logger.error(
                "Batch %s failed in workflow run %s: %s",
                batch.batch_id,
                self._input.run_id,
                exc,
            )
            self._update_progress(last_error=str(exc))
            raise
        finally:
            self._semaphore.release()

    async def _run_extract(self, batch: SourceBatch):
        """Execute the appropriate extract activity based on source type."""
        assert self._input is not None
        if self._input.source_type is SourceType.HTTP_API:
            http_config = self._input.http_source
            assert http_config is not None
            return await workflow.execute_activity(
                fetch_http_batch,
                HttpBatchRequest(config=http_config, batch=batch),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=self._retry_policy(
                    non_retryable_errors=["NonRetryableSourceError"]
                ),
            )
        object_config = self._input.object_source
        assert object_config is not None
        return await workflow.execute_activity(
            fetch_object_batch,
            ObjectBatchRequest(config=object_config, batch=batch),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=self._retry_policy(
                non_retryable_errors=["NonRetryableSourceError"]
            ),
        )

    async def _run_transform(self, batch: SourceBatch, records):
        """Execute transform stage."""
        assert self._input is not None
        return await workflow.execute_activity(
            transform_batch,
            TransformBatchRequest(
                run_id=self._input.run_id,
                config=self._input.transform,
                batch=batch,
                records=records,
            ),
            start_to_close_timeout=timedelta(seconds=20),
            retry_policy=self._retry_policy(non_retryable_errors=[]),
        )

    async def _run_load(self, batch: SourceBatch, records):
        """Execute load stage against the warehouse sink."""
        assert self._input is not None
        return await workflow.execute_activity(
            load_batch,
            LoadBatchRequest(
                run_id=self._input.run_id,
                config=self._input.sink,
                batch=batch,
                records=records,
            ),
            start_to_close_timeout=timedelta(seconds=120),
            heartbeat_timeout=timedelta(seconds=30),
            retry_policy=self._retry_policy(non_retryable_errors=["SinkWriteError"]),
        )

    def _retry_policy(self, non_retryable_errors: List[str]) -> RetryPolicy:
        """Create a retry policy shared across activities."""
        return RetryPolicy(
            maximum_attempts=6,
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            non_retryable_error_types=non_retryable_errors,
        )

    def _increment_retries(self, attempt: int) -> None:
        """Track retry attempts for progress reporting."""
        if attempt > 1:
            self._retry_count += attempt - 1
        self._progress.retry_count = self._retry_count
        self._update_progress()

    async def _wait_if_paused(self) -> None:
        """Suspend scheduling of new batches when the workflow is paused."""
        await workflow.wait_condition(lambda: not self._paused)

    def _update_progress(self, last_error: str | None = None) -> None:
        """Refresh the cached progress snapshot with the latest state."""
        if self._progress is None:
            return
        self._progress.paused = self._paused
        self._progress.retry_count = self._retry_count
        if last_error is not None:
            self._progress.last_error = last_error
        self._progress.last_updated = workflow.now()





if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
