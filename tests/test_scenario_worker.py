from atlas.database.run_repository import RunClaim, RunFailure
from atlas.events.kafka import KafkaCommitError, KafkaEvent
from atlas.workers.scenario_worker import ScenarioRequestedPayload, ScenarioWorker


def event_value(scenario_id: str = "scn_123") -> dict[str, object]:
    return {
        "event_id": "7bb84de4-738d-49f1-a293-4211ccea08ab",
        "event_type": "scenario.requested",
        "schema_version": 1,
        "event_time": "2026-09-23T20:00:00Z",
        "producer": "atlas-api",
        "key": scenario_id,
        "trace_id": "trace-123",
        "payload": {
            "scenario_id": scenario_id,
            "request_hash": "a" * 64,
            "model_version": "test-model-v1",
            "data_version": "test-data-v1",
            "request": {"name": "Worker test"},
        },
    }


class FakeConsumer:
    def __init__(self, value: dict[str, object], *, commit_error: bool = False) -> None:
        self.event = KafkaEvent("topic", 0, 7, "scn_123", value, object())
        self.commit_error = commit_error
        self.committed: list[KafkaEvent] = []
        self.retried: list[KafkaEvent] = []

    def poll(self, timeout_seconds: float = 1.0) -> KafkaEvent | None:
        del timeout_seconds
        event, self.event = self.event, None  # type: ignore[assignment]
        return event

    def commit(self, event: KafkaEvent) -> None:
        if self.commit_error:
            raise KafkaCommitError("consumer group membership changed")
        self.committed.append(event)

    def retry(self, event: KafkaEvent) -> None:
        self.retried.append(event)

    def close(self) -> None:
        pass


class FakeRuns:
    def __init__(self, *, claimed: bool = True, retryable: bool = False) -> None:
        self.claimed = claimed
        self.retryable = retryable
        self.completed: list[tuple[str, str]] = []
        self.failed: list[tuple[str, str, str]] = []

    def claim(self, scenario_id: str) -> RunClaim:
        assert scenario_id == "scn_123"
        return RunClaim(
            "job_123",
            "running" if self.claimed else "completed",
            self.claimed,
            1,
            3,
            "optimizing" if self.claimed else "completed",
        )

    def complete(self, job_id: str, optimizer_run_id: str) -> None:
        self.completed.append((job_id, optimizer_run_id))

    def fail(
        self,
        job_id: str,
        error_message: str,
        error_type: str = "RuntimeError",
    ) -> RunFailure:
        self.failed.append((job_id, error_message, error_type))
        return RunFailure(
            retryable=self.retryable,
            attempt=1,
            max_attempts=3,
            failure_stage="optimizing",
        )


class RecordingProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.payloads: list[ScenarioRequestedPayload] = []
        self.error = error

    def process(self, payload: ScenarioRequestedPayload, job_id: str) -> str:
        assert job_id == "job_123"
        self.payloads.append(payload)
        if self.error is not None:
            raise self.error
        return "opt_run_123"


def test_worker_completes_and_commits_claimed_job() -> None:
    consumer = FakeConsumer(event_value())
    runs = FakeRuns()
    processor = RecordingProcessor()
    worker = ScenarioWorker(consumer, runs, processor)

    assert worker.process_one() is True

    assert runs.completed == [("job_123", "opt_run_123")]
    assert runs.failed == []
    assert processor.payloads[0].scenario_id == "scn_123"
    assert len(consumer.committed) == 1


def test_worker_marks_processing_failure_terminal_and_commits() -> None:
    consumer = FakeConsumer(event_value())
    runs = FakeRuns()
    processor = RecordingProcessor(RuntimeError("solver failed"))
    worker = ScenarioWorker(consumer, runs, processor)

    assert worker.process_one() is True

    assert runs.completed == []
    assert runs.failed == [("job_123", "solver failed", "RuntimeError")]
    assert len(consumer.committed) == 1


def test_worker_retries_transient_failure_without_committing() -> None:
    consumer = FakeConsumer(event_value())
    runs = FakeRuns(retryable=True)
    processor = RecordingProcessor(RuntimeError("temporary database failure"))
    worker = ScenarioWorker(consumer, runs, processor)

    assert worker.process_one() is True

    assert runs.completed == []
    assert runs.failed == [
        ("job_123", "temporary database failure", "RuntimeError")
    ]
    assert len(consumer.retried) == 1
    assert consumer.committed == []


def test_worker_skips_already_completed_duplicate() -> None:
    consumer = FakeConsumer(event_value())
    runs = FakeRuns(claimed=False)
    processor = RecordingProcessor()
    worker = ScenarioWorker(consumer, runs, processor)

    assert worker.process_one() is True

    assert processor.payloads == []
    assert len(consumer.committed) == 1


def test_worker_survives_commit_failure_after_durable_completion() -> None:
    consumer = FakeConsumer(event_value(), commit_error=True)
    runs = FakeRuns()
    processor = RecordingProcessor()
    worker = ScenarioWorker(consumer, runs, processor)

    assert worker.process_one() is True
    assert runs.completed == [("job_123", "opt_run_123")]
