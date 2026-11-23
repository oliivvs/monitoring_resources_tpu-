
"""N26: регрессионные тесты сквозного сценария HEARTBEAT."""

from scripts.heartbeat_e2e import run_smoke


def test_status_heartbeat_retry_and_deduplication():
    result = run_smoke()

    assert (
        result.status_event_id
        != result.heartbeat_event_id
    )

    assert result.http_attempts == 3
    assert result.heartbeat_attempts == 2
    assert result.duplicate_http_status == 200
    assert result.conflicting_http_status == 409


def test_smoke_is_isolated_between_runs():
    first = run_smoke()
    second = run_smoke()

    assert (
        first.status_event_id
        != second.status_event_id
    )

    assert (
        first.heartbeat_event_id
        != second.heartbeat_event_id
    )
