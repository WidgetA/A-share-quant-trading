from __future__ import annotations

from src.web import v15_scan_service
from src.web.v20_canonical_selection import V20CanonicalSelectionState


def test_v16_runtime_has_no_post_window_canonical_restore_surface() -> None:
    """V16 cannot be repopulated or vetoed by a V20 durable artifact."""

    state = v15_scan_service.V15ScanState()
    assert not hasattr(state, "canonical_coordinator")
    assert not hasattr(state, "canonical_sink")
    assert not hasattr(state, "canonical_artifact_probe")
    assert not hasattr(state, "canonical_durable_received_at")
    assert not hasattr(v15_scan_service, "_restore_canonical_artifact")
    assert not hasattr(v15_scan_service, "_fail_not_ready_deadline")


def test_v20_canonical_state_is_not_a_v16_runtime_state() -> None:
    state = V20CanonicalSelectionState()
    assert not isinstance(state, v15_scan_service.V15ScanState)
    assert hasattr(state, "canonical_coordinator")
    assert hasattr(state, "canonical_sink")
    assert not hasattr(state, "today_recommendation")
    assert not hasattr(state, "scan_done_date")
    assert not hasattr(state, "scheduler_task")
