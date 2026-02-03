# === MODULE PURPOSE ===
# Unit tests for BackfillManager recovery mechanism.

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.backfill_manager import (
    BackfillManager,
    BackfillReport,
    BackfillStrategy,
    BackfillTask,
)
from src.common.state_manager import StateManager, SystemState
from src.data.models.message import Message
from src.data.sources.base import BaseMessageSource


# === TEST FIXTURES ===


class MockNewsSource(BaseMessageSource):
    """Mock news source that doesn't support historical queries."""

    def __init__(self, messages: list[Message] | None = None):
        super().__init__(interval=30.0)
        self._messages = messages or []

    @property
    def source_type(self) -> str:
        return "news"

    @property
    def source_name(self) -> str:
        return "mock_news"

    @property
    def supports_historical(self) -> bool:
        return False

    async def fetch_messages(self) -> AsyncIterator[Message]:
        for msg in self._messages:
            if not self.is_duplicate(msg.id):
                yield msg


class MockAnnouncementSource(BaseMessageSource):
    """Mock announcement source that supports historical queries."""

    def __init__(self, messages: list[Message] | None = None):
        super().__init__(interval=300.0)
        self._messages = messages or []

    @property
    def source_type(self) -> str:
        return "announcement"

    @property
    def source_name(self) -> str:
        return "mock_announcement"

    @property
    def supports_historical(self) -> bool:
        return True

    async def fetch_messages(self) -> AsyncIterator[Message]:
        for msg in self._messages:
            if not self.is_duplicate(msg.id):
                yield msg

    async def fetch_by_date_range(self, start_date: str, end_date: str) -> AsyncIterator[Message]:
        """Fetch messages within date range."""
        for msg in self._messages:
            if not self.is_duplicate(msg.id):
                yield msg


def create_test_message(
    source_name: str,
    publish_time: datetime,
    title: str = "Test message",
) -> Message:
    """Create a test message with specified parameters."""
    msg_id = f"{source_name}_{publish_time.isoformat()}_{title}"
    return Message(
        id=msg_id,
        source_type="news",
        source_name=source_name,
        title=title,
        content=f"Content for {title}",
        url="https://example.com",
        stock_codes=[],
        publish_time=publish_time,
        raw_data={},
    )


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    """Create a temporary database path."""
    return tmp_path / "test_state.db"


@pytest.fixture
async def state_manager(temp_db_path: Path) -> StateManager:
    """Create a connected state manager."""
    manager = StateManager(temp_db_path)
    await manager.connect()
    yield manager
    await manager.close()


@pytest.fixture
def backfill_manager(state_manager: StateManager) -> BackfillManager:
    """Create a backfill manager with test configuration."""
    return BackfillManager(
        state_manager=state_manager,
        min_gap_seconds=60,  # 1 minute minimum gap
        max_gap_hours=24,
    )


# === UNIT TESTS ===


class TestBackfillTask:
    """Tests for BackfillTask dataclass."""

    def test_gap_duration_calculated(self):
        """Gap duration should be calculated automatically."""
        gap_start = datetime(2026, 1, 28, 10, 0, 0)
        gap_end = datetime(2026, 1, 28, 12, 0, 0)

        task = BackfillTask(
            source_name="test",
            source_type="news",
            strategy=BackfillStrategy.BEST_EFFORT,
            gap_start=gap_start,
            gap_end=gap_end,
        )

        assert task.gap_duration == timedelta(hours=2)

    def test_to_dict(self):
        """Task should serialize to dictionary."""
        gap_start = datetime(2026, 1, 28, 10, 0, 0)
        gap_end = datetime(2026, 1, 28, 12, 0, 0)

        task = BackfillTask(
            source_name="test",
            source_type="news",
            strategy=BackfillStrategy.FULL_HISTORICAL,
            gap_start=gap_start,
            gap_end=gap_end,
        )
        task.status = "completed"
        task.messages_fetched = 10
        task.messages_saved = 8

        d = task.to_dict()

        assert d["source_name"] == "test"
        assert d["strategy"] == "full_historical"
        assert d["gap_duration_seconds"] == 7200
        assert d["status"] == "completed"
        assert d["messages_fetched"] == 10
        assert d["messages_saved"] == 8


class TestBackfillReport:
    """Tests for BackfillReport dataclass."""

    def test_no_recovery_needed(self):
        """Report should indicate when no recovery was needed."""
        report = BackfillReport(
            recovery_triggered=False,
            system_down_at=None,
            system_up_at=datetime.now(),
            total_downtime=None,
        )

        assert not report.recovery_triggered
        assert len(report.tasks) == 0

    def test_aggregate_stats(self):
        """Report should track aggregate statistics."""
        report = BackfillReport(
            recovery_triggered=True,
            system_down_at=datetime(2026, 1, 28, 10, 0, 0),
            system_up_at=datetime(2026, 1, 28, 12, 0, 0),
            total_downtime=timedelta(hours=2),
        )
        report.total_messages_fetched = 100
        report.total_messages_saved = 85
        report.sources_fully_recovered = ["source1"]
        report.sources_partially_recovered = ["source2"]
        report.sources_failed = ["source3"]

        d = report.to_dict()

        assert d["recovery_triggered"] is True
        assert d["total_downtime_seconds"] == 7200
        assert d["total_messages_fetched"] == 100
        assert d["total_messages_saved"] == 85


class TestBackfillManager:
    """Tests for BackfillManager class."""

    @pytest.mark.asyncio
    async def test_check_recovery_not_needed_clean_shutdown(
        self, backfill_manager: BackfillManager, state_manager: StateManager
    ):
        """No recovery needed if system was shut down cleanly."""
        # Simulate clean shutdown
        await state_manager.save_state(SystemState.STOPPED)

        needs_recovery = await backfill_manager.check_recovery_needed()

        assert not needs_recovery

    @pytest.mark.asyncio
    async def test_check_recovery_needed_after_crash(
        self, backfill_manager: BackfillManager, state_manager: StateManager
    ):
        """Recovery needed if system was in RUNNING state."""
        # Simulate crash (state left as RUNNING)
        await state_manager.save_state(SystemState.RUNNING)

        needs_recovery = await backfill_manager.check_recovery_needed()

        assert needs_recovery

    @pytest.mark.asyncio
    async def test_save_and_get_fetch_time(self, backfill_manager: BackfillManager):
        """Should save and retrieve fetch timestamps."""
        source_name = "test_source"
        fetch_time = datetime(2026, 1, 28, 10, 30, 0)

        await backfill_manager.save_fetch_time(source_name, fetch_time)
        retrieved = await backfill_manager.get_last_fetch_time(source_name)

        assert retrieved == fetch_time

    @pytest.mark.asyncio
    async def test_get_fetch_time_not_found(self, backfill_manager: BackfillManager):
        """Should return None for unknown source."""
        result = await backfill_manager.get_last_fetch_time("unknown_source")

        assert result is None

    def test_determine_strategy_for_historical_source(self, backfill_manager: BackfillManager):
        """Source with supports_historical=True gets FULL_HISTORICAL strategy."""
        source = MockAnnouncementSource()

        strategy = backfill_manager.determine_strategy(source)

        assert strategy == BackfillStrategy.FULL_HISTORICAL

    def test_determine_strategy_for_news_source(self, backfill_manager: BackfillManager):
        """Source without historical support gets BEST_EFFORT strategy."""
        source = MockNewsSource()

        strategy = backfill_manager.determine_strategy(source)

        assert strategy == BackfillStrategy.BEST_EFFORT

    @pytest.mark.asyncio
    async def test_create_backfill_tasks_with_gap(self, backfill_manager: BackfillManager):
        """Should create tasks for sources with gaps."""
        source = MockNewsSource()
        last_fetch = datetime.now() - timedelta(hours=2)

        # Save checkpoint
        await backfill_manager.save_fetch_time(source.source_name, last_fetch)

        tasks = await backfill_manager.create_backfill_tasks([source])

        assert len(tasks) == 1
        assert tasks[0].source_name == "mock_news"
        assert tasks[0].strategy == BackfillStrategy.BEST_EFFORT
        assert tasks[0].gap_duration >= timedelta(hours=1, minutes=59)

    @pytest.mark.asyncio
    async def test_create_backfill_tasks_no_gap(self, backfill_manager: BackfillManager):
        """Should not create tasks for sources without gaps."""
        source = MockNewsSource()
        last_fetch = datetime.now() - timedelta(seconds=30)  # Below min_gap

        await backfill_manager.save_fetch_time(source.source_name, last_fetch)

        tasks = await backfill_manager.create_backfill_tasks([source])

        assert len(tasks) == 0

    @pytest.mark.asyncio
    async def test_create_backfill_tasks_no_checkpoint(self, backfill_manager: BackfillManager):
        """Should not create tasks for sources without checkpoints."""
        source = MockNewsSource()  # No checkpoint saved

        tasks = await backfill_manager.create_backfill_tasks([source])

        assert len(tasks) == 0

    @pytest.mark.asyncio
    async def test_execute_backfill_task_best_effort(self, backfill_manager: BackfillManager):
        """Best-effort backfill should fetch available messages."""
        now = datetime.now()
        gap_start = now - timedelta(hours=2)

        # Create messages within the gap period
        messages = [
            create_test_message("mock_news", gap_start + timedelta(minutes=30)),
            create_test_message("mock_news", gap_start + timedelta(minutes=60)),
            create_test_message("mock_news", gap_start + timedelta(minutes=90)),
        ]
        source = MockNewsSource(messages)

        task = BackfillTask(
            source_name="mock_news",
            source_type="news",
            strategy=BackfillStrategy.BEST_EFFORT,
            gap_start=gap_start,
            gap_end=now,
        )

        saved_messages = []

        async def save_callback(msg: Message) -> bool:
            saved_messages.append(msg)
            return True

        await backfill_manager.execute_backfill_task(task, source, save_callback)

        assert task.status == "partial"  # Best-effort is always partial
        assert task.messages_fetched == 3
        assert task.messages_saved == 3
        assert len(saved_messages) == 3

    @pytest.mark.asyncio
    async def test_execute_backfill_task_full_historical(self, backfill_manager: BackfillManager):
        """Full historical backfill should use date range query."""
        now = datetime.now()
        gap_start = now - timedelta(hours=2)

        messages = [
            create_test_message("mock_announcement", gap_start + timedelta(minutes=30)),
            create_test_message("mock_announcement", gap_start + timedelta(minutes=60)),
        ]
        source = MockAnnouncementSource(messages)

        task = BackfillTask(
            source_name="mock_announcement",
            source_type="announcement",
            strategy=BackfillStrategy.FULL_HISTORICAL,
            gap_start=gap_start,
            gap_end=now,
        )

        saved_messages = []

        async def save_callback(msg: Message) -> bool:
            saved_messages.append(msg)
            return True

        await backfill_manager.execute_backfill_task(task, source, save_callback)

        assert task.status == "completed"
        assert task.messages_fetched == 2
        assert task.messages_saved == 2

    @pytest.mark.asyncio
    async def test_execute_recovery_no_recovery_needed(
        self, backfill_manager: BackfillManager, state_manager: StateManager
    ):
        """Should return report with recovery_triggered=False."""
        # Simulate clean shutdown
        await state_manager.save_state(SystemState.STOPPED)

        async def save_callback(msg: Message) -> bool:
            return True

        report = await backfill_manager.execute_recovery([], save_callback)

        assert not report.recovery_triggered
        assert len(report.tasks) == 0

    @pytest.mark.asyncio
    async def test_execute_recovery_with_multiple_sources(
        self, backfill_manager: BackfillManager, state_manager: StateManager
    ):
        """Should handle recovery for multiple sources."""
        # Simulate crash
        await state_manager.save_state(SystemState.RUNNING)

        now = datetime.now()
        gap_start = now - timedelta(hours=1)

        # Create sources with checkpoints
        news_source = MockNewsSource(
            [
                create_test_message("mock_news", gap_start + timedelta(minutes=15)),
            ]
        )
        ann_source = MockAnnouncementSource(
            [
                create_test_message("mock_announcement", gap_start + timedelta(minutes=30)),
            ]
        )

        await backfill_manager.save_fetch_time("mock_news", gap_start)
        await backfill_manager.save_fetch_time("mock_announcement", gap_start)

        saved_messages = []

        async def save_callback(msg: Message) -> bool:
            saved_messages.append(msg)
            return True

        report = await backfill_manager.execute_recovery([news_source, ann_source], save_callback)

        assert report.recovery_triggered
        assert len(report.tasks) == 2
        assert report.total_messages_saved == 2
        assert "mock_announcement" in report.sources_fully_recovered
        assert "mock_news" in report.sources_partially_recovered

    @pytest.mark.asyncio
    async def test_execute_backfill_task_handles_errors_full_historical(
        self, backfill_manager: BackfillManager
    ):
        """Full historical backfill should fail on errors."""
        now = datetime.now()
        gap_start = now - timedelta(hours=1)

        # Create source that raises error via async generator
        class ErrorSource(MockAnnouncementSource):
            async def fetch_by_date_range(self, start_date: str, end_date: str):
                raise Exception("API Error")
                yield  # Make it a generator

        source = ErrorSource([])

        task = BackfillTask(
            source_name="mock_announcement",
            source_type="announcement",
            strategy=BackfillStrategy.FULL_HISTORICAL,
            gap_start=gap_start,
            gap_end=now,
        )

        async def save_callback(msg: Message) -> bool:
            return True

        await backfill_manager.execute_backfill_task(task, source, save_callback)

        assert task.status == "failed"
        assert "API Error" in task.error_message

    @pytest.mark.asyncio
    async def test_execute_backfill_task_best_effort_resilient(
        self, backfill_manager: BackfillManager
    ):
        """Best-effort backfill should continue despite fetch errors."""
        now = datetime.now()
        gap_start = now - timedelta(hours=1)

        # Create source that raises error via async generator
        class ErrorSource(MockNewsSource):
            async def fetch_messages(self):
                raise Exception("API Error")
                yield  # Make it a generator

        source = ErrorSource([])

        task = BackfillTask(
            source_name="mock_news",
            source_type="news",
            strategy=BackfillStrategy.BEST_EFFORT,
            gap_start=gap_start,
            gap_end=now,
        )

        async def save_callback(msg: Message) -> bool:
            return True

        await backfill_manager.execute_backfill_task(task, source, save_callback)

        # Best-effort should complete (with 0 messages) even if fetches fail
        assert task.status == "completed"
        assert task.messages_fetched == 0

    @pytest.mark.asyncio
    async def test_filters_messages_outside_gap(self, backfill_manager: BackfillManager):
        """Should only save messages within the gap period."""
        now = datetime.now()
        gap_start = now - timedelta(hours=1)

        # Create messages - one inside gap, one outside
        messages = [
            create_test_message("mock_announcement", gap_start + timedelta(minutes=30)),  # Inside
            create_test_message("mock_announcement", gap_start - timedelta(hours=1)),  # Before gap
        ]
        source = MockAnnouncementSource(messages)

        task = BackfillTask(
            source_name="mock_announcement",
            source_type="announcement",
            strategy=BackfillStrategy.FULL_HISTORICAL,
            gap_start=gap_start,
            gap_end=now,
        )

        saved_messages = []

        async def save_callback(msg: Message) -> bool:
            saved_messages.append(msg)
            return True

        await backfill_manager.execute_backfill_task(task, source, save_callback)

        # Should have fetched 2 but only saved 1 (within gap)
        assert task.messages_fetched == 2
        assert task.messages_saved == 1
        assert len(saved_messages) == 1


class TestBackfillReportLogging:
    """Tests for BackfillReport logging functionality."""

    def test_log_summary_no_recovery(self, caplog):
        """Should log simple message when no recovery needed."""
        report = BackfillReport(
            recovery_triggered=False,
            system_down_at=None,
            system_up_at=datetime.now(),
            total_downtime=None,
        )

        import logging

        with caplog.at_level(logging.INFO):
            report.log_summary()

        assert "No recovery needed" in caplog.text

    def test_log_summary_with_recovery(self, caplog):
        """Should log detailed summary for recovery."""
        report = BackfillReport(
            recovery_triggered=True,
            system_down_at=datetime(2026, 1, 28, 10, 0, 0),
            system_up_at=datetime(2026, 1, 28, 12, 0, 0),
            total_downtime=timedelta(hours=2),
        )
        report.total_messages_fetched = 50
        report.total_messages_saved = 45
        report.sources_fully_recovered = ["source1"]
        report.sources_partially_recovered = ["source2"]
        report.unrecoverable_gaps = [
            {
                "source_name": "source2",
                "gap_start": "2026-01-28T10:00:00",
                "gap_end": "2026-01-28T12:00:00",
                "reason": "API does not support historical queries",
            }
        ]

        import logging

        with caplog.at_level(logging.INFO):
            report.log_summary()

        assert "BACKFILL RECOVERY REPORT" in caplog.text
        assert "Total messages fetched: 50" in caplog.text
        assert "Fully recovered" in caplog.text
        assert "Partially recovered" in caplog.text
        assert "UNRECOVERABLE GAPS" in caplog.text
