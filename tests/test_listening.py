"""
Tests for listening.py — Fixes 4, 5
- Fix 4: Circuit breaker on consecutive ChangeStreamHistoryLost events
- Fix 5: Do NOT persist None resume token to LZ
"""
import os
import pytest
import pickle
import time
import threading
from unittest.mock import patch, MagicMock, PropertyMock, call
from bson.timestamp import Timestamp
import pymongo.errors

from listening import listening, process_accumulative_df
from file_utils import FileType
from tests.conftest import make_change_event, make_documents


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _make_operation_failure_286():
    """Create a fake OperationFailure with code 286 (ChangeStreamHistoryLost)."""
    exc = pymongo.errors.OperationFailure(
        "ChangeStreamHistoryLost", code=286,
    )
    return exc


def _make_resumable_error():
    """Create a fake resumable ConnectionFailure."""
    return pymongo.errors.ConnectionFailure("connection reset by peer")


# ─────────────────────────────────────────────
# Fix 4: Circuit Breaker
# ─────────────────────────────────────────────
class TestCircuitBreaker:
    """Verify the circuit breaker stops after MAX_CONSECUTIVE_HISTORY_LOSS."""

    @patch("listening.write_to_file")
    @patch("listening.read_from_file")
    @patch("listening.pymongo.MongoClient")
    @patch("listening.time.sleep")
    def test_circuit_breaker_stops_after_max_failures(
        self, mock_sleep, mock_client_cls, mock_read, mock_write, env_vars
    ):
        """After 3 consecutive ChangeStreamHistoryLost, the listener should return."""
        mock_read.side_effect = lambda *args, **kwargs: None  # no resume token, no cluster time

        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        # Every watch() call raises ChangeStreamHistoryLost
        mock_collection.watch.side_effect = _make_operation_failure_286()

        # Run listening — it should return after MAX_CONSECUTIVE_HISTORY_LOSS failures
        listening("testcoll")

        # Should have written the _NEEDS_REINIT sentinel
        sentinel_calls = [
            c for c in mock_write.call_args_list
            if len(c.args) >= 3 and c.args[2] == "_NEEDS_REINIT.txt"
        ]
        assert len(sentinel_calls) == 1, "Should write _NEEDS_REINIT sentinel file"
        assert sentinel_calls[0].args[0] == "NEEDS_REINIT"

    @patch("listening.write_to_file")
    @patch("listening.read_from_file")
    @patch("listening.pymongo.MongoClient")
    @patch("listening.time.sleep")
    def test_circuit_breaker_resets_on_resumable_error(
        self, mock_sleep, mock_client_cls, mock_read, mock_write, env_vars
    ):
        """A resumable error between two history-loss events resets the counter."""
        mock_read.side_effect = lambda *args, **kwargs: None

        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        call_count = [0]

        def watch_side_effect(**kwargs):
            call_count[0] += 1
            n = call_count[0]
            if n <= 2:
                raise _make_operation_failure_286()
            elif n == 3:
                raise _make_resumable_error()  # resets the counter
            elif n <= 5:
                raise _make_operation_failure_286()
            else:
                # After 3 more (total 5), circuit breaker should fire
                raise _make_operation_failure_286()

        mock_collection.watch.side_effect = watch_side_effect

        listening("testcoll")

        # The circuit breaker fired after 5 watch calls (2 history loss + 1 resumable + 3 history loss)
        # But wait — the counter resets on resumable, so it's 3 consecutive after the reset
        # 2 (history) + 1 (resumable, resets) + 3 (history) = 6 total watch calls, then return
        assert call_count[0] == 6

        sentinel_calls = [
            c for c in mock_write.call_args_list
            if len(c.args) >= 3 and c.args[2] == "_NEEDS_REINIT.txt"
        ]
        assert len(sentinel_calls) == 1


# ─────────────────────────────────────────────
# Fix 5: Do NOT persist None resume token
# ─────────────────────────────────────────────
class TestNoPersistNoneToken:
    """Verify that ChangeStreamHistoryLost does NOT write None to LZ."""

    @patch("listening.write_to_file")
    @patch("listening.read_from_file")
    @patch("listening.pymongo.MongoClient")
    @patch("listening.time.sleep")
    def test_none_token_not_written_to_lz(
        self, mock_sleep, mock_client_cls, mock_read, mock_write, env_vars
    ):
        """On ChangeStreamHistoryLost, write_to_file should NOT be called with None for resume token."""
        mock_read.side_effect = lambda *args, **kwargs: None

        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        mock_collection.watch.side_effect = _make_operation_failure_286()

        listening("testcoll")

        # Check all write_to_file calls
        for c in mock_write.call_args_list:
            if len(c.args) >= 3 and "_resume_token" in str(c.args[2]):
                assert c.args[0] is not None, (
                    "write_to_file should NOT be called with None for resume token"
                )

    @patch("listening.write_to_file")
    @patch("listening.read_from_file")
    @patch("listening.pymongo.MongoClient")
    @patch("listening.time.sleep")
    def test_in_memory_token_cleared_but_lz_file_preserved(
        self, mock_sleep, mock_client_cls, mock_read, mock_write, env_vars
    ):
        """In-memory resume_token should be None after history loss, but LZ file untouched."""
        mock_read.side_effect = lambda *args, **kwargs: None

        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        # First call raises history loss, second call raises it again (to trigger circuit breaker)
        mock_collection.watch.side_effect = _make_operation_failure_286()

        listening("testcoll")

        # The only write_to_file calls should be for _NEEDS_REINIT.txt
        token_writes = [
            c for c in mock_write.call_args_list
            if len(c.args) >= 3 and "_resume_token" in str(c.args[2])
        ]
        assert len(token_writes) == 0, (
            f"Expected no writes to resume token file, but got: {token_writes}"
        )


# ─────────────────────────────────────────────
# process_accumulative_df tests
# ─────────────────────────────────────────────
class TestProcessAccumulativeDf:
    @patch("listening.push_file_to_lz")
    @patch("listening.write_to_file")
    @patch("listening.read_from_file")
    def test_flushes_when_batch_size_reached(
        self, mock_read, mock_write, mock_push, env_vars
    ):
        """Should write parquet and push to LZ when batch size is reached."""
        import pandas as pd

        mock_read.return_value = 0  # last_parquet_file_num = 0

        df = pd.DataFrame([{"_id": i, "val": i} for i in range(100)])
        logger = MagicMock()

        result_df, result_time = process_accumulative_df(
            accumulative_df=df,
            collection_name="testcoll",
            init_sync_stat_flag="Y",
            last_sync_time=time.time(),
            time_threshold_in_sec=180,
            resume_token={"_data": "test"},
            logger=logger,
        )

        assert result_df is None, "DataFrame should be flushed (None)"
        mock_push.assert_called_once()
        # Should write resume token and last parquet number
        assert mock_write.call_count == 2

    @patch("listening.push_file_to_lz")
    @patch("listening.write_to_file")
    @patch("listening.read_from_file")
    def test_flushes_when_time_threshold_reached(
        self, mock_read, mock_write, mock_push, env_vars
    ):
        """Should flush when time threshold is exceeded even with small batch."""
        import pandas as pd

        mock_read.return_value = 0
        df = pd.DataFrame([{"_id": 1, "val": 1}])  # small batch
        logger = MagicMock()

        result_df, result_time = process_accumulative_df(
            accumulative_df=df,
            collection_name="testcoll",
            init_sync_stat_flag="Y",
            last_sync_time=time.time() - 200,  # 200 seconds ago
            time_threshold_in_sec=180,
            resume_token={"_data": "test"},
            logger=logger,
        )

        assert result_df is None
        mock_push.assert_called_once()

    @patch("listening.push_file_to_lz")
    @patch("listening.write_to_file")
    @patch("listening.read_from_file")
    def test_no_flush_when_below_thresholds(
        self, mock_read, mock_write, mock_push, env_vars
    ):
        """Should NOT flush when below both batch size and time threshold."""
        import pandas as pd

        df = pd.DataFrame([{"_id": 1, "val": 1}])  # 1 row, below batch size of 100
        logger = MagicMock()

        result_df, result_time = process_accumulative_df(
            accumulative_df=df,
            collection_name="testcoll",
            init_sync_stat_flag="Y",
            last_sync_time=time.time(),  # just now
            time_threshold_in_sec=180,
            resume_token={"_data": "test"},
            logger=logger,
        )

        assert result_df is not None, "DataFrame should NOT be flushed"
        mock_push.assert_not_called()
