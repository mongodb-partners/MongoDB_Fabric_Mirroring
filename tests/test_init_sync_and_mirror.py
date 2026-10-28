"""
Tests for init_sync.py — Fix 7 (configurable batch sleep)
and for mongodb_generic_mirroring.py — Fix 6 (init sync retry + always start listener)
"""
import os
import time
import pytest
from unittest.mock import patch, MagicMock, call

from init_sync import init_sync
from mongodb_generic_mirroring import mirror


# ─────────────────────────────────────────────
# Fix 7: Configurable batch sleep
# ─────────────────────────────────────────────
class TestConfigurableSleep:
    @patch("init_sync.pd.DataFrame.to_parquet")
    @patch("init_sync.time.sleep")
    @patch("init_sync.pymongo.MongoClient")
    @patch("init_sync.get_cluster_time")
    @patch("init_sync.delete_file")
    @patch("init_sync.write_to_file")
    @patch("init_sync.read_from_file")
    @patch("init_sync.push_file_to_lz")
    def test_sleep_duration_from_env(
        self, mock_push, mock_read, mock_write, mock_delete,
        mock_cluster_time, mock_client_cls, mock_sleep, mock_to_parquet,
        env_vars, monkeypatch
    ):
        """Init sync should sleep for INIT_SYNC_BATCH_SLEEP_SEC, not hardcoded 30s."""
        monkeypatch.setenv("INIT_SYNC_BATCH_SLEEP_SEC", "7")

        from bson import ObjectId
        from bson.timestamp import Timestamp

        # Set up mock MongoDB
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        # All read_from_file calls return None (no saved state)
        mock_read.return_value = None

        mock_cluster_time.return_value = Timestamp(1000, 1)
        mock_collection.estimated_document_count.return_value = 2

        # Return docs on first find, empty on second
        doc1_id = ObjectId()
        doc2_id = ObjectId()
        doc1 = {"_id": doc1_id, "val": 1}
        doc2 = {"_id": doc2_id, "val": 2}
        find_call_count = [0]

        class MockCursor:
            def __init__(self, docs):
                self.docs = docs
            def sort(self, *a, **kw):
                return self
            def limit(self, *a, **kw):
                return self
            def __iter__(self):
                return iter(self.docs)

        def find_side_effect(*args, **kwargs):
            find_call_count[0] += 1
            if find_call_count[0] == 1:
                return MockCursor([doc1, doc2])
            return MockCursor([])

        mock_collection.find.side_effect = find_side_effect

        # Mock the aggregate for max_id — must return an ObjectId
        mock_agg_result = MagicMock()
        mock_agg_result.next.return_value = {"_id": doc2_id}
        mock_collection.aggregate.return_value = mock_agg_result

        # Mock session
        mock_session = MagicMock()
        mock_client.start_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_client.start_session.return_value.__exit__ = MagicMock(return_value=False)

        with patch("init_sync.schema_utils") as mock_schema:
            mock_schema.process_dataframe = MagicMock()
            mock_schema.finalize_dataframe_for_parquet = MagicMock()
            init_sync("testcoll")

        sleep_calls = [c for c in mock_sleep.call_args_list if c.args[0] == 7]
        assert len(sleep_calls) >= 1, f"Expected sleep(7) but got: {mock_sleep.call_args_list}"

    @patch("init_sync.pd.DataFrame.to_parquet")
    @patch("init_sync.time.sleep")
    @patch("init_sync.pymongo.MongoClient")
    @patch("init_sync.get_cluster_time")
    @patch("init_sync.delete_file")
    @patch("init_sync.write_to_file")
    @patch("init_sync.read_from_file")
    @patch("init_sync.push_file_to_lz")
    def test_default_sleep_is_5_seconds(
        self, mock_push, mock_read, mock_write, mock_delete,
        mock_cluster_time, mock_client_cls, mock_sleep, mock_to_parquet,
        env_vars
    ):
        """Default sleep should be 5s (was 30s)."""
        os.environ.pop("INIT_SYNC_BATCH_SLEEP_SEC", None)

        from bson import ObjectId
        from bson.timestamp import Timestamp

        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        mock_read.return_value = None
        mock_cluster_time.return_value = Timestamp(1000, 1)
        mock_collection.estimated_document_count.return_value = 1

        doc1_id = ObjectId()
        doc1 = {"_id": doc1_id, "val": 1}
        find_call_count = [0]

        class MockCursor:
            def __init__(self, docs):
                self.docs = docs
            def sort(self, *a, **kw):
                return self
            def limit(self, *a, **kw):
                return self
            def __iter__(self):
                return iter(self.docs)

        def find_side_effect(*args, **kwargs):
            find_call_count[0] += 1
            if find_call_count[0] == 1:
                return MockCursor([doc1])
            return MockCursor([])

        mock_collection.find.side_effect = find_side_effect

        mock_agg_result = MagicMock()
        mock_agg_result.next.return_value = {"_id": doc1_id}
        mock_collection.aggregate.return_value = mock_agg_result

        mock_session = MagicMock()
        mock_client.start_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_client.start_session.return_value.__exit__ = MagicMock(return_value=False)

        with patch("init_sync.schema_utils") as mock_schema:
            mock_schema.process_dataframe = MagicMock()
            mock_schema.finalize_dataframe_for_parquet = MagicMock()
            init_sync("testcoll")

        sleep_calls = [c for c in mock_sleep.call_args_list if c.args[0] == 5]
        assert len(sleep_calls) >= 1, f"Expected sleep(5) but got: {mock_sleep.call_args_list}"


# ─────────────────────────────────────────────
# Fix 6: Init sync retry + always start listener
# ─────────────────────────────────────────────
class TestInitSyncRetry:
    @patch("mongodb_generic_mirroring.Thread")
    @patch("mongodb_generic_mirroring.init_sync")
    @patch("mongodb_generic_mirroring.init_table_schema")
    @patch("mongodb_generic_mirroring.push_file_to_lz")
    @patch("mongodb_generic_mirroring.read_from_file")
    @patch("mongodb_generic_mirroring.__get_all_collections")
    @patch("mongodb_generic_mirroring.__ensure_partner_events_in_lz")
    @patch("mongodb_generic_mirroring.Event")
    def test_listener_starts_even_if_init_sync_fails(
        self, mock_event, mock_partner, mock_get_colls, mock_read,
        mock_push, mock_schema, mock_init_sync, mock_thread, env_vars
    ):
        """Even when init_sync raises, a Thread should be started for listening."""
        mock_get_colls.return_value = ["testcoll"]
        mock_read.return_value = None
        mock_init_sync.side_effect = Exception("OneLake connection refused")

        # Make Event().wait() return immediately so mirror() doesn't block
        mock_event_instance = MagicMock()
        mock_event.return_value = mock_event_instance

        mirror()

        # Thread should have been created for the listener
        assert mock_thread.called, "Thread should be started even if init_sync fails"
        # Verify the thread target is `listening`
        thread_calls = mock_thread.call_args_list
        listening_threads = [c for c in thread_calls if c.kwargs.get("target").__name__ == "listening"]
        assert len(listening_threads) == 1

    @patch("mongodb_generic_mirroring.Thread")
    @patch("mongodb_generic_mirroring.init_sync")
    @patch("mongodb_generic_mirroring.init_table_schema")
    @patch("mongodb_generic_mirroring.push_file_to_lz")
    @patch("mongodb_generic_mirroring.read_from_file")
    @patch("mongodb_generic_mirroring.__get_all_collections")
    @patch("mongodb_generic_mirroring.__ensure_partner_events_in_lz")
    @patch("mongodb_generic_mirroring.Event")
    @patch("time.sleep")
    def test_init_sync_retries_on_failure(
        self, mock_sleep, mock_event, mock_partner, mock_get_colls,
        mock_read, mock_push, mock_schema, mock_init_sync, mock_thread, env_vars
    ):
        """init_sync should be retried up to MAX_INIT_SYNC_RETRIES times."""
        mock_get_colls.return_value = ["testcoll"]
        mock_read.return_value = None
        mock_init_sync.side_effect = [
            Exception("fail 1"),
            Exception("fail 2"),
            None,  # third attempt succeeds
        ]

        mock_event_instance = MagicMock()
        mock_event.return_value = mock_event_instance

        mirror()

        # init_sync should have been called 3 times
        assert mock_init_sync.call_count == 3

    @patch("mongodb_generic_mirroring.Thread")
    @patch("mongodb_generic_mirroring.init_sync")
    @patch("mongodb_generic_mirroring.init_table_schema")
    @patch("mongodb_generic_mirroring.push_file_to_lz")
    @patch("mongodb_generic_mirroring.read_from_file")
    @patch("mongodb_generic_mirroring.__get_all_collections")
    @patch("mongodb_generic_mirroring.__ensure_partner_events_in_lz")
    @patch("mongodb_generic_mirroring.Event")
    def test_no_retry_on_success(
        self, mock_event, mock_partner, mock_get_colls,
        mock_read, mock_push, mock_schema, mock_init_sync, mock_thread, env_vars
    ):
        """If init_sync succeeds on first try, no retries."""
        mock_get_colls.return_value = ["testcoll"]
        mock_read.return_value = None
        mock_init_sync.return_value = None  # success

        mock_event_instance = MagicMock()
        mock_event.return_value = mock_event_instance

        mirror()

        assert mock_init_sync.call_count == 1
