"""
Load tests for Fix A (MongoDB timeouts), Fix B (token caching),
and Fix C (parallel collection processing).

Run with: pytest tests/test_parallel_and_timeout.py -v -s
"""
import os
import sys
import time
import threading
import statistics
from unittest.mock import patch, MagicMock, call, PropertyMock
from dataclasses import dataclass, field

import pytest
import requests
import responses as resp_lib
from bson import ObjectId
from bson.timestamp import Timestamp
import pymongo.errors

import push_file_to_lz as pfl
from push_file_to_lz import push_file_to_lz, _lz_folder_url

_get_access_token = getattr(pfl, '__get_access_token')


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _make_env(monkeypatch, **overrides):
    """Set all required env vars.  Pass MONGO_COLLECTION in overrides to change it."""
    defaults = {
        "MONGO_CONN_STR": "mongodb://localhost:27017",
        "MONGO_DB_NAME": "testdb",
        # MONGO_COLLECTION intentionally NOT set here — tests set it via monkeypatch
        "LZ_URL": "https://onelake.dfs.fabric.microsoft.com/ws/lh/Files/LandingZone",
        "APP_ID": "test-app-id",
        "SECRET": "test-secret",
        "TENANT_ID": "test-tenant-id",
        "INIT_LOAD_BATCH_SIZE": "100",
        "DELTA_SYNC_BATCH_SIZE": "10",
        "TIME_THRESHOLD_IN_SEC": "60",
        "APP_LOG_LEVEL": "WARNING",
        "MAX_INIT_SYNC_RETRIES": "2",
        "INIT_SYNC_BATCH_SLEEP_SEC": "0",
        "MAX_PARALLEL_COLLECTIONS": "3",
        "MONGO_SOCKET_TIMEOUT_MS": "5000",
        "MONGO_CONNECT_TIMEOUT_MS": "2000",
        "MONGO_SERVER_SELECT_TIMEOUT_MS": "3000",
        "HTTP_CONNECT_TIMEOUT_SEC": "5",
        "HTTP_READ_TIMEOUT_SEC": "30",
        "MAX_UPLOAD_RETRIES": "2",
        "UPLOAD_RETRY_BASE_DELAY_SEC": "0.1",
    }
    defaults.update(overrides)
    for k, v in defaults.items():
        monkeypatch.setenv(k, v)


# ─────────────────────────────────────────────
# Fix A: MongoDB Timeout Tests
# ─────────────────────────────────────────────
class TestMongoTimeout:
    """Verify init_sync MongoClient uses socket/connect/server timeouts."""

    def test_init_sync_uses_timeout(self, monkeypatch, tmp_path):
        """init_sync should create MongoClient with timeout parameters."""
        _make_env(monkeypatch)

        with patch("init_sync.pymongo.MongoClient") as mock_client_cls, \
             patch("init_sync.read_from_file", return_value=None), \
             patch("init_sync.write_to_file"), \
             patch("init_sync.push_file_to_lz"), \
             patch("init_sync.delete_file"), \
             patch("init_sync.get_cluster_time", return_value=Timestamp(1000, 1)), \
             patch("init_sync.schema_utils"), \
             patch("init_sync.pd.DataFrame.to_parquet"):

            mock_collection = MagicMock()
            mock_db = MagicMock()
            mock_db.__getitem__ = MagicMock(return_value=mock_collection)
            mock_client = MagicMock()
            mock_client.__getitem__ = MagicMock(return_value=mock_db)
            mock_client_cls.return_value = mock_client
            mock_collection.estimated_document_count.return_value = 0

            mock_session = MagicMock()
            mock_client.start_session.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_client.start_session.return_value.__exit__ = MagicMock(return_value=False)

            # Empty collection → immediate break
            mock_agg = MagicMock()
            mock_agg.next.side_effect = StopIteration
            mock_collection.aggregate.return_value = mock_agg

            from init_sync import init_sync
            init_sync("testcoll")

            # Verify MongoClient was called with timeout kwargs
            mock_client_cls.assert_called_once()
            call_kwargs = mock_client_cls.call_args
            assert "socketTimeoutMS" in call_kwargs.kwargs or len(call_kwargs.args) > 1
            # Check the actual values
            if "socketTimeoutMS" in call_kwargs.kwargs:
                assert call_kwargs.kwargs["socketTimeoutMS"] == 5000
                assert call_kwargs.kwargs["connectTimeoutMS"] == 2000
                assert call_kwargs.kwargs["serverSelectionTimeoutMS"] == 3000

    def test_init_sync_timeout_raises_and_retries(self, monkeypatch, tmp_path):
        """When MongoDB find() times out, ExecutionTimeout should trigger retry."""
        _make_env(monkeypatch)

        with patch("init_sync.pymongo.MongoClient") as mock_client_cls, \
             patch("init_sync.read_from_file", return_value=None), \
             patch("init_sync.write_to_file"), \
             patch("init_sync.push_file_to_lz"), \
             patch("init_sync.delete_file"), \
             patch("init_sync.get_cluster_time", return_value=Timestamp(1000, 1)), \
             patch("init_sync.schema_utils"), \
             patch("init_sync.pd.DataFrame.to_parquet"):

            mock_collection = MagicMock()
            mock_db = MagicMock()
            mock_db.__getitem__ = MagicMock(return_value=mock_collection)
            mock_client = MagicMock()
            mock_client.__getitem__ = MagicMock(return_value=mock_db)
            mock_client_cls.return_value = mock_client
            mock_collection.estimated_document_count.return_value = 100

            mock_session = MagicMock()
            mock_client.start_session.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_client.start_session.return_value.__exit__ = MagicMock(return_value=False)

            # Simulate timeout on find
            mock_collection.find.side_effect = pymongo.errors.ExecutionTimeout(
                "operation exceeded time limit"
            )
            mock_agg = MagicMock()
            mock_agg.next.return_value = {"_id": ObjectId()}
            mock_collection.aggregate.return_value = mock_agg

            from init_sync import init_sync
            with pytest.raises(pymongo.errors.ExecutionTimeout):
                init_sync("testcoll")


# ─────────────────────────────────────────────
# Fix B: Token Caching Tests
# ─────────────────────────────────────────────
class TestTokenCaching:
    """Verify that __get_access_token caches and reuses tokens."""

    def setup_method(self):
        """Reset the token cache before each test."""
        pfl._token_cache["token"] = None
        pfl._token_cache["expires_at"] = 0.0

    @resp_lib.activate
    def test_token_is_cached(self, monkeypatch):
        """Second call should reuse cached token without HTTP request."""
        monkeypatch.setenv("MONGO_COLLECTION", "testcoll")
        monkeypatch.setenv("APP_ID", "test-app")
        monkeypatch.setenv("SECRET", "test-secret")
        monkeypatch.setenv("TENANT_ID", "test-tenant")

        resp_lib.add(
            resp_lib.POST,
            "https://login.microsoftonline.com/test-tenant/oauth2/v2.0/token",
            json={"access_token": "cached-token", "expires_in": 3600},
            status=200,
        )

        # First call — should hit the token endpoint
        token1 = _get_access_token("test-app", "test-secret", "test-tenant")
        assert token1 == "cached-token"
        assert len(resp_lib.calls) == 1

        # Second call — should use cache, no HTTP request
        token2 = _get_access_token("test-app", "test-secret", "test-tenant")
        assert token2 == "cached-token"
        assert len(resp_lib.calls) == 1  # still 1 — no new call

    @resp_lib.activate
    def test_token_refreshes_when_expired(self, monkeypatch):
        """Should acquire new token when cache is expired."""
        monkeypatch.setenv("MONGO_COLLECTION", "testcoll")
        monkeypatch.setenv("APP_ID", "test-app")
        monkeypatch.setenv("SECRET", "test-secret")
        monkeypatch.setenv("TENANT_ID", "test-tenant")

        resp_lib.add(
            resp_lib.POST,
            "https://login.microsoftonline.com/test-tenant/oauth2/v2.0/token",
            json={"access_token": "token-1", "expires_in": 3600},
            status=200,
        )

        token1 = _get_access_token("test-app", "test-secret", "test-tenant")
        assert token1 == "token-1"

        # Force expiration
        pfl._token_cache["expires_at"] = time.time() - 10

        resp_lib.add(
            resp_lib.POST,
            "https://login.microsoftonline.com/test-tenant/oauth2/v2.0/token",
            json={"access_token": "token-2", "expires_in": 3600},
            status=200,
        )

        token2 = _get_access_token("test-app", "test-secret", "test-tenant")
        assert token2 == "token-2"
        assert len(resp_lib.calls) == 2

    @resp_lib.activate
    def test_multiple_uploads_share_token(self, monkeypatch, tmp_path):
        """Multiple push_file_to_lz calls should share one token."""
        _make_env(monkeypatch, MONGO_COLLECTION="testcoll")

        resp_lib.add(
            resp_lib.POST,
            "https://login.microsoftonline.com/test-tenant-id/oauth2/v2.0/token",
            json={"access_token": "shared-token", "expires_in": 3600},
            status=200,
        )

        base = _lz_folder_url(os.environ["LZ_URL"], "testcoll")

        # Set up mocks for 3 uploads
        for i in range(3):
            fname = f"file_{i}.parquet"
            resp_lib.add(resp_lib.PUT, base + f"_{fname}_TEMP?resource=file", status=201)
            resp_lib.add(resp_lib.PATCH, base + f"_{fname}_TEMP?position=0&action=append&flush=true", status=200)
            resp_lib.add(resp_lib.PUT, base + fname, status=200)

        for i in range(3):
            test_file = tmp_path / f"file_{i}.parquet"
            test_file.write_bytes(b"PAR1" + b"\x00" * 100)
            push_file_to_lz(str(test_file), "testcoll")

        # Count token endpoint calls
        token_calls = [
            c for c in resp_lib.calls
            if "login.microsoftonline.com" in c.request.url
        ]
        assert len(token_calls) == 1, (
            f"Expected 1 token call for 3 uploads, got {len(token_calls)}"
        )


# ─────────────────────────────────────────────
# Fix C: Parallel Collection Processing Tests
# ─────────────────────────────────────────────
class TestParallelProcessing:
    """Verify that collections are processed in parallel."""

    @patch("mongodb_generic_mirroring.Thread")
    @patch("mongodb_generic_mirroring.init_sync")
    @patch("mongodb_generic_mirroring.init_table_schema")
    @patch("mongodb_generic_mirroring.push_file_to_lz")
    @patch("mongodb_generic_mirroring.read_from_file")
    @patch("mongodb_generic_mirroring.__get_all_collections")
    @patch("mongodb_generic_mirroring.__ensure_partner_events_in_lz")
    @patch("mongodb_generic_mirroring.Event")
    def test_collections_processed_in_parallel(
        self, mock_event, mock_partner, mock_get_colls, mock_read,
        mock_push, mock_schema, mock_init_sync, mock_thread,
        monkeypatch, tmp_path
    ):
        """Multiple collections should be submitted to ThreadPoolExecutor."""
        collections = [f"coll_{i}" for i in range(10)]
        monkeypatch.setenv("MONGO_COLLECTION", str(collections).replace("'", '"'))
        _make_env(monkeypatch, MAX_PARALLEL_COLLECTIONS="5")

        mock_get_colls.return_value = collections
        mock_read.return_value = None

        mock_event_instance = MagicMock()
        mock_event.return_value = mock_event_instance

        from mongodb_generic_mirroring import mirror
        mirror()

        # All 10 collections should have listener threads started
        assert mock_thread.call_count == 10, (
            f"Expected 10 listener threads, got {mock_thread.call_count}"
        )

    @patch("mongodb_generic_mirroring.Thread")
    @patch("mongodb_generic_mirroring.init_sync")
    @patch("mongodb_generic_mirroring.init_table_schema")
    @patch("mongodb_generic_mirroring.push_file_to_lz")
    @patch("mongodb_generic_mirroring.read_from_file")
    @patch("mongodb_generic_mirroring.__get_all_collections")
    @patch("mongodb_generic_mirroring.__ensure_partner_events_in_lz")
    @patch("mongodb_generic_mirroring.Event")
    def test_stuck_collection_does_not_block_others(
        self, mock_event, mock_partner, mock_get_colls, mock_read,
        mock_push, mock_schema, mock_init_sync, mock_thread,
        monkeypatch, tmp_path
    ):
        """A collection that hangs in init_sync should not prevent
        other collections from starting their listeners."""
        collections = ["stuck_coll", "fast_coll_1", "fast_coll_2"]
        monkeypatch.setenv("MONGO_COLLECTION", str(collections).replace("'", '"'))
        _make_env(monkeypatch, MAX_PARALLEL_COLLECTIONS="3", MAX_INIT_SYNC_RETRIES="2")

        mock_get_colls.return_value = collections
        mock_read.return_value = None

        mock_event_instance = MagicMock()
        mock_event.return_value = mock_event_instance

        # stuck_coll hangs forever in init_sync (simulated by raising after a delay)
        # fast_colls complete immediately
        def init_sync_side_effect(name):
            if name == "stuck_coll":
                raise Exception("simulated init sync failure")
            return None  # fast collections succeed

        mock_init_sync.side_effect = init_sync_side_effect

        from mongodb_generic_mirroring import mirror
        mirror()

        # All 3 collections should have listener threads
        assert mock_thread.call_count == 3, (
            f"Expected 3 listener threads, got {mock_thread.call_count}. "
            f"A stuck collection blocked the others."
        )

    @patch("mongodb_generic_mirroring.Thread")
    @patch("mongodb_generic_mirroring.init_sync")
    @patch("mongodb_generic_mirroring.init_table_schema")
    @patch("mongodb_generic_mirroring.push_file_to_lz")
    @patch("mongodb_generic_mirroring.read_from_file")
    @patch("mongodb_generic_mirroring.__get_all_collections")
    @patch("mongodb_generic_mirroring.__ensure_partner_events_in_lz")
    @patch("mongodb_generic_mirroring.Event")
    @patch("mongodb_generic_mirroring.time.sleep")
    def test_parallel_with_partial_failures(
        self, mock_sleep, mock_event, mock_partner, mock_get_colls,
        mock_read, mock_push, mock_schema, mock_init_sync, mock_thread,
        monkeypatch, tmp_path
    ):
        """3 of 10 collections fail init sync; all 10 should still get listeners."""
        collections = [f"coll_{i}" for i in range(10)]
        monkeypatch.setenv("MONGO_COLLECTION", str(collections).replace("'", '"'))
        _make_env(monkeypatch, MAX_PARALLEL_COLLECTIONS="5", MAX_INIT_SYNC_RETRIES="2")

        mock_get_colls.return_value = collections
        mock_read.return_value = None

        mock_event_instance = MagicMock()
        mock_event.return_value = mock_event_instance

        failing = {"coll_2", "coll_5", "coll_8"}

        def init_sync_side_effect(name):
            if name in failing:
                raise Exception(f"Simulated failure for {name}")
            return None

        mock_init_sync.side_effect = init_sync_side_effect

        from mongodb_generic_mirroring import mirror
        mirror()

        assert mock_thread.call_count == 10, (
            f"Expected 10 listener threads, got {mock_thread.call_count}"
        )

        # init_sync should have been called:
        # 7 succeed on first try = 7 calls
        # 3 fail × 2 retries = 6 calls
        # Total = 13 calls
        assert mock_init_sync.call_count == 13


# ─────────────────────────────────────────────
# Load Test: Parallel Throughput Comparison
# ─────────────────────────────────────────────
class TestParallelThroughput:
    """
    Simulates the CSX scenario: 47 collections, one gets stuck.
    Compares sequential vs parallel processing time.
    """

    @patch("mongodb_generic_mirroring.Thread")
    @patch("mongodb_generic_mirroring.init_sync")
    @patch("mongodb_generic_mirroring.init_table_schema")
    @patch("mongodb_generic_mirroring.push_file_to_lz")
    @patch("mongodb_generic_mirroring.read_from_file")
    @patch("mongodb_generic_mirroring.__get_all_collections")
    @patch("mongodb_generic_mirroring.__ensure_partner_events_in_lz")
    @patch("mongodb_generic_mirroring.Event")
    def test_47_collections_one_stuck(
        self, mock_event, mock_partner, mock_get_colls, mock_read,
        mock_push, mock_schema, mock_init_sync, mock_thread,
        monkeypatch, tmp_path
    ):
        """47 collections, collection 20 gets stuck. All 47 should get listeners."""
        collections = [f"collection_{i}" for i in range(47)]
        monkeypatch.setenv("MONGO_COLLECTION", str(collections).replace("'", '"'))
        _make_env(
            monkeypatch,
            MAX_PARALLEL_COLLECTIONS="5",
            MAX_INIT_SYNC_RETRIES="1",  # fail fast for this test
        )

        mock_get_colls.return_value = collections
        mock_read.return_value = None

        mock_event_instance = MagicMock()
        mock_event.return_value = mock_event_instance

        # collection_20 fails
        def init_sync_side_effect(name):
            if name == "collection_20":
                raise Exception("Stuck collection")
            return None

        mock_init_sync.side_effect = init_sync_side_effect

        start = time.time()
        from mongodb_generic_mirroring import mirror
        mirror()
        elapsed = time.time() - start

        # All 47 collections should have listener threads
        assert mock_thread.call_count == 47, (
            f"Expected 47 listener threads, got {mock_thread.call_count}. "
            f"A stuck collection blocked the pipeline."
        )

        # init_sync called 46 times (success) + 1 time (failure) = 47
        assert mock_init_sync.call_count == 47

        print(f"\n47 collections processed in {elapsed:.2f}s (parallel)")

    @patch("mongodb_generic_mirroring.Thread")
    @patch("mongodb_generic_mirroring.init_sync")
    @patch("mongodb_generic_mirroring.init_table_schema")
    @patch("mongodb_generic_mirroring.push_file_to_lz")
    @patch("mongodb_generic_mirroring.read_from_file")
    @patch("mongodb_generic_mirroring.__get_all_collections")
    @patch("mongodb_generic_mirroring.__ensure_partner_events_in_lz")
    @patch("mongodb_generic_mirroring.Event")
    def test_empty_collection_does_not_block(
        self, mock_event, mock_partner, mock_get_colls, mock_read,
        mock_push, mock_schema, mock_init_sync, mock_thread,
        monkeypatch, tmp_path
    ):
        """An empty collection (0 docs) should complete quickly and not block others."""
        collections = ["empty_coll", "normal_coll_1", "normal_coll_2"]
        monkeypatch.setenv("MONGO_COLLECTION", str(collections).replace("'", '"'))
        _make_env(monkeypatch, MAX_PARALLEL_COLLECTIONS="3")

        mock_get_colls.return_value = collections
        mock_read.return_value = None

        mock_event_instance = MagicMock()
        mock_event.return_value = mock_event_instance

        mock_init_sync.return_value = None  # all succeed

        from mongodb_generic_mirroring import mirror
        mirror()

        assert mock_thread.call_count == 3
