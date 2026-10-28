"""
Load test harness for MongoDB → Fabric mirroring.
Tests the fixes under realistic high-volume conditions with simulated failures.

Usage:
    # Run all load tests
    pytest tests/test_load.py -v -k load --timeout=300

    # Run a specific scenario
    pytest tests/test_load.py::TestLoadOnelake::test_upload_with_intermittent_failures -v

    # Run with verbose output and timing
    pytest tests/test_load.py -v -s --durations=10
"""
import os
import sys
import time
import random
import threading
import statistics
import tempfile
import pickle
from unittest.mock import patch, MagicMock, PropertyMock
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import pytest
import pandas as pd
import requests
import responses as resp_lib
from bson import ObjectId
from bson.timestamp import Timestamp
import pymongo.errors


# ─────────────────────────────────────────────
# Metrics collector
# ─────────────────────────────────────────────
@dataclass
class LoadTestMetrics:
    """Collects metrics during load tests."""
    total_uploads: int = 0
    successful_uploads: int = 0
    failed_uploads: int = 0
    retried_uploads: int = 0
    temp_files_created: int = 0
    temp_files_cleaned: int = 0
    temp_files_orphaned: int = 0
    history_loss_events: int = 0
    circuit_breaker_fires: int = 0
    total_latency_ms: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    start_time: float = 0
    end_time: float = 0

    @property
    def duration_sec(self) -> float:
        return self.end_time - self.start_time

    @property
    def throughput_per_sec(self) -> float:
        if self.duration_sec == 0:
            return 0
        return self.successful_uploads / self.duration_sec

    @property
    def avg_latency_ms(self) -> float:
        if not self.total_latency_ms:
            return 0
        return statistics.mean(self.total_latency_ms)

    @property
    def p99_latency_ms(self) -> float:
        if not self.total_latency_ms:
            return 0
        sorted_lat = sorted(self.total_latency_ms)
        idx = int(len(sorted_lat) * 0.99)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]

    def summary(self) -> str:
        return (
            f"\n{'='*60}\n"
            f"LOAD TEST RESULTS\n"
            f"{'='*60}\n"
            f"Duration:              {self.duration_sec:.2f}s\n"
            f"Total uploads:         {self.total_uploads}\n"
            f"Successful:            {self.successful_uploads}\n"
            f"Failed:                {self.failed_uploads}\n"
            f"Retried:               {self.retried_uploads}\n"
            f"TEMP files created:    {self.temp_files_created}\n"
            f"TEMP files cleaned:    {self.temp_files_cleaned}\n"
            f"TEMP files orphaned:   {self.temp_files_orphaned}\n"
            f"History loss events:   {self.history_loss_events}\n"
            f"Circuit breaker fires: {self.circuit_breaker_fires}\n"
            f"Throughput:            {self.throughput_per_sec:.1f} uploads/sec\n"
            f"Avg latency:           {self.avg_latency_ms:.1f}ms\n"
            f"P99 latency:           {self.p99_latency_ms:.1f}ms\n"
            f"Errors:                {len(self.errors)}\n"
            f"{'='*60}"
        )


# ─────────────────────────────────────────────
# Simulated OneLake with configurable failure modes
# ─────────────────────────────────────────────
class SimulatedOneLake:
    """
    Simulates OneLake behavior with configurable failure rates.
    Tracks TEMP file lifecycle to verify cleanup.
    """

    def __init__(
        self,
        failure_rate: float = 0.0,
        latency_ms: tuple = (10, 100),
        failure_modes: list = None,
    ):
        self.failure_rate = failure_rate
        self.latency_ms = latency_ms
        self.failure_modes = failure_modes or ["connection_refused"]
        self._lock = threading.Lock()
        self._temp_files: dict[str, str] = {}  # temp_name → status (created/appended/renamed)
        self._request_count = 0
        self._failures_injected = 0

    @property
    def request_count(self):
        with self._lock:
            return self._request_count

    @property
    def failures_injected(self):
        with self._lock:
            return self._failures_injected

    @property
    def orphaned_temp_files(self) -> list[str]:
        with self._lock:
            return [
                name for name, status in self._temp_files.items()
                if status in ("created", "appended")
            ]

    def _should_fail(self) -> bool:
        return random.random() < self.failure_rate

    def _get_failure(self):
        mode = random.choice(self.failure_modes)
        if mode == "connection_refused":
            return requests.exceptions.ConnectionError(
                "Connection refused by onelake.dfs.fabric.microsoft.com"
            )
        elif mode == "timeout":
            return requests.exceptions.ReadTimeout("Read timed out")
        elif mode == "server_error":
            resp = MagicMock()
            resp.status_code = 500
            resp.text = "Internal Server Error"
            return requests.exceptions.HTTPError("500 Server Error", response=resp)
        return requests.exceptions.ConnectionError("generic failure")

    def _simulate_latency(self):
        lo, hi = self.latency_ms
        time.sleep(random.uniform(lo, hi) / 1000.0)

    def handle_create_temp(self, url: str):
        """Simulate PUT ?resource=file"""
        with self._lock:
            self._request_count += 1
        self._simulate_latency()
        if self._should_fail():
            with self._lock:
                self._failures_injected += 1
            raise self._get_failure()
        # Extract temp filename from URL
        temp_name = url.split("/")[-1].split("?")[0]
        with self._lock:
            self._temp_files[temp_name] = "created"
        resp = MagicMock()
        resp.status_code = 201
        return resp

    def handle_append(self, url: str, data: bytes):
        """Simulate PATCH ?action=append"""
        with self._lock:
            self._request_count += 1
        self._simulate_latency()
        if self._should_fail():
            with self._lock:
                self._failures_injected += 1
            raise self._get_failure()
        temp_name = url.split("/")[-1].split("?")[0]
        with self._lock:
            if temp_name in self._temp_files:
                self._temp_files[temp_name] = "appended"
        resp = MagicMock()
        resp.status_code = 200
        return resp

    def handle_rename(self, url: str, source_url: str):
        """Simulate PUT (rename)"""
        with self._lock:
            self._request_count += 1
        self._simulate_latency()
        if self._should_fail():
            with self._lock:
                self._failures_injected += 1
            raise self._get_failure()
        source_name = source_url.split("/")[-1].split("?")[0]
        with self._lock:
            if source_name in self._temp_files:
                del self._temp_files[source_name]
        resp = MagicMock()
        resp.status_code = 200
        return resp

    def handle_delete(self, url: str):
        """Simulate DELETE"""
        with self._lock:
            self._request_count += 1
        self._simulate_latency()
        name = url.split("/")[-1]
        with self._lock:
            if name in self._temp_files:
                del self._temp_files[name]
        resp = MagicMock()
        resp.status_code = 200
        return resp

    def handle_get(self, url: str):
        """Simulate GET"""
        with self._lock:
            self._request_count += 1
        self._simulate_latency()
        resp = MagicMock()
        resp.status_code = 404
        return resp


# ─────────────────────────────────────────────
# Load Test: OneLake Upload Resilience
# ─────────────────────────────────────────────
class TestLoadOnelake:
    """Load tests for push_file_to_lz under various failure conditions."""

    def _run_upload_load_test(
        self,
        env_vars,
        num_files: int = 100,
        failure_rate: float = 0.0,
        max_workers: int = 1,
        metrics: LoadTestMetrics = None,
    ) -> LoadTestMetrics:
        """Run a load test pushing files to a simulated OneLake.
        Uses max_workers=1 (sequential) to avoid thread-safety issues
        with module-level patching.
        """
        if metrics is None:
            metrics = LoadTestMetrics()

        onelake = SimulatedOneLake(failure_rate=failure_rate, latency_ms=(5, 50))
        metrics.start_time = time.time()

        import push_file_to_lz as pfl

        # Save originals once
        orig_put = pfl.requests.put
        orig_patch = pfl.requests.patch
        orig_delete = pfl.requests.delete
        orig_post = pfl.requests.post
        orig_get = pfl.requests.get

        token_resp = MagicMock()
        token_resp.text = '{"access_token": "fake"}'
        token_resp.status_code = 200
        token_resp.json.return_value = {"access_token": "fake"}
        pfl.requests.post = lambda *a, **kw: token_resp

        def put_se(url, **kwargs):
            headers = kwargs.get("headers", {})
            if "?resource=file" in url:
                resp = onelake.handle_create_temp(url)
                m = MagicMock(); m.status_code = resp.status_code; m.raise_for_status = MagicMock()
                return m
            elif "x-ms-rename-source" in headers:
                resp = onelake.handle_rename(url, headers["x-ms-rename-source"])
                m = MagicMock(); m.status_code = resp.status_code; m.raise_for_status = MagicMock()
                return m
            m = MagicMock(); m.status_code = 200; m.raise_for_status = MagicMock()
            return m

        def patch_se(url, **kwargs):
            resp = onelake.handle_append(url, kwargs.get("data", b""))
            m = MagicMock(); m.status_code = resp.status_code; m.raise_for_status = MagicMock()
            return m

        def delete_se(url, **kwargs):
            resp = onelake.handle_delete(url)
            m = MagicMock(); m.status_code = resp.status_code; m.raise_for_status = MagicMock()
            return m

        def get_se(url, **kwargs):
            return onelake.handle_get(url)

        pfl.requests.put = put_se
        pfl.requests.patch = patch_se
        pfl.requests.delete = delete_se
        pfl.requests.get = get_se

        try:
            def do_upload(file_idx: int):
                filepath = os.path.join(
                    str(env_vars), f"test_file_{file_idx}.parquet"
                )
                with open(filepath, "wb") as f:
                    f.write(b"PAR1" + os.urandom(1024))

                start = time.time()
                try:
                    pfl.push_file_to_lz(filepath, "testcoll")
                    latency = (time.time() - start) * 1000
                    metrics.total_latency_ms.append(latency)
                    metrics.total_uploads += 1
                    metrics.successful_uploads += 1
                except Exception as e:
                    metrics.total_uploads += 1
                    metrics.failed_uploads += 1
                    metrics.errors.append(str(e))
                finally:
                    if os.path.exists(filepath):
                        os.remove(filepath)

            # Run uploads sequentially (thread-safety for module patching)
            for i in range(num_files):
                do_upload(i)
        finally:
            # Restore originals
            pfl.requests.put = orig_put
            pfl.requests.patch = orig_patch
            pfl.requests.delete = orig_delete
            pfl.requests.post = orig_post
            pfl.requests.get = orig_get

        metrics.end_time = time.time()
        metrics.temp_files_created = onelake.request_count
        metrics.temp_files_orphaned = len(onelake.orphaned_temp_files)
        return metrics

    @pytest.mark.timeout(120)
    def test_upload_with_no_failures(self, env_vars):
        """Baseline: 50 files, 0% failure rate."""
        metrics = self._run_upload_load_test(
            env_vars, num_files=50, failure_rate=0.0, max_workers=4
        )
        print(metrics.summary())

        assert metrics.successful_uploads == 50
        assert metrics.failed_uploads == 0
        assert metrics.temp_files_orphaned == 0, "No TEMP files should be orphaned"

    def test_upload_with_intermittent_failures(self, env_vars):
        """50 files, 20% failure rate per HTTP call — retries should handle most.
        
        With 3 HTTP calls per upload and 20% per-call failure:
        P(single attempt fails) = 1 - 0.8^3 = 48.8%
        P(all 3 retries fail) = 0.488^3 = 11.6%
        Expected success rate ≈ 88%
        """
        metrics = self._run_upload_load_test(
            env_vars, num_files=50, failure_rate=0.2
        )
        print(metrics.summary())

        success_rate = metrics.successful_uploads / metrics.total_uploads
        assert success_rate > 0.75, (
            f"Success rate {success_rate:.2%} is too low. "
            f"Expected >75% with retries (3 HTTP calls per upload, 20% per-call failure)."
        )
        assert metrics.temp_files_orphaned == 0, (
            f"Found {metrics.temp_files_orphaned} orphaned TEMP files"
        )

    def test_upload_with_high_failure_rate(self, env_vars):
        """50 files, 50% failure rate per HTTP call — some will fail but TEMP files should be cleaned.
        
        With 3 HTTP calls per upload and 50% per-call failure:
        P(single attempt fails) = 1 - 0.5^3 = 87.5%
        P(all 3 retries fail) = 0.875^3 = 67%
        Expected success rate ≈ 33%
        """
        metrics = self._run_upload_load_test(
            env_vars, num_files=50, failure_rate=0.5
        )
        print(metrics.summary())

        # With 50% per-call failure and 3 retries: expected ~33% success
        success_rate = metrics.successful_uploads / metrics.total_uploads
        assert success_rate > 0.20, f"Success rate {success_rate:.2%} too low"

        # TEMP file cleanup should prevent orphans even on failures
        assert metrics.temp_files_orphaned == 0, (
            f"Found {metrics.temp_files_orphaned} orphaned TEMP files. "
            f"Cleanup is not working."
        )

    @pytest.mark.timeout(120)
    def test_upload_with_connection_refused_only(self, env_vars):
        """Test specifically with connection refused errors (the CSX issue)."""
        metrics = LoadTestMetrics()
        onelake = SimulatedOneLake(
            failure_rate=0.3,
            latency_ms=(5, 30),
            failure_modes=["connection_refused"],
        )

        # Same as above but only connection_refused
        metrics = self._run_upload_load_test(
            env_vars, num_files=30, failure_rate=0.3, max_workers=2, metrics=metrics
        )
        print(metrics.summary())

        assert metrics.temp_files_orphaned == 0


# ─────────────────────────────────────────────
# Load Test: Change Stream Resilience
# ─────────────────────────────────────────────
class TestLoadChangeStream:
    """Load tests for change stream listener under failure conditions."""

    @patch("listening.write_to_file")
    @patch("listening.read_from_file")
    @patch("listening.pymongo.MongoClient")
    @patch("listening.time.sleep")
    def test_repeated_history_loss_triggers_circuit_breaker(
        self, mock_sleep, mock_client_cls, mock_read, mock_write, env_vars
    ):
        """Simulate 10 consecutive ChangeStreamHistoryLost events.
        The circuit breaker should fire after 3 and stop the listener.
        """
        mock_read.side_effect = lambda *args, **kwargs: None

        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        watch_call_count = [0]

        def watch_side_effect(**kwargs):
            watch_call_count[0] += 1
            raise pymongo.errors.OperationFailure(
                "ChangeStreamHistoryLost", code=286
            )

        mock_collection.watch.side_effect = watch_side_effect

        from listening import listening
        listening("testcoll")

        # Should have stopped after 3 attempts (MAX_CONSECUTIVE_HISTORY_LOSS)
        assert watch_call_count[0] == 3, (
            f"Expected 3 watch calls (circuit breaker), got {watch_call_count[0]}"
        )

        # Should have written _NEEDS_REINIT sentinel
        sentinel_calls = [
            c for c in mock_write.call_args_list
            if len(c.args) >= 3 and "_NEEDS_REINIT" in str(c.args[2])
        ]
        assert len(sentinel_calls) == 1

    @patch("listening.write_to_file")
    @patch("listening.read_from_file")
    @patch("listening.pymongo.MongoClient")
    @patch("listening.time.sleep")
    def test_mixed_errors_do_not_cascade(
        self, mock_sleep, mock_client_cls, mock_read, mock_write, env_vars
    ):
        """Alternating resumable and non-resumable errors.
        Circuit breaker should NOT fire until 3 CONSECUTIVE history losses.
        Pattern: H R H R H H H → circuit breaker fires at call 7.
        """
        mock_read.side_effect = lambda *args, **kwargs: None

        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        # Pattern: 1=history, 2=resumable(reset), 3=history, 4=resumable(reset),
        #          5=history, 6=history, 7=history → circuit breaker at 7
        errors = [
            pymongo.errors.OperationFailure("history lost", code=286),       # 1: H (count=1)
            pymongo.errors.ConnectionFailure("connection reset"),             # 2: R (reset to 0)
            pymongo.errors.OperationFailure("history lost", code=286),       # 3: H (count=1)
            pymongo.errors.ConnectionFailure("connection reset"),             # 4: R (reset to 0)
            pymongo.errors.OperationFailure("history lost", code=286),       # 5: H (count=1)
            pymongo.errors.OperationFailure("history lost", code=286),       # 6: H (count=2)
            pymongo.errors.OperationFailure("history lost", code=286),       # 7: H (count=3) → BREAK
        ]
        call_count = [0]

        def watch_side_effect(**kwargs):
            idx = call_count[0]
            call_count[0] += 1
            if idx < len(errors):
                raise errors[idx]
            raise pymongo.errors.OperationFailure("extra", code=286)

        mock_collection.watch.side_effect = watch_side_effect

        from listening import listening
        listening("testcoll")

        # Should have made exactly 7 calls before circuit breaker fired
        assert call_count[0] == 7, f"Expected 7 calls, got {call_count[0]}"

        # Verify sentinel was written
        sentinel_calls = [
            c for c in mock_write.call_args_list
            if len(c.args) >= 3 and "_NEEDS_REINIT" in str(c.args[2])
        ]
        assert len(sentinel_calls) == 1


# ─────────────────────────────────────────────
# Load Test: Init Sync with Failures
# ─────────────────────────────────────────────
class TestLoadInitSync:
    @patch("mongodb_generic_mirroring.Thread")
    @patch("mongodb_generic_mirroring.init_sync")
    @patch("mongodb_generic_mirroring.init_table_schema")
    @patch("mongodb_generic_mirroring.push_file_to_lz")
    @patch("mongodb_generic_mirroring.read_from_file")
    @patch("mongodb_generic_mirroring.__get_all_collections")
    @patch("mongodb_generic_mirroring.__ensure_partner_events_in_lz")
    @patch("mongodb_generic_mirroring.Event")
    @patch("time.sleep")
    def test_multiple_collections_with_partial_failures(
        self, mock_sleep, mock_event, mock_partner, mock_get_colls,
        mock_read, mock_push, mock_schema, mock_init_sync, mock_thread,
        env_vars, monkeypatch
    ):
        """Simulate 10 collections, 3 fail init sync.
        All 10 should still get listener threads.
        """
        collections = [f"coll_{i}" for i in range(10)]
        monkeypatch.setenv("MONGO_COLLECTION", str(collections).replace("'", '"'))
        mock_get_colls.return_value = collections
        mock_read.return_value = None

        # Collections 2, 5, 8 fail init sync
        failing = {"coll_2", "coll_5", "coll_8"}

        def init_sync_side_effect(name):
            if name in failing:
                raise Exception(f"Simulated failure for {name}")
            return None

        mock_init_sync.side_effect = init_sync_side_effect
        mock_event_instance = MagicMock()
        mock_event.return_value = mock_event_instance

        from mongodb_generic_mirroring import mirror
        mirror()

        # All 10 collections should have listener threads
        assert mock_thread.call_count == 10, (
            f"Expected 10 listener threads, got {mock_thread.call_count}"
        )

        # init_sync should have been called for each collection
        # (failing ones get retried 3 times each)
        # 7 succeed on first try = 7 calls
        # 3 fail × 3 retries = 9 calls
        # Total = 16 calls
        assert mock_init_sync.call_count == 16


# ─────────────────────────────────────────────
# Stress test: sustained load with realistic failure patterns
# ─────────────────────────────────────────────
class TestStressScenario:
    """
    Simulates the exact CSX scenario:
    - Large collection doing init sync
    - Intermittent OneLake connection failures
    - ChangeStreamHistoryLost events
    - Verifies no TEMP file orphans and circuit breaker works
    """

    @pytest.mark.timeout(300)
    def test_csx_scenario_simulation(self, env_vars):
        """
        The CSX scenario:
        1. Waybill has 3.2M records — we simulate with 100 batches
        2. 15% of OneLake calls fail with connection refused
        3. ChangeStreamHistoryLost occurs 5 times for a collection
        """
        metrics = LoadTestMetrics()
        metrics.start_time = time.time()

        # Phase 1: Upload resilience
        onelake = SimulatedOneLake(failure_rate=0.15, latency_ms=(10, 100))
        upload_success = 0
        upload_fail = 0

        import push_file_to_lz as pfl

        for i in range(50):
            filepath = os.path.join(str(env_vars), f"batch_{i}.parquet")
            with open(filepath, "wb") as f:
                f.write(b"PAR1" + os.urandom(2048))

            try:
                orig_put = pfl.requests.put
                orig_patch = pfl.requests.patch
                orig_delete = pfl.requests.delete
                orig_post = pfl.requests.post
                orig_get = pfl.requests.get

                token_resp = MagicMock()
                token_resp.text = '{"access_token": "fake"}'
                token_resp.status_code = 200
                token_resp.json.return_value = {"access_token": "fake"}
                pfl.requests.post = lambda *a, **kw: token_resp

                def put_se(url, **kwargs):
                    headers = kwargs.get("headers", {})
                    if "?resource=file" in url:
                        resp = onelake.handle_create_temp(url)
                        m = MagicMock(); m.status_code = resp.status_code; m.raise_for_status = MagicMock()
                        return m
                    elif "x-ms-rename-source" in headers:
                        resp = onelake.handle_rename(url, headers["x-ms-rename-source"])
                        m = MagicMock(); m.status_code = resp.status_code; m.raise_for_status = MagicMock()
                        return m
                    m = MagicMock(); m.status_code = 200; m.raise_for_status = MagicMock()
                    return m

                def patch_se(url, **kwargs):
                    resp = onelake.handle_append(url, kwargs.get("data", b""))
                    m = MagicMock(); m.status_code = resp.status_code; m.raise_for_status = MagicMock()
                    return m

                def delete_se(url, **kwargs):
                    resp = onelake.handle_delete(url)
                    m = MagicMock(); m.status_code = resp.status_code; m.raise_for_status = MagicMock()
                    return m

                pfl.requests.put = put_se
                pfl.requests.patch = patch_se
                pfl.requests.delete = delete_se
                pfl.requests.get = lambda url, **kw: onelake.handle_get(url)

                try:
                    pfl.push_file_to_lz(filepath, "waybill")
                finally:
                    pfl.requests.put = orig_put
                    pfl.requests.patch = orig_patch
                    pfl.requests.delete = orig_delete
                    pfl.requests.post = orig_post
                    pfl.requests.get = orig_get

                upload_success += 1
            except Exception:
                upload_fail += 1
            finally:
                if os.path.exists(filepath):
                    os.remove(filepath)

        metrics.end_time = time.time()

        print(f"\nCSX Scenario Results:")
        print(f"  Uploads succeeded: {upload_success}/50")
        print(f"  Uploads failed:    {upload_fail}/50")
        print(f"  Orphaned TEMP:     {len(onelake.orphaned_temp_files)}")
        print(f"  OneLake requests:  {onelake.request_count}")
        print(f"  Failures injected: {onelake.failures_injected}")

        # Assertions
        assert len(onelake.orphaned_temp_files) == 0, (
            f"Found {len(onelake.orphaned_temp_files)} orphaned TEMP files: "
            f"{onelake.orphaned_temp_files}"
        )
        success_rate = upload_success / 50
        assert success_rate > 0.85, (
            f"Success rate {success_rate:.2%} below 85% threshold"
        )
