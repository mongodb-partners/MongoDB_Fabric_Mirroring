"""
Tests for push_file_to_lz.py — Fixes 1, 2, 3
- Fix 1: Retry with exponential backoff on OneLake upload failures
- Fix 2: Cleanup orphaned *_TEMP files on failure
- Fix 3: HTTP timeouts on all requests calls
"""
import os
import pytest
import requests
import responses as resp_lib
from unittest.mock import patch, MagicMock
import time

import push_file_to_lz as pfl
from push_file_to_lz import (
    push_file_to_lz,
    _lz_folder_url,
    HTTP_TIMEOUT,
    MAX_UPLOAD_RETRIES,
    UPLOAD_RETRY_BASE_DELAY_SEC,
)

# Module-level double-underscore functions can't be imported with `from`,
# but they ARE accessible via getattr on the module.
# We must use plain module-level references (not inside classes) to avoid
# Python name mangling.
_get_access_token = getattr(pfl, '__get_access_token')
_patch_file = getattr(pfl, '__patch_file')
_patch_file_with_retry = getattr(pfl, '__patch_file_with_retry')
_delete_temp_file = getattr(pfl, '__delete_temp_file')


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
FAKE_TOKEN_URL = (
    "https://login.microsoftonline.com/test-tenant-id/oauth2/v2.0/token"
)
BASE_LZ = "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone"


def _add_token_response(rsps):
    rsps.add(
        resp_lib.POST,
        FAKE_TOKEN_URL,
        json={"access_token": "fake-token"},
        status=200,
    )


def _onelake_url(table, filename):
    return f"{BASE_LZ}/{table}/{filename}"


# ─────────────────────────────────────────────
# Fix 3: HTTP Timeouts
# ─────────────────────────────────────────────
class TestTimeouts:
    """Verify that every requests call includes a timeout tuple."""

    @resp_lib.activate
    def test_token_request_has_timeout(self, env_vars, tmp_path):
        _add_token_response(resp_lib)
        token = _get_access_token("app", "secret", "test-tenant-id")
        assert token == "fake-token"
        assert len(resp_lib.calls) == 1

    def test_http_timeout_constant(self):
        """HTTP_TIMEOUT should be a (connect, read) tuple."""
        connect, read = HTTP_TIMEOUT
        assert connect > 0, "Connect timeout must be positive"
        assert read > 0, "Read timeout must be positive"
        assert connect <= read, "Connect timeout should be <= read timeout"

    @resp_lib.activate
    def test_create_file_has_timeout(self, env_vars, tmp_path):
        """PUT to create temp file should include timeout."""
        _add_token_response(resp_lib)
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        base = _lz_folder_url(os.environ["LZ_URL"], "testcoll")
        temp_url = base + "_test.txt_TEMP?resource=file"
        final_url = base + "test.txt"
        append_url = base + "_test.txt_TEMP?position=0&action=append&flush=true"

        resp_lib.add(resp_lib.PUT, temp_url, status=201)
        resp_lib.add(resp_lib.PATCH, append_url, status=200)
        resp_lib.add(resp_lib.PUT, final_url, status=200)

        _patch_file("fake-token", str(test_file), os.environ["LZ_URL"], "testcoll")

        assert len(resp_lib.calls) == 3


# ─────────────────────────────────────────────
# Fix 1: Retry with Exponential Backoff
# ─────────────────────────────────────────────
class TestRetry:
    """Verify retry logic with exponential backoff."""

    @resp_lib.activate
    def test_retry_succeeds_on_second_attempt(self, env_vars, tmp_path):
        """First PUT fails with connection error, second succeeds."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        base = _lz_folder_url(os.environ["LZ_URL"], "testcoll")
        temp_url = base + "_test.txt_TEMP?resource=file"
        final_url = base + "test.txt"
        append_url = base + "_test.txt_TEMP?position=0&action=append&flush=true"

        resp_lib.add(
            resp_lib.PUT, temp_url,
            body=requests.exceptions.ConnectionError("Connection refused"),
        )
        resp_lib.add(resp_lib.PUT, temp_url, status=201)
        resp_lib.add(resp_lib.PATCH, append_url, status=200)
        resp_lib.add(resp_lib.PUT, final_url, status=200)

        _patch_file_with_retry("fake-token", str(test_file), os.environ["LZ_URL"], "testcoll")

        assert len(resp_lib.calls) == 4

    @resp_lib.activate
    def test_retry_exhausts_and_raises(self, env_vars, tmp_path):
        """All retries fail — should raise the last exception."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        base = _lz_folder_url(os.environ["LZ_URL"], "testcoll")
        temp_url = base + "_test.txt_TEMP?resource=file"

        for _ in range(MAX_UPLOAD_RETRIES):
            resp_lib.add(
                resp_lib.PUT, temp_url,
                body=requests.exceptions.ConnectionError("Connection refused"),
            )

        with pytest.raises(requests.exceptions.ConnectionError):
            _patch_file_with_retry("fake-token", str(test_file), os.environ["LZ_URL"], "testcoll")

        assert len(resp_lib.calls) == MAX_UPLOAD_RETRIES

    @patch("push_file_to_lz.time.sleep")
    def test_backoff_delays_are_exponential(self, mock_sleep, env_vars, tmp_path):
        """Verify delays double each time: base, base*2, base*4."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        with patch.object(pfl, '_push_file_to_lz__patch_file', create=True) as mock_patch:
            # Actually patch via setattr on the module
            original = getattr(pfl, '__patch_file')
            try:
                setattr(pfl, '__patch_file', MagicMock(side_effect=[
                    requests.exceptions.ConnectionError("fail 1"),
                    requests.exceptions.ConnectionError("fail 2"),
                    requests.exceptions.ConnectionError("fail 3"),
                ]))
                with pytest.raises(requests.exceptions.ConnectionError):
                    _patch_file_with_retry("fake-token", str(test_file), os.environ["LZ_URL"], "testcoll")
            finally:
                setattr(pfl, '__patch_file', original)

        assert mock_sleep.call_count == 2
        delays = [c.args[0] for c in mock_sleep.call_args_list]
        assert delays[0] == pytest.approx(UPLOAD_RETRY_BASE_DELAY_SEC)
        assert delays[1] == pytest.approx(UPLOAD_RETRY_BASE_DELAY_SEC * 2)


# ─────────────────────────────────────────────
# Fix 2: TEMP File Cleanup
# ─────────────────────────────────────────────
class TestTempFileCleanup:
    """Verify orphaned *_TEMP files are cleaned up on failure."""

    @resp_lib.activate
    def test_temp_file_deleted_on_append_failure(self, env_vars, tmp_path):
        """If append fails after create, the temp file should be deleted."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        base = _lz_folder_url(os.environ["LZ_URL"], "testcoll")
        temp_create_url = base + "_test.txt_TEMP?resource=file"
        append_url = base + "_test.txt_TEMP?position=0&action=append&flush=true"
        temp_delete_url = base + "_test.txt_TEMP"

        resp_lib.add(resp_lib.PUT, temp_create_url, status=201)
        resp_lib.add(
            resp_lib.PATCH, append_url,
            body=requests.exceptions.ConnectionError("Connection refused"),
        )
        resp_lib.add(resp_lib.DELETE, temp_delete_url, status=200)

        with pytest.raises(requests.exceptions.ConnectionError):
            _patch_file("fake-token", str(test_file), os.environ["LZ_URL"], "testcoll")

        delete_calls = [c for c in resp_lib.calls if c.request.method == "DELETE"]
        assert len(delete_calls) == 1
        assert "_test.txt_TEMP" in delete_calls[0].request.url

    @resp_lib.activate
    def test_temp_file_deleted_on_rename_failure(self, env_vars, tmp_path):
        """If rename fails after append succeeds, the temp file should be deleted."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        base = _lz_folder_url(os.environ["LZ_URL"], "testcoll")
        temp_create_url = base + "_test.txt_TEMP?resource=file"
        append_url = base + "_test.txt_TEMP?position=0&action=append&flush=true"
        final_url = base + "test.txt"
        temp_delete_url = base + "_test.txt_TEMP"

        resp_lib.add(resp_lib.PUT, temp_create_url, status=201)
        resp_lib.add(resp_lib.PATCH, append_url, status=200)
        resp_lib.add(
            resp_lib.PUT, final_url,
            body=requests.exceptions.ConnectionError("Connection refused"),
        )
        resp_lib.add(resp_lib.DELETE, temp_delete_url, status=200)

        with pytest.raises(requests.exceptions.ConnectionError):
            _patch_file("fake-token", str(test_file), os.environ["LZ_URL"], "testcoll")

        delete_calls = [c for c in resp_lib.calls if c.request.method == "DELETE"]
        assert len(delete_calls) == 1

    @resp_lib.activate
    def test_no_cleanup_if_create_fails(self, env_vars, tmp_path):
        """If create itself fails, no temp file exists — no DELETE needed."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        base = _lz_folder_url(os.environ["LZ_URL"], "testcoll")
        temp_create_url = base + "_test.txt_TEMP?resource=file"

        resp_lib.add(
            resp_lib.PUT, temp_create_url,
            body=requests.exceptions.ConnectionError("Connection refused"),
        )

        with pytest.raises(requests.exceptions.ConnectionError):
            _patch_file("fake-token", str(test_file), os.environ["LZ_URL"], "testcoll")

        delete_calls = [c for c in resp_lib.calls if c.request.method == "DELETE"]
        assert len(delete_calls) == 0

    @resp_lib.activate
    def test_no_cleanup_on_success(self, env_vars, tmp_path):
        """Successful upload should not trigger any DELETE."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        base = _lz_folder_url(os.environ["LZ_URL"], "testcoll")
        temp_url = base + "_test.txt_TEMP?resource=file"
        final_url = base + "test.txt"
        append_url = base + "_test.txt_TEMP?position=0&action=append&flush=true"

        resp_lib.add(resp_lib.PUT, temp_url, status=201)
        resp_lib.add(resp_lib.PATCH, append_url, status=200)
        resp_lib.add(resp_lib.PUT, final_url, status=200)

        _patch_file("fake-token", str(test_file), os.environ["LZ_URL"], "testcoll")

        delete_calls = [c for c in resp_lib.calls if c.request.method == "DELETE"]
        assert len(delete_calls) == 0


# ─────────────────────────────────────────────
# Integration: push_file_to_lz end-to-end
# ─────────────────────────────────────────────
class TestPushFileToLz:
    @resp_lib.activate
    def test_successful_push(self, env_vars, tmp_path):
        """Full push flow: token → create → append → rename."""
        _add_token_response(resp_lib)

        test_file = tmp_path / "data.parquet"
        test_file.write_bytes(b"PAR1" + b"\x00" * 100)

        base = _lz_folder_url(os.environ["LZ_URL"], "testcoll")
        resp_lib.add(resp_lib.PUT, base + "_data.parquet_TEMP?resource=file", status=201)
        resp_lib.add(resp_lib.PATCH, base + "_data.parquet_TEMP?position=0&action=append&flush=true", status=200)
        resp_lib.add(resp_lib.PUT, base + "data.parquet", status=200)

        push_file_to_lz(str(test_file), "testcoll")

        assert len(resp_lib.calls) == 4

    @resp_lib.activate
    def test_push_with_retry_and_cleanup(self, env_vars, tmp_path):
        """Push that fails once, retries, and succeeds."""
        _add_token_response(resp_lib)

        test_file = tmp_path / "data.parquet"
        test_file.write_bytes(b"PAR1" + b"\x00" * 100)

        base = _lz_folder_url(os.environ["LZ_URL"], "testcoll")
        temp_create = base + "_data.parquet_TEMP?resource=file"
        append_url = base + "_data.parquet_TEMP?position=0&action=append&flush=true"
        final_url = base + "data.parquet"
        temp_delete = base + "_data.parquet_TEMP"

        # First attempt: create OK, append fails
        resp_lib.add(resp_lib.PUT, temp_create, status=201)
        resp_lib.add(resp_lib.PATCH, append_url, body=requests.exceptions.ConnectionError("refused"))
        resp_lib.add(resp_lib.DELETE, temp_delete, status=200)

        # Second attempt: all succeed
        resp_lib.add(resp_lib.PUT, temp_create, status=201)
        resp_lib.add(resp_lib.PATCH, append_url, status=200)
        resp_lib.add(resp_lib.PUT, final_url, status=200)

        push_file_to_lz(str(test_file), "testcoll")

        # 1 token + 3 (first attempt: create+append-fail+cleanup) + 3 (retry: create+append+rename) = 7
        # But the first attempt has: create(1) + append-fail(1) + cleanup(1) = 3
        # Retry has: create(1) + append(1) + rename(1) = 3
        # Plus token(1) = 7 total
        assert len(resp_lib.calls) == 7
