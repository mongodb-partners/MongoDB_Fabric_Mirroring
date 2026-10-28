"""
Shared fixtures and helpers for all tests.
"""
import os
import sys
import pytest
import pickle
import tempfile
import shutil
from unittest.mock import MagicMock, patch
from bson import ObjectId
from bson.timestamp import Timestamp

# Ensure the project root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ─────────────────────────────────────────────
# Environment fixture — sets all required env vars
# ─────────────────────────────────────────────
@pytest.fixture(autouse=True)
def env_vars(tmp_path, monkeypatch):
    """Set all required environment variables for every test."""
    data_dir = str(tmp_path / "data_files")
    os.makedirs(data_dir, exist_ok=True)

    defaults = {
        "MONGO_CONN_STR": "mongodb://localhost:27017",
        "MONGO_DB_NAME": "testdb",
        "MONGO_COLLECTION": "testcoll",
        "LZ_URL": "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone",
        "APP_ID": "test-app-id",
        "SECRET": "test-secret",
        "TENANT_ID": "test-tenant-id",
        "INIT_LOAD_BATCH_SIZE": "1000",
        "DELTA_SYNC_BATCH_SIZE": "100",
        "TIME_THRESHOLD_IN_SEC": "180",
        "APP_LOG_LEVEL": "DEBUG",
        # Fix-related env vars
        "HTTP_CONNECT_TIMEOUT_SEC": "5",
        "HTTP_READ_TIMEOUT_SEC": "30",
        "MAX_UPLOAD_RETRIES": "3",
        "UPLOAD_RETRY_BASE_DELAY_SEC": "0.1",  # fast retries in tests
        "MAX_CONSECUTIVE_HISTORY_LOSS": "3",
        "MAX_INIT_SYNC_RETRIES": "3",
        "INIT_SYNC_BATCH_SLEEP_SEC": "0",  # no sleep in tests
    }
    for key, val in defaults.items():
        monkeypatch.setenv(key, val)

    # Patch DATA_FILES_PATH to use tmp_path
    monkeypatch.setattr("constants.DATA_FILES_PATH", data_dir)

    return tmp_path


# ─────────────────────────────────────────────
# Mock OneLake HTTP responses
# ─────────────────────────────────────────────
@pytest.fixture()
def mock_onelake_success():
    """Mock all OneLake HTTP calls to succeed."""
    import responses as resp_lib

    with resp_lib.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        # Token endpoint
        rsps.add(
            resp_lib.POST,
            "https://login.microsoftonline.com/test-tenant-id/oauth2/v2.0/token",
            json={"access_token": "fake-token", "expires_in": 3600},
            status=200,
        )
        # Create temp file (PUT ?resource=file)
        rsps.add(
            resp_lib.PUT,
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone/testcoll/_test_TEMP",
            status=201,
        )
        # Append (PATCH)
        rsps.add(
            resp_lib.PATCH,
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone/testcoll/_test_TEMP",
            status=200,
        )
        # Rename (PUT to final)
        rsps.add(
            resp_lib.PUT,
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone/testcoll/test",
            status=200,
        )
        # Read file (GET) — return 404 by default (not found)
        rsps.add(
            resp_lib.GET,
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone/testcoll/_resume_token.pkl",
            status=404,
        )
        rsps.add(
            resp_lib.GET,
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone/testcoll/_init_sync_status.pkl",
            status=404,
        )
        rsps.add(
            resp_lib.GET,
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone/testcoll/_init_cluster_time.pkl",
            status=404,
        )
        rsps.add(
            resp_lib.GET,
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone/testcoll/_max_id.pkl",
            status=404,
        )
        rsps.add(
            resp_lib.GET,
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone/testcoll/_last_id.pkl",
            status=404,
        )
        rsps.add(
            resp_lib.GET,
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone/testcoll/_last_created_parquet.pkl",
            status=404,
        )
        rsps.add(
            resp_lib.GET,
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone/testcoll/_metadata.json",
            status=404,
        )
        rsps.add(
            resp_lib.GET,
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone/testcoll/_internal_schema.pkl",
            status=404,
        )
        # Catch-all for any other GET/PUT/PATCH/DELETE
        rsps.add_callback(
            resp_lib.GET,
            "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files/LandingZone/",
            callback=lambda request: (404, {}, ""),
        )
        yield rsps


# ─────────────────────────────────────────────
# Mock MongoDB
# ─────────────────────────────────────────────
@pytest.fixture()
def mock_mongo():
    """Provide a mocked pymongo MongoClient."""
    mock_client = MagicMock()
    mock_db = MagicMock()
    mock_collection = MagicMock()

    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)

    return {
        "client": mock_client,
        "db": mock_db,
        "collection": mock_collection,
    }


# ─────────────────────────────────────────────
# Helper: create fake documents
# ─────────────────────────────────────────────
def make_documents(count: int, start_id: int = 0) -> list[dict]:
    """Generate a list of fake MongoDB documents."""
    return [
        {
            "_id": ObjectId(),
            "field1": f"value_{i}",
            "field2": i,
            "field3": 100.5 + i,
            "field4": True,
        }
        for i in range(start_id, start_id + count)
    ]


# ─────────────────────────────────────────────
# Helper: create a fake change event
# ─────────────────────────────────────────────
def make_change_event(operation: str = "insert", doc: dict = None) -> dict:
    """Create a fake MongoDB change stream event."""
    if doc is None:
        doc = make_documents(1)[0]
    token = {"_data": f"fake-token-{ObjectId()}"}
    event = {
        "_id": token,
        "operationType": operation,
        "fullDocument": doc,
        "documentKey": {"_id": doc["_id"]},
        "ns": {"db": "testdb", "coll": "testcoll"},
    }
    return event
