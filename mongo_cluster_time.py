"""Helpers for cluster-time handoff between init sync and change streams."""

from __future__ import annotations

from bson.timestamp import Timestamp
from pymongo import MongoClient
from pymongo.client_session import ClientSession


def get_cluster_time(client: MongoClient) -> Timestamp:
    """Return the cluster operationTime from a ping (fallback: $clusterTime)."""
    ping = client.admin.command("ping")
    operation_time = ping.get("operationTime")
    if isinstance(operation_time, Timestamp):
        return operation_time
    cluster_time_doc = ping.get("$clusterTime") or {}
    cluster_time = cluster_time_doc.get("clusterTime")
    if isinstance(cluster_time, Timestamp):
        return cluster_time
    raise RuntimeError(
        "ping response did not include operationTime or $clusterTime.clusterTime"
    )


def next_timestamp(ts: Timestamp) -> Timestamp:
    """Return Timestamp N+1 (inc overflow rolls into time+1, inc=1)."""
    if ts.inc >= 0xFFFFFFFF:
        return Timestamp(ts.time + 1, 1)
    return Timestamp(ts.time, ts.inc + 1)


def start_snapshot_session_at(
    client: MongoClient, cluster_time: Timestamp
) -> ClientSession:
    """
    Start a snapshot session pinned to atClusterTime=cluster_time.

    PyMongo 4.x exposes snapshot=True but no public snapshotTime/atClusterTime
    option; the driver stores the pin on ClientSession._snapshot_time and sends
    readConcern.level=snapshot with atClusterTime on subsequent reads.
    """
    session = client.start_session(snapshot=True)
    session._snapshot_time = cluster_time
    return session
