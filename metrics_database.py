import sqlite3
import os
import threading
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class MetricsDatabase:
    """
    SQLite database manager for storing and querying application metrics.
    Thread-safe implementation using connection per thread.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, db_path: str = None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_path: str = None):
        if self._initialized:
            return
            
        if db_path is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(current_dir, "metrics.db")
        
        self.db_path = db_path
        self._local = threading.local()
        self._init_schema()
        self._initialized = True
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            self._local.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            self._local.connection.row_factory = sqlite3.Row
        return self._local.connection
    
    def _init_schema(self):
        """Initialize the database schema."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Documents fetched metrics
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS documents_fetched (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                collection_name TEXT NOT NULL,
                sync_type TEXT NOT NULL,
                document_count INTEGER NOT NULL,
                batch_number INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Conversion metrics
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS conversions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                collection_name TEXT NOT NULL,
                sync_type TEXT NOT NULL DEFAULT 'init',
                successful INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Add sync_type column if it doesn't exist (migration for existing databases)
        try:
            cursor.execute("ALTER TABLE conversions ADD COLUMN sync_type TEXT NOT NULL DEFAULT 'init'")
            conn.commit()
        except:
            pass  # Column already exists
        
        # Parquet file metrics
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS parquet_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                collection_name TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_size_bytes INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                sync_type TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_docs_timestamp ON documents_fetched(timestamp DESC)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_docs_collection ON documents_fetched(collection_name)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_conv_timestamp ON conversions(timestamp DESC)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_conv_collection ON conversions(collection_name)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_parquet_timestamp ON parquet_files(timestamp DESC)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_parquet_collection ON parquet_files(collection_name)
        ''')
        
        conn.commit()
    
    def record_documents_fetched(
        self,
        collection_name: str,
        sync_type: str,
        document_count: int,
        batch_number: Optional[int] = None
    ) -> int:
        """
        Record documents fetched from MongoDB.
        sync_type: 'init' or 'delta'
        """
        timestamp = datetime.utcnow().isoformat()
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO documents_fetched (timestamp, collection_name, sync_type, document_count, batch_number)
            VALUES (?, ?, ?, ?, ?)
        ''', (timestamp, collection_name, sync_type, document_count, batch_number))
        
        conn.commit()
        return cursor.lastrowid
    
    def record_conversion(
        self,
        collection_name: str,
        successful: int = 0,
        failed: int = 0,
        sync_type: str = 'init'
    ) -> int:
        """
        Record conversion success/failure counts.
        sync_type: 'init' or 'delta'
        """
        timestamp = datetime.utcnow().isoformat()
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO conversions (timestamp, collection_name, sync_type, successful, failed)
            VALUES (?, ?, ?, ?, ?)
        ''', (timestamp, collection_name, sync_type, successful, failed))
        
        conn.commit()
        return cursor.lastrowid
    
    def record_parquet_file(
        self,
        collection_name: str,
        file_name: str,
        file_size_bytes: int,
        row_count: int,
        sync_type: str
    ) -> int:
        """
        Record parquet file upload details.
        sync_type: 'init' or 'delta'
        """
        timestamp = datetime.utcnow().isoformat()
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO parquet_files (timestamp, collection_name, file_name, file_size_bytes, row_count, sync_type)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (timestamp, collection_name, file_name, file_size_bytes, row_count, sync_type))
        
        conn.commit()
        return cursor.lastrowid
    
    def get_documents_fetched_summary(
        self,
        collection_name: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get summary of documents fetched in the time period."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        
        where_clause = "timestamp >= ?"
        params = [cutoff]
        
        if collection_name:
            where_clause += " AND collection_name = ?"
            params.append(collection_name)
        
        cursor.execute(f'''
            SELECT 
                collection_name,
                sync_type,
                SUM(document_count) as total_documents,
                COUNT(*) as batch_count
            FROM documents_fetched
            WHERE {where_clause}
            GROUP BY collection_name, sync_type
            ORDER BY collection_name, sync_type
        ''', params)
        
        results = [dict(row) for row in cursor.fetchall()]
        
        cursor.execute(f'''
            SELECT SUM(document_count) as grand_total
            FROM documents_fetched
            WHERE {where_clause}
        ''', params)
        grand_total = cursor.fetchone()['grand_total'] or 0
        
        return {
            "by_collection": results,
            "grand_total": grand_total,
            "hours": hours
        }
    
    def get_documents_fetched_timeseries(
        self,
        collection_name: Optional[str] = None,
        hours: int = 24,
        interval_minutes: int = 60
    ) -> List[Dict[str, Any]]:
        """Get time series data for documents fetched."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        
        where_clause = "timestamp >= ?"
        params = [cutoff]
        
        if collection_name:
            where_clause += " AND collection_name = ?"
            params.append(collection_name)
        
        cursor.execute(f'''
            SELECT 
                strftime('%Y-%m-%dT%H:', timestamp) || 
                    printf('%02d', (CAST(strftime('%M', timestamp) AS INTEGER) / {interval_minutes}) * {interval_minutes}) || 
                    ':00' as time_bucket,
                SUM(document_count) as document_count,
                sync_type
            FROM documents_fetched
            WHERE {where_clause}
            GROUP BY time_bucket, sync_type
            ORDER BY time_bucket
        ''', params)
        
        return [dict(row) for row in cursor.fetchall()]
    
    def get_conversion_summary(
        self,
        collection_name: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get summary of conversions in the time period."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        
        where_clause = "timestamp >= ?"
        params = [cutoff]
        
        if collection_name:
            where_clause += " AND collection_name = ?"
            params.append(collection_name)
        
        cursor.execute(f'''
            SELECT 
                collection_name,
                SUM(successful) as total_successful,
                SUM(failed) as total_failed
            FROM conversions
            WHERE {where_clause}
            GROUP BY collection_name
            ORDER BY collection_name
        ''', params)
        
        results = [dict(row) for row in cursor.fetchall()]
        
        cursor.execute(f'''
            SELECT 
                SUM(successful) as total_successful,
                SUM(failed) as total_failed
            FROM conversions
            WHERE {where_clause}
        ''', params)
        row = cursor.fetchone()
        
        return {
            "by_collection": results,
            "total_successful": row['total_successful'] or 0,
            "total_failed": row['total_failed'] or 0,
            "hours": hours
        }
    
    def get_conversion_timeseries(
        self,
        collection_name: Optional[str] = None,
        hours: int = 24,
        interval_minutes: int = 60
    ) -> List[Dict[str, Any]]:
        """Get time series data for conversions grouped by sync_type."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        
        where_clause = "timestamp >= ?"
        params = [cutoff]
        
        if collection_name:
            where_clause += " AND collection_name = ?"
            params.append(collection_name)
        
        cursor.execute(f'''
            SELECT 
                strftime('%Y-%m-%dT%H:', timestamp) || 
                    printf('%02d', (CAST(strftime('%M', timestamp) AS INTEGER) / {interval_minutes}) * {interval_minutes}) || 
                    ':00' as time_bucket,
                sync_type,
                SUM(successful) as successful,
                SUM(failed) as failed
            FROM conversions
            WHERE {where_clause}
            GROUP BY time_bucket, sync_type
            ORDER BY time_bucket
        ''', params)
        
        return [dict(row) for row in cursor.fetchall()]
    
    def get_parquet_files_summary(
        self,
        collection_name: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get summary of parquet files uploaded in the time period."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        
        where_clause = "timestamp >= ?"
        params = [cutoff]
        
        if collection_name:
            where_clause += " AND collection_name = ?"
            params.append(collection_name)
        
        cursor.execute(f'''
            SELECT 
                collection_name,
                sync_type,
                COUNT(*) as file_count,
                SUM(file_size_bytes) as total_size_bytes,
                SUM(row_count) as total_rows,
                AVG(file_size_bytes) as avg_file_size,
                AVG(row_count) as avg_rows_per_file
            FROM parquet_files
            WHERE {where_clause}
            GROUP BY collection_name, sync_type
            ORDER BY collection_name, sync_type
        ''', params)
        
        results = [dict(row) for row in cursor.fetchall()]
        
        cursor.execute(f'''
            SELECT 
                COUNT(*) as total_files,
                SUM(file_size_bytes) as total_size_bytes,
                SUM(row_count) as total_rows
            FROM parquet_files
            WHERE {where_clause}
        ''', params)
        row = cursor.fetchone()
        
        return {
            "by_collection": results,
            "total_files": row['total_files'] or 0,
            "total_size_bytes": row['total_size_bytes'] or 0,
            "total_rows": row['total_rows'] or 0,
            "hours": hours
        }
    
    def get_parquet_files_list(
        self,
        collection_name: Optional[str] = None,
        hours: int = 24,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Get list of parquet files with pagination."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        
        where_clause = "timestamp >= ?"
        params = [cutoff]
        
        if collection_name:
            where_clause += " AND collection_name = ?"
            params.append(collection_name)
        
        # Get total count
        cursor.execute(f'''
            SELECT COUNT(*) FROM parquet_files WHERE {where_clause}
        ''', params)
        total = cursor.fetchone()[0]
        
        # Get paginated results
        cursor.execute(f'''
            SELECT 
                timestamp,
                collection_name,
                file_name,
                file_size_bytes,
                row_count,
                sync_type
            FROM parquet_files
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        ''', params + [limit, offset])
        
        files = [dict(row) for row in cursor.fetchall()]
        
        return {
            "files": files,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": offset + len(files) < total
        }
    
    def get_parquet_timeseries(
        self,
        collection_name: Optional[str] = None,
        hours: int = 24,
        interval_minutes: int = 60
    ) -> List[Dict[str, Any]]:
        """Get time series data for parquet files."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        
        where_clause = "timestamp >= ?"
        params = [cutoff]
        
        if collection_name:
            where_clause += " AND collection_name = ?"
            params.append(collection_name)
        
        cursor.execute(f'''
            SELECT 
                strftime('%Y-%m-%dT%H:', timestamp) || 
                    printf('%02d', (CAST(strftime('%M', timestamp) AS INTEGER) / {interval_minutes}) * {interval_minutes}) || 
                    ':00' as time_bucket,
                COUNT(*) as file_count,
                SUM(file_size_bytes) as total_size,
                SUM(row_count) as total_rows
            FROM parquet_files
            WHERE {where_clause}
            GROUP BY time_bucket
            ORDER BY time_bucket
        ''', params)
        
        return [dict(row) for row in cursor.fetchall()]
    
    def get_collections(self) -> List[str]:
        """Get list of unique collection names from all metrics tables."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT collection_name FROM (
                SELECT collection_name FROM documents_fetched
                UNION
                SELECT collection_name FROM conversions
                UNION
                SELECT collection_name FROM parquet_files
            )
            ORDER BY collection_name
        ''')
        
        return [row['collection_name'] for row in cursor.fetchall()]
    
    def get_dashboard_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get overall dashboard summary."""
        docs = self.get_documents_fetched_summary(hours=hours)
        conv = self.get_conversion_summary(hours=hours)
        parquet = self.get_parquet_files_summary(hours=hours)
        
        return {
            "documents": {
                "total_fetched": docs["grand_total"],
            },
            "conversions": {
                "successful": conv["total_successful"],
                "failed": conv["total_failed"],
                "success_rate": round(
                    conv["total_successful"] / (conv["total_successful"] + conv["total_failed"]) * 100, 2
                ) if (conv["total_successful"] + conv["total_failed"]) > 0 else 100
            },
            "parquet_files": {
                "total_files": parquet["total_files"],
                "total_size_bytes": parquet["total_size_bytes"],
                "total_rows": parquet["total_rows"]
            },
            "hours": hours
        }
    
    def cleanup_old_metrics(self, retention_days: int = 30) -> Dict[str, int]:
        """Delete metrics older than the specified retention period."""
        cutoff_date = (datetime.utcnow() - timedelta(days=retention_days)).isoformat()
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        deleted = {}
        
        for table in ['documents_fetched', 'conversions', 'parquet_files']:
            cursor.execute(f'DELETE FROM {table} WHERE timestamp < ?', (cutoff_date,))
            deleted[table] = cursor.rowcount
        
        conn.commit()
        
        if sum(deleted.values()) > 0:
            cursor.execute('VACUUM')
        
        return deleted
    
    def close(self):
        """Close the thread-local database connection."""
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.close()
            self._local.connection = None


def get_metrics_database() -> MetricsDatabase:
    """Get the singleton MetricsDatabase instance."""
    return MetricsDatabase()
