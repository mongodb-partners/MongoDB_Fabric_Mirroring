import sqlite3
import os
import json
import threading
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class DeadQueueDatabase:
    """
    SQLite database manager for storing documents with failed conversions.
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
            db_path = os.path.join(current_dir, "dead_queue.db")
        
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
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dead_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                collection_name TEXT NOT NULL,
                document_id TEXT,
                field_name TEXT NOT NULL,
                original_value TEXT,
                original_type TEXT,
                target_type TEXT,
                error_message TEXT,
                document_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dq_timestamp ON dead_queue(timestamp DESC)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dq_collection ON dead_queue(collection_name)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dq_document_id ON dead_queue(document_id)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dq_field ON dead_queue(field_name)
        ''')
        
        conn.commit()
    
    def add_failed_conversion(
        self,
        collection_name: str,
        document_id: Optional[str],
        field_name: str,
        original_value: str,
        original_type: str,
        target_type: str,
        error_message: str,
        document_json: Optional[str] = None
    ) -> int:
        """
        Add a failed conversion to the dead queue.
        Returns the inserted row ID.
        """
        timestamp = datetime.utcnow().isoformat()
        
        # Truncate very long values
        if original_value and len(original_value) > 1000:
            original_value = original_value[:1000] + "...[truncated]"
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO dead_queue (
                timestamp, collection_name, document_id, field_name,
                original_value, original_type, target_type, error_message, document_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            timestamp, collection_name, document_id, field_name,
            original_value, original_type, target_type, error_message, document_json
        ))
        
        conn.commit()
        return cursor.lastrowid
    
    def query_dead_queue(
        self,
        collection_filter: Optional[str] = None,
        field_filter: Optional[str] = None,
        search_text: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_desc: bool = True
    ) -> Dict[str, Any]:
        """
        Query the dead queue with various filters.
        Returns a dict with 'entries', 'total', 'limit', 'offset'.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        where_clauses = []
        params = []
        
        if collection_filter:
            where_clauses.append("collection_name = ?")
            params.append(collection_filter)
        
        if field_filter:
            where_clauses.append("field_name = ?")
            params.append(field_filter)
        
        if search_text:
            where_clauses.append("""
                (original_value LIKE ? OR error_message LIKE ? OR document_id LIKE ? OR field_name LIKE ?)
            """)
            search_param = f"%{search_text}%"
            params.extend([search_param, search_param, search_param, search_param])
        
        if start_time:
            where_clauses.append("timestamp >= ?")
            params.append(start_time)
        
        if end_time:
            where_clauses.append("timestamp <= ?")
            params.append(end_time)
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        order_dir = "DESC" if order_desc else "ASC"
        
        # Get total count
        count_sql = f"SELECT COUNT(*) FROM dead_queue WHERE {where_sql}"
        cursor.execute(count_sql, params)
        total = cursor.fetchone()[0]
        
        # Get entries
        query_sql = f'''
            SELECT id, timestamp, collection_name, document_id, field_name,
                   original_value, original_type, target_type, error_message
            FROM dead_queue
            WHERE {where_sql}
            ORDER BY timestamp {order_dir}, id {order_dir}
            LIMIT ? OFFSET ?
        '''
        cursor.execute(query_sql, params + [limit, offset])
        
        entries = [dict(row) for row in cursor.fetchall()]
        
        return {
            "entries": entries,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": offset + len(entries) < total
        }
    
    def get_entry_details(self, entry_id: int) -> Optional[Dict[str, Any]]:
        """Get full details of a dead queue entry including document JSON."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM dead_queue WHERE id = ?
        ''', (entry_id,))
        
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def get_collections(self) -> List[str]:
        """Get list of unique collection names from dead queue."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT collection_name
            FROM dead_queue
            ORDER BY collection_name
        ''')
        
        return [row['collection_name'] for row in cursor.fetchall()]
    
    def get_fields(self, collection_name: Optional[str] = None) -> List[str]:
        """Get list of unique field names that had failures."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if collection_name:
            cursor.execute('''
                SELECT DISTINCT field_name
                FROM dead_queue
                WHERE collection_name = ?
                ORDER BY field_name
            ''', (collection_name,))
        else:
            cursor.execute('''
                SELECT DISTINCT field_name
                FROM dead_queue
                ORDER BY field_name
            ''')
        
        return [row['field_name'] for row in cursor.fetchall()]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get dead queue statistics."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Total count
        cursor.execute('SELECT COUNT(*) FROM dead_queue')
        total = cursor.fetchone()[0]
        
        # Count by collection
        cursor.execute('''
            SELECT collection_name, COUNT(*) as count
            FROM dead_queue
            GROUP BY collection_name
            ORDER BY count DESC
        ''')
        by_collection = [dict(row) for row in cursor.fetchall()]
        
        # Count by field
        cursor.execute('''
            SELECT field_name, COUNT(*) as count
            FROM dead_queue
            GROUP BY field_name
            ORDER BY count DESC
            LIMIT 10
        ''')
        by_field = [dict(row) for row in cursor.fetchall()]
        
        # Recent 24 hours
        twenty_four_hours_ago = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        cursor.execute('''
            SELECT COUNT(*) FROM dead_queue WHERE timestamp >= ?
        ''', (twenty_four_hours_ago,))
        last_24_hours = cursor.fetchone()[0]
        
        return {
            "total": total,
            "by_collection": by_collection,
            "by_field": by_field,
            "last_24_hours": last_24_hours
        }
    
    def delete_entry(self, entry_id: int) -> bool:
        """Delete a single entry from the dead queue."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM dead_queue WHERE id = ?', (entry_id,))
        conn.commit()
        
        return cursor.rowcount > 0
    
    def delete_by_collection(self, collection_name: str) -> int:
        """Delete all entries for a collection."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM dead_queue WHERE collection_name = ?', (collection_name,))
        deleted = cursor.rowcount
        conn.commit()
        
        return deleted
    
    def cleanup_old_entries(self, retention_days: int = 30) -> int:
        """Delete entries older than the specified retention period."""
        cutoff_date = (datetime.utcnow() - timedelta(days=retention_days)).isoformat()
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM dead_queue WHERE timestamp < ?', (cutoff_date,))
        deleted_count = cursor.rowcount
        conn.commit()
        
        if deleted_count > 0:
            cursor.execute('VACUUM')
        
        return deleted_count
    
    def close(self):
        """Close the thread-local database connection."""
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.close()
            self._local.connection = None


def get_dead_queue_database() -> DeadQueueDatabase:
    """Get the singleton DeadQueueDatabase instance."""
    return DeadQueueDatabase()
