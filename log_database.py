import sqlite3
import os
import re
import threading
import base64
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

try:
    import cramjam
    COMPRESSION_AVAILABLE = True
except ImportError:
    COMPRESSION_AVAILABLE = False

COMPRESSED_PREFIX = "ZSTD:"
COMPRESSION_LEVEL = 3  # Balance between speed and compression ratio

logger = logging.getLogger(__name__)

BACKEND_LOGGER_PREFIXES = (
    'werkzeug',
    'flask',
    'urllib3',
    'requests',
    'pymongo',
    'bson',
    'asyncio',
    'concurrent',
    'multiprocessing',
    'threading',
    'socket',
    'ssl',
    'http',
    'certifi',
    'charset_normalizer',
    'idna',
)

SOURCE_TYPE_APPLICATION = 'application'
SOURCE_TYPE_BACKEND = 'backend'


class LogDatabase:
    """
    SQLite database manager for storing and querying application logs.
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
            db_path = os.path.join(current_dir, "logs.db")
        
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
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                level TEXT NOT NULL,
                level_no INTEGER NOT NULL,
                logger_name TEXT NOT NULL,
                collection_name TEXT,
                source_type TEXT DEFAULT 'application',
                message TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute("PRAGMA table_info(logs)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'source_type' not in columns:
            cursor.execute("ALTER TABLE logs ADD COLUMN source_type TEXT DEFAULT 'application'")
            cursor.execute(f'''
                UPDATE logs SET source_type = 'backend' 
                WHERE {" OR ".join([f"logger_name LIKE '{p}%'" for p in BACKEND_LOGGER_PREFIXES])}
            ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp DESC)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_logs_collection ON logs(collection_name)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_logs_level_no ON logs(level_no)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_logs_source_type ON logs(source_type)
        ''')
        
        conn.commit()
    
    @staticmethod
    def compress_message(message: str) -> str:
        """
        Compress a log message using zstd if available and beneficial.
        Returns the original message if compression is not available or not beneficial.
        """
        if not COMPRESSION_AVAILABLE or len(message) < 100:
            return message
        
        try:
            compressed = cramjam.zstd.compress(message.encode('utf-8'), level=COMPRESSION_LEVEL)
            encoded = base64.b64encode(bytes(compressed)).decode('ascii')
            result = f"{COMPRESSED_PREFIX}{encoded}"
            if len(result) < len(message):
                return result
            return message
        except Exception:
            return message
    
    @staticmethod
    def decompress_message(message: str) -> str:
        """
        Decompress a log message if it was compressed.
        Returns the original message if not compressed or decompression fails.
        """
        if not message.startswith(COMPRESSED_PREFIX):
            return message
        
        if not COMPRESSION_AVAILABLE:
            return message
        
        try:
            encoded = message[len(COMPRESSED_PREFIX):]
            compressed = base64.b64decode(encoded)
            decompressed = cramjam.zstd.decompress(compressed)
            return bytes(decompressed).decode('utf-8')
        except Exception:
            return message
    
    @staticmethod
    def extract_collection_name(logger_name: str) -> Optional[str]:
        """
        Extract collection name from logger name pattern like 'module[collection]'.
        Returns None if no collection name is found.
        """
        match = re.search(r'\[([^\]]+)\]', logger_name)
        return match.group(1) if match else None
    
    @staticmethod
    def determine_source_type(logger_name: str) -> str:
        """
        Determine if the log is from application code or backend/framework.
        Returns 'application' or 'backend'.
        """
        logger_lower = logger_name.lower()
        base_logger = logger_lower.split('[')[0].split('.')[0]
        
        if base_logger in BACKEND_LOGGER_PREFIXES or logger_lower.startswith(BACKEND_LOGGER_PREFIXES):
            return SOURCE_TYPE_BACKEND
        return SOURCE_TYPE_APPLICATION
    
    def insert_log(
        self,
        timestamp: str,
        level: str,
        level_no: int,
        logger_name: str,
        message: str
    ) -> int:
        """
        Insert a single log record into the database.
        Messages are compressed using zstd if beneficial.
        Returns the inserted row ID.
        """
        collection_name = self.extract_collection_name(logger_name)
        source_type = self.determine_source_type(logger_name)
        compressed_message = self.compress_message(message)
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO logs (timestamp, level, level_no, logger_name, collection_name, source_type, message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (timestamp, level, level_no, logger_name, collection_name, source_type, compressed_message))
        
        conn.commit()
        return cursor.lastrowid
    
    def insert_logs_batch(self, logs: List[Tuple[str, str, int, str, str]]) -> int:
        """
        Insert multiple log records in a batch.
        Each tuple: (timestamp, level, level_no, logger_name, message)
        Messages are compressed using zstd if beneficial.
        Returns the number of inserted rows.
        """
        if not logs:
            return 0
        
        records = [
            (ts, lvl, lvl_no, name, self.extract_collection_name(name), 
             self.determine_source_type(name), self.compress_message(msg))
            for ts, lvl, lvl_no, name, msg in logs
        ]
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.executemany('''
            INSERT INTO logs (timestamp, level, level_no, logger_name, collection_name, source_type, message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', records)
        
        conn.commit()
        return cursor.rowcount
    
    def query_logs(
        self,
        level_filter: Optional[str] = None,
        collection_filter: Optional[str] = None,
        source_type_filter: Optional[str] = None,
        search_text: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_desc: bool = True
    ) -> Dict[str, Any]:
        """
        Query logs with various filters.
        Returns a dict with 'logs', 'total', 'limit', 'offset'.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        where_clauses = []
        params = []
        
        if level_filter:
            where_clauses.append("level = ?")
            params.append(level_filter.upper())
        
        if collection_filter:
            where_clauses.append("collection_name = ?")
            params.append(collection_filter)
        
        if source_type_filter:
            where_clauses.append("source_type = ?")
            params.append(source_type_filter.lower())
        
        if search_text:
            where_clauses.append("message LIKE ?")
            params.append(f"%{search_text}%")
        
        if start_time:
            where_clauses.append("timestamp >= ?")
            params.append(start_time)
        
        if end_time:
            where_clauses.append("timestamp <= ?")
            params.append(end_time)
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        order_dir = "DESC" if order_desc else "ASC"
        
        count_sql = f"SELECT COUNT(*) FROM logs WHERE {where_sql}"
        cursor.execute(count_sql, params)
        total = cursor.fetchone()[0]
        
        query_sql = f'''
            SELECT id, timestamp, level, level_no, logger_name, collection_name, source_type, message
            FROM logs
            WHERE {where_sql}
            ORDER BY timestamp {order_dir}, id {order_dir}
            LIMIT ? OFFSET ?
        '''
        cursor.execute(query_sql, params + [limit, offset])
        
        logs = []
        for row in cursor.fetchall():
            log_dict = dict(row)
            log_dict['message'] = self.decompress_message(log_dict['message'])
            logs.append(log_dict)
        
        return {
            "logs": logs,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": offset + len(logs) < total
        }
    
    def get_source_types(self) -> List[str]:
        """Get list of available source types."""
        return [SOURCE_TYPE_APPLICATION, SOURCE_TYPE_BACKEND]
    
    def get_log_stats(self) -> Dict[str, Any]:
        """
        Get log statistics for dashboard display.
        Returns counts by level and recent activity metrics.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT level, COUNT(*) as count
            FROM logs
            GROUP BY level
        ''')
        level_counts = {row['level']: row['count'] for row in cursor.fetchall()}
        
        cursor.execute('SELECT COUNT(*) FROM logs')
        total_logs = cursor.fetchone()[0]
        
        one_hour_ago = (datetime.utcnow() - timedelta(hours=1)).isoformat()
        cursor.execute('''
            SELECT COUNT(*) FROM logs WHERE timestamp >= ?
        ''', (one_hour_ago,))
        last_hour = cursor.fetchone()[0]
        
        twenty_four_hours_ago = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        cursor.execute('''
            SELECT COUNT(*) FROM logs WHERE timestamp >= ?
        ''', (twenty_four_hours_ago,))
        last_24_hours = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT timestamp FROM logs ORDER BY timestamp DESC LIMIT 1
        ''')
        row = cursor.fetchone()
        last_log_time = row['timestamp'] if row else None
        
        return {
            "total_logs": total_logs,
            "level_counts": level_counts,
            "last_hour": last_hour,
            "last_24_hours": last_24_hours,
            "last_log_time": last_log_time
        }
    
    def get_collections(self) -> List[str]:
        """Get list of unique collection names from logs."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT collection_name
            FROM logs
            WHERE collection_name IS NOT NULL
            ORDER BY collection_name
        ''')
        
        return [row['collection_name'] for row in cursor.fetchall()]
    
    def get_log_levels(self) -> List[str]:
        """Get list of unique log levels from logs."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT level
            FROM logs
            ORDER BY level_no
        ''')
        
        return [row['level'] for row in cursor.fetchall()]
    
    def cleanup_old_logs(self, retention_days: int = 30) -> int:
        """
        Delete logs older than the specified retention period.
        Returns the number of deleted rows.
        """
        cutoff_date = (datetime.utcnow() - timedelta(days=retention_days)).isoformat()
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            DELETE FROM logs WHERE timestamp < ?
        ''', (cutoff_date,))
        
        deleted_count = cursor.rowcount
        conn.commit()
        
        if deleted_count > 0:
            cursor.execute('VACUUM')
        
        return deleted_count
    
    def get_compression_stats(self) -> Dict[str, Any]:
        """
        Get statistics about log compression.
        Returns counts of compressed vs uncompressed messages and estimated savings.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute(f'''
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN message LIKE '{COMPRESSED_PREFIX}%' THEN 1 ELSE 0 END) as compressed,
                SUM(LENGTH(message)) as total_size
            FROM logs
        ''')
        row = cursor.fetchone()
        
        total = row['total'] or 0
        compressed = row['compressed'] or 0
        total_size = row['total_size'] or 0
        
        return {
            "total_logs": total,
            "compressed_logs": compressed,
            "uncompressed_logs": total - compressed,
            "compression_rate": round(compressed / total * 100, 2) if total > 0 else 0,
            "total_storage_bytes": total_size,
            "compression_available": COMPRESSION_AVAILABLE
        }
    
    def compress_existing_logs(self, batch_size: int = 1000) -> Dict[str, int]:
        """
        Compress existing uncompressed log messages.
        Processes in batches to avoid memory issues.
        Returns statistics about the compression operation.
        """
        if not COMPRESSION_AVAILABLE:
            return {"error": "Compression not available", "compressed": 0, "skipped": 0}
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        compressed_count = 0
        skipped_count = 0
        
        while True:
            cursor.execute(f'''
                SELECT id, message FROM logs 
                WHERE message NOT LIKE '{COMPRESSED_PREFIX}%'
                AND LENGTH(message) >= 100
                LIMIT ?
            ''', (batch_size,))
            
            rows = cursor.fetchall()
            if not rows:
                break
            
            for row in rows:
                log_id = row['id']
                original_message = row['message']
                compressed_message = self.compress_message(original_message)
                
                if compressed_message != original_message:
                    cursor.execute('''
                        UPDATE logs SET message = ? WHERE id = ?
                    ''', (compressed_message, log_id))
                    compressed_count += 1
                else:
                    skipped_count += 1
            
            conn.commit()
            if compressed_count > 0:
                logger.debug(f"Compression progress: {compressed_count} compressed, {skipped_count} skipped")
        
        if compressed_count > 0:
            cursor.execute('VACUUM')
            conn.commit()
            logger.debug(f"Compression complete: {compressed_count} logs compressed")
        
        return {
            "compressed": compressed_count,
            "skipped": skipped_count,
            "total_processed": compressed_count + skipped_count
        }
    
    def close(self):
        """Close the thread-local database connection."""
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.close()
            self._local.connection = None


def get_log_database() -> LogDatabase:
    """Get the singleton LogDatabase instance."""
    return LogDatabase()
