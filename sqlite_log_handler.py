import logging
import os
import queue
import threading
import atexit
from datetime import datetime
from typing import Optional

from log_database import get_log_database, BACKEND_LOGGER_PREFIXES


class BackendLogFilter(logging.Filter):
    """
    A logging filter that removes backend/framework log messages.
    Can be applied to any handler (console, file, etc.).
    """
    
    def __init__(self, name: str = ''):
        super().__init__(name)
    
    def filter(self, record: logging.LogRecord) -> bool:
        """
        Return False to filter out the record (don't log it).
        Returns True for application logs, False for backend logs.
        """
        logger_lower = record.name.lower()
        base_logger = logger_lower.split('[')[0].split('.')[0]
        
        if base_logger in BACKEND_LOGGER_PREFIXES or logger_lower.startswith(BACKEND_LOGGER_PREFIXES):
            return False
        return True


class SQLiteLogHandler(logging.Handler):
    """
    A logging handler that writes log records to SQLite database.
    Uses a background thread with queue-based async writing to avoid blocking.
    """
    
    def __init__(
        self,
        batch_size: int = 50,
        flush_interval: float = 5.0,
        level: int = logging.NOTSET,
        log_backend: bool = False
    ):
        """
        Initialize the SQLite log handler.
        
        Args:
            batch_size: Number of records to batch before writing
            flush_interval: Maximum seconds between flushes
            level: Minimum logging level to handle
            log_backend: Whether to log backend/framework logs (default: False)
        """
        super().__init__(level)
        
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.log_backend = log_backend
        
        self._queue: queue.Queue = queue.Queue()
        self._shutdown = threading.Event()
        self._flush_event = threading.Event()
        
        self._worker_thread = threading.Thread(
            target=self._worker,
            daemon=True,
            name="SQLiteLogHandler-Worker"
        )
        self._worker_thread.start()
        
        atexit.register(self.close)
    
    def _is_backend_logger(self, logger_name: str) -> bool:
        """Check if the logger is from a backend/framework module."""
        logger_lower = logger_name.lower()
        base_logger = logger_lower.split('[')[0].split('.')[0]
        return base_logger in BACKEND_LOGGER_PREFIXES or logger_lower.startswith(BACKEND_LOGGER_PREFIXES)
    
    def emit(self, record: logging.LogRecord):
        """
        Emit a log record by adding it to the queue.
        This method is called by the logging framework.
        """
        try:
            if self._shutdown.is_set():
                return
            
            if not self.log_backend and self._is_backend_logger(record.name):
                return
            
            timestamp = datetime.utcfromtimestamp(record.created).isoformat()
            
            log_entry = (
                timestamp,
                record.levelname,
                record.levelno,
                record.name,
                self.format(record) if self.formatter else record.getMessage()
            )
            
            self._queue.put_nowait(log_entry)
            
        except Exception:
            self.handleError(record)
    
    def _worker(self):
        """Background worker that batches and writes log records to SQLite."""
        batch = []
        
        while not self._shutdown.is_set():
            try:
                try:
                    log_entry = self._queue.get(timeout=self.flush_interval)
                    batch.append(log_entry)
                    self._queue.task_done()
                except queue.Empty:
                    pass
                
                while not self._queue.empty() and len(batch) < self.batch_size:
                    try:
                        log_entry = self._queue.get_nowait()
                        batch.append(log_entry)
                        self._queue.task_done()
                    except queue.Empty:
                        break
                
                should_flush = (
                    len(batch) >= self.batch_size or
                    self._flush_event.is_set() or
                    (batch and self._queue.empty())
                )
                
                if batch and should_flush:
                    self._write_batch(batch)
                    batch = []
                    self._flush_event.clear()
                    
            except Exception as e:
                import sys
                print(f"SQLiteLogHandler worker error: {e}", file=sys.stderr)
        
        if batch:
            self._write_batch(batch)
        
        while not self._queue.empty():
            remaining = []
            while not self._queue.empty():
                try:
                    log_entry = self._queue.get_nowait()
                    remaining.append(log_entry)
                    self._queue.task_done()
                except queue.Empty:
                    break
            if remaining:
                self._write_batch(remaining)
    
    def _write_batch(self, batch):
        """Write a batch of log records to the database."""
        try:
            db = get_log_database()
            db.insert_logs_batch(batch)
        except Exception as e:
            import sys
            print(f"SQLiteLogHandler write error: {e}", file=sys.stderr)
    
    def flush(self):
        """Force flush of any pending log records."""
        self._flush_event.set()
        try:
            self._queue.join()
        except Exception:
            pass
    
    def close(self):
        """
        Close the handler and ensure all pending records are written.
        """
        if self._shutdown.is_set():
            return
        
        self._shutdown.set()
        self._flush_event.set()
        
        if self._worker_thread.is_alive():
            self._worker_thread.join(timeout=10.0)
        
        super().close()


def create_sqlite_handler(
    level: int = logging.DEBUG,
    format_str: Optional[str] = None,
    batch_size: int = 50,
    flush_interval: float = 5.0,
    log_backend: Optional[bool] = None
) -> SQLiteLogHandler:
    """
    Factory function to create a configured SQLiteLogHandler.
    
    Args:
        level: Minimum logging level
        format_str: Log format string (optional)
        batch_size: Number of records to batch
        flush_interval: Seconds between flushes
        log_backend: Whether to log backend/framework logs (default from env var LOG_BACKEND_LOGS)
    
    Returns:
        Configured SQLiteLogHandler instance
    """
    if log_backend is None:
        log_backend = os.getenv("LOG_BACKEND_LOGS", "false").lower() == "true"
    
    handler = SQLiteLogHandler(
        batch_size=batch_size,
        flush_interval=flush_interval,
        level=level,
        log_backend=log_backend
    )
    
    if format_str:
        formatter = logging.Formatter(format_str)
        handler.setFormatter(formatter)
    
    return handler
