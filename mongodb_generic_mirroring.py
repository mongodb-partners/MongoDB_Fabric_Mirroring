import logging.handlers
import os
import logging
import threading
from threading import Thread
import traceback
import pymongo
from pymongo.errors import ServerSelectionTimeoutError
from dotenv import load_dotenv
import json

from init_sync import init_sync
from listening import listening
from schema_utils import init_table_schema
from constants import (
    METADATA_FILE_NAME,
)
from push_file_to_lz import push_file_to_lz
from file_utils import FileType, read_from_file
from sqlite_log_handler import create_sqlite_handler, BackendLogFilter
from log_database import get_log_database

def mirror():
    load_dotenv()
    log_format_os = os.getenv("APP_LOG_LEVEL")
    print(f"log_level before getlevels =={log_format_os}")
    # changed to _nameToLevel as getLevelNamesMapping is available from python 3.11
    #log_level = logging.getLevelNamesMapping().get(log_format_os, logging.INFO)
    log_level = logging._nameToLevel.get(log_format_os, logging.INFO)
    #Display Log level set
    print(f"log_level set ={log_level}")
    log_format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=log_level, format=log_format_str)
    root_logger = logging.getLogger()
    logging_formatter = logging.Formatter(log_format_str)
    #Changed to rotate logs
    #file_handler = logging.FileHandler("mirroring.log")
    file_handler = logging.handlers.RotatingFileHandler('mirroring.log', maxBytes=50*1024*1024, backupCount=5)

    file_handler.setFormatter(logging_formatter)
    root_logger.addHandler(file_handler)

    log_backend = os.getenv("LOG_BACKEND_LOGS", "false").lower() == "true"
    
    if not log_backend:
        backend_filter = BackendLogFilter()
        for handler in root_logger.handlers:
            handler.addFilter(backend_filter)
        file_handler.addFilter(backend_filter)
        print("Backend/framework logs filtered from console and file output")
    
    def thread_exception_handler(args):
        """Handle uncaught exceptions in threads and log them properly."""
        thread_logger = logging.getLogger("thread_exception")
        exc_info = (args.exc_type, args.exc_value, args.exc_traceback)
        thread_name = args.thread.name if args.thread else "Unknown"
        thread_logger.error(
            f"Unhandled exception in thread '{thread_name}': {args.exc_value}",
            exc_info=exc_info
        )
        formatted_tb = ''.join(traceback.format_exception(*exc_info))
        thread_logger.error(f"Full traceback:\n{formatted_tb}")
    
    threading.excepthook = thread_exception_handler
    print("Thread exception handler installed")

    if os.getenv("LOG_TO_SQLITE", "true").lower() == "true":
        sqlite_handler = create_sqlite_handler(
            level=log_level,
            format_str=log_format_str,
            batch_size=50,
            flush_interval=5.0,
            log_backend=log_backend
        )
        root_logger.addHandler(sqlite_handler)
        print(f"SQLite logging enabled (backend logs: {'enabled' if log_backend else 'disabled'})")
        
        retention_days = int(os.getenv("LOG_RETENTION_DAYS", "30"))
        db = get_log_database()
        deleted = db.cleanup_old_logs(retention_days)
        if deleted > 0:
            print(f"Cleaned up {deleted} old log entries (older than {retention_days} days)")
        
        # Compress existing uncompressed logs on startup
        if os.getenv("LOG_COMPRESS_ON_STARTUP", "true").lower() == "true":
            compress_result = db.compress_existing_logs(batch_size=1000)
            if compress_result.get("compressed", 0) > 0:
                print(f"Compressed {compress_result['compressed']} existing log entries")

    logger = logging.getLogger(__name__)
    if (
        not os.getenv("MONGO_CONN_STR")
        or not os.getenv("MONGO_DB_NAME")
        or not os.getenv("MONGO_COLLECTION")
        or not os.getenv("LZ_URL")
        or not os.getenv("APP_ID")
        or not os.getenv("SECRET")
        or not os.getenv("TENANT_ID")
        or not os.getenv("INIT_LOAD_BATCH_SIZE")
        or not os.getenv("DELTA_SYNC_BATCH_SIZE")
    # added threshold time    
        or not os.getenv("TIME_THRESHOLD_IN_SEC")
    ):
        raise ValueError("Missing environment variable.")

    mongodb_coll_name = os.getenv("MONGO_COLLECTION")
    collection_list = []
    all_collections = __get_all_collections()
    if mongodb_coll_name == "all":
        collection_list = all_collections
    elif mongodb_coll_name.startswith("["):
        collection_list = json.loads(mongodb_coll_name)
    elif isinstance(mongodb_coll_name, str):
        collection_list = [mongodb_coll_name]
    else:
        raise ValueError(
            'Invalid parameter value: mongodb_coll_name. "\
            "Expected a list of collection names, a str of a single collection"\
            " name, or "all" for all collections in the database.'
        )

    # threads: list[Thread] = []

    # remove non-exists collections
    removed_collections = []
    collection_list = [
        item
        for item in collection_list
        if item in all_collections or removed_collections.append(item) is None
    ]
    for non_exists_collection in removed_collections:
        logger.warning(f"removed non-exists collection {non_exists_collection}")

    for collection_name in collection_list:
    #>>># changes to write metadata.json a the first file - 6Mar2025
        metadata_file_exists = read_from_file(
            collection_name, METADATA_FILE_NAME, FileType.TEXT
        )
        if not metadata_file_exists: 
            metadata_json_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), METADATA_FILE_NAME
                )
            logger.info("writing metadata file to LZ")
            push_file_to_lz(metadata_json_path, collection_name)

        init_table_schema(collection_name)

        Thread(target=listening, args=(collection_name,)).start()
        # listener_thread = Thread(target=listening, args=(collection_name,))
        # listener_thread.start()
        # threads.append(listener_thread)

        # Moved the starting of init_sync to listening so as not to miss any records which may come by the time we start init_sync
        # Thread(target=init_sync, args=(collection_name,)).start()
        # init_thread = Thread(target=init_sync, args=(collection_name,))
        # init_thread.start()
        # threads.append(init_thread)

    # for thread in threads:
    #     thread.join()
    while True:
        cmd = input()
        if cmd.lower() == "quit":
            os._exit(0)


def __get_all_collections() -> list[str]:
    client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
    # check database existence
    db_name = os.getenv("MONGO_DB_NAME")
    print(f"db_name={db_name}")
    try:
        all_db_names = client.list_database_names()
        if db_name not in all_db_names:
            raise ValueError(f"Database name provided do not exists: {db_name}")
        db = client[db_name]
        return db.list_collection_names()
    except ServerSelectionTimeoutError:
        raise ValueError("Can not connect to MongoDB with the provided MONGO_CONN_STR.")


if __name__ == "__main__":
    mirror()
