import logging.handlers
import os
import logging
from threading import Thread
import pymongo
from pymongo.errors import ServerSelectionTimeoutError
from dotenv import load_dotenv
import json

from init_sync import init_sync
from listening import listening
from schema_utils import init_table_schema


def mirror():
    load_dotenv()
    log_format_os = os.getenv("APP_LOG_LEVEL")
    # print(f"log_level before getlevels =={log_format_os}")
    #log_level = logging.getLevelNamesMapping().get(log_format_os, logging.INFO)
    log_level = logging._nameToLevel.get(log_format_os, logging.INFO)
    #diana-added to check why log level not set
    print(f"log_level set ={log_level}")
    log_format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=log_level, format=log_format_str)
    root_logger = logging.getLogger()
    logging_formatter = logging.Formatter(log_format_str)
    #diana -changed to rotate logs
    #file_handler = logging.FileHandler("mirroring.log")
    file_handler = logging.handlers.RotatingFileHandler('mirroring.log', maxBytes=50*1024*1024, backupCount=5)

    file_handler.setFormatter(logging_formatter)
    root_logger.addHandler(file_handler)

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

        init_table_schema(collection_name)

        Thread(target=listening, args=(collection_name,)).start()
        # listener_thread = Thread(target=listening, args=(collection_name,))
        # listener_thread.start()
        # threads.append(listener_thread)

        # Moved the starting of init_sync to listening
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
