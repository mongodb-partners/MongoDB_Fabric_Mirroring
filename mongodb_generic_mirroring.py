import logging.handlers
import os
import logging
from threading import Event, Thread
import pymongo
from pymongo.errors import ServerSelectionTimeoutError
from dotenv import load_dotenv
import json

from init_sync import init_sync
from listening import listening
from schema_utils import init_table_schema
from constants import (
    METADATA_FILE_NAME,
    PARTNER_EVENTS_FILE_NAME,
    APP_VERSION,
)
from push_file_to_lz import push_file_to_lz, get_file_from_lz_root, push_file_to_lz_root
from file_utils import FileType, read_from_file

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

    logger = logging.getLogger(__name__)
    logger.info("MongoDB Fabric Mirroring starting, version=%s", APP_VERSION)
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

    __ensure_partner_events_in_lz(logger)

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

        try:
            init_table_schema(collection_name)
        except Exception:
            logger.exception(
                "schema bootstrap failed for collection %s; continuing with init sync",
                collection_name,
            )

        try:
            init_sync(collection_name)
        except Exception:
            logger.exception(
                "init sync failed for collection %s; skipping change stream listening",
                collection_name,
            )
            continue

        Thread(target=listening, args=(collection_name,)).start()

    # Keep the mirror thread alive for App Service / non-interactive hosts.
    # Do not use input(): stdin is closed there and raises EOFError.
    Event().wait()


# def __get_all_collections() -> list[str]:
#     client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
#     # check database existence
#     db_name = os.getenv("MONGO_DB_NAME")
#     print(f"db_name={db_name}")
#     try:
#         all_db_names = client.list_database_names()
#         if db_name not in all_db_names:
#             raise ValueError(f"Database name provided do not exists: {db_name}")
#         db = client[db_name]
#         return db.list_collection_names()
#     except ServerSelectionTimeoutError:
#         raise ValueError("Can not connect to MongoDB with the provided MONGO_CONN_STR.")
    

# New class:
def __ensure_partner_events_in_lz(logger: logging.Logger) -> None:
    """
    Write _partnerEvents.json once at the landing zone root (mirrored database level).
    Per Microsoft open mirroring, this file is not per-table and should not include
    a single collection name.
    """
    response_status_code, _ = get_file_from_lz_root(PARTNER_EVENTS_FILE_NAME)
    if response_status_code == 200:
        logger.info("_partnerEvents.json already present at landing zone root")
        return

    app_dir = os.path.dirname(os.path.abspath(__file__))
    partner_events_template_path = os.path.join(app_dir, "_partnerEvents_template.json")
    partner_events_output_path = os.path.join(app_dir, PARTNER_EVENTS_FILE_NAME)
    logger.info("writing _partnerEvents.json to landing zone root")
    with open(partner_events_template_path, "r", encoding="utf-8") as template_file:
        partner_events_content = template_file.read()
    partner_events_content = partner_events_content.replace(
        "${MONGO_DB_NAME}", os.getenv("MONGO_DB_NAME", "")
    ).replace("${LZ_URL}", os.getenv("LZ_URL", ""))
    with open(partner_events_output_path, "w", encoding="utf-8") as output_file:
        output_file.write(partner_events_content)
    push_file_to_lz_root(partner_events_output_path)


def __get_all_collections() -> list[str]:
    client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
    # check database existence
    db_name = os.getenv("MONGO_DB_NAME")
    print(f"db_name={db_name}")
    try:
        all_db_names = [db["name"] for db in client.list_databases(nameOnly=True,authorizedDatabases=True,)]
        if db_name not in all_db_names:
            raise ValueError(f"Database name provided do not exists: {db_name}")
        db = client[db_name]
        result = db.command({"listCollections": 1,"nameOnly": True,"authorizedCollections": True,"cursor": {},})
        collection_names = [doc["name"] for doc in result["cursor"]["firstBatch"]]
        return collection_names
    except ServerSelectionTimeoutError:
        raise ValueError("Can not connect to MongoDB with the provided MONGO_CONN_STR.")


if __name__ == "__main__":
    mirror()
