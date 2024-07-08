import os
import logging
from threading import Thread
import pymongo
from dotenv import load_dotenv
import json

from init_sync import init_sync
from listening import listening


def mirror():
    load_dotenv()
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(level=log_level)
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
    if mongodb_coll_name == "all":
        collection_list = __get_all_collections()
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

    threads: list[Thread] = []

    for col in collection_list:
        # TODO: find a better way to ensure Change Stream monitoring starts before init sync
        Thread(target=listening, args=(col,)).start()
        # listener_thread = Thread(target=listening, args=(col,))
        # listener_thread.start()
        # threads.append(listener_thread)

        # Moved the starting of init_sync to listening
        # Thread(target=init_sync, args=(col,)).start()
        # init_thread = Thread(target=init_sync, args=(col,))
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
    db = client[os.getenv("MONGO_DB_NAME")]
    return db.list_collection_names()


if __name__ == "__main__":
    mirror()
