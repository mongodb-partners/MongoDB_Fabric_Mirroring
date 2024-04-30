import os
from threading import Thread
import pymongo
from dotenv import load_dotenv
from init_sync import init_sync
from listening import listening


def mirror(
    mongodb_conn_str: str,
    mongodb_db_name: str,
    mongodb_coll_name: str | list[str],
    lz_url: str,
    lz_app_id: str = None,
    lz_secret: str = None,
    lz_tenant_id: str = None,
):
    load_dotenv()
    if not lz_app_id:
        lz_app_id = os.getenv("APP_ID")
    if not lz_secret:
        lz_secret = os.getenv("SECRET")
    if not lz_tenant_id:
        lz_tenant_id = os.getenv("TENANT_ID")
    if (
        not mongodb_conn_str
        or not mongodb_db_name
        or not mongodb_coll_name
        or not lz_url
        or not lz_app_id
        or not lz_secret
        or not lz_tenant_id
    ):
        raise ValueError("Invalid parameter value detected!")

    collection_list = []

    if mongodb_coll_name == "all":
        collection_list = __get_all_collections(mongodb_conn_str, mongodb_db_name)
    elif isinstance(mongodb_coll_name, str):
        collection_list = [mongodb_coll_name]
    elif isinstance(mongodb_coll_name, list):
        collection_list = mongodb_coll_name
    else:
        raise ValueError(
            'Invalid parameter value: mongodb_coll_name. "\
            "Expected a list of collection names, a str of a single collection"\
            " name, or "all" for all collections in the database.'
        )

    threads: list[Thread] = []

    for col in collection_list:
        mongodb_params = {
            "conn_str": mongodb_conn_str,
            "db_name": mongodb_db_name,
            "collection": col,
        }

        lz_params = {
            "url": lz_url,
            "app_id": lz_app_id,
            "secret": lz_secret,
            "tenant_id": lz_tenant_id,
        }

        # init_sync(mongodb_params, lz_params)
        Thread(target=init_sync, args=(mongodb_params, lz_params)).start()
        # init_thread = Thread(target=init_sync, args=(mongodb_params, lz_params))
        # init_thread.start()
        # threads.append(init_thread)

        # listening(mongodb_params, lz_params)
        Thread(target=listening, args=(mongodb_params, lz_params)).start()
        # listener_thread = Thread(target=listening, args=(mongodb_params, lz_params))
        # listener_thread.start()
        # threads.append(listener_thread)

    # for thread in threads:
    #     thread.join()
    while True:
        cmd = input()
        if cmd.lower() == "quit":
            os._exit(0)


def __get_all_collections(conn_str: str, db_name: str) -> list[str]:
    client = pymongo.MongoClient(conn_str)
    db = client[db_name]
    return db.list_collection_names()
