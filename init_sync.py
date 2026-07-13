import pprint
import pymongo
from pymongo.collection import Collection
import time
import os
import pandas as pd
import shutil
import logging
import glob
from bson import ObjectId, Decimal128
from bson.timestamp import Timestamp
import pickle
import numpy as np

from constants import (
    TYPES_TO_CONVERT_TO_STR,
    MONGODB_READING_BATCH_SIZE,
    METADATA_FILE_NAME,
    DATA_FILES_PATH,
#    INIT_SYNC_CURRENT_SKIP_FILE_NAME,
# added the two new files to save the initial sync status and last parquet file number
    INIT_SYNC_STATUS_FILE_NAME,
    LAST_PARQUET_FILE_NUMBER,
    INIT_SYNC_LAST_ID_FILE_NAME,
    INIT_SYNC_MAX_ID_FILE_NAME,
    INIT_SYNC_CLUSTER_TIME_FILE_NAME,
)
import schema_utils
from utils import get_parquet_full_path_filename, to_string, get_table_dir
from push_file_to_lz import push_file_to_lz
# not required as now init_sync stat is stored in LZ
#from flags import set_init_flag, clear_init_flag
from file_utils import FileType, read_from_file, write_to_file, delete_file
from mongo_cluster_time import get_cluster_time, start_snapshot_session_at


def init_sync(collection_name: str):
    logger = logging.getLogger(f"{__name__}[{collection_name}]")

    # detect if there's a init_sync_stat file in LZ, and get its value
    init_sync_stat_flag = read_from_file(
        collection_name, INIT_SYNC_STATUS_FILE_NAME, FileType.PICKLE
    )
    if init_sync_stat_flag == "Y":
        logger.info(
            f"init sync for collection {collection_name} has already finished previously. Skipping init sync this time."
        )
        return
    
    # detect if there's a last_id file, and restore last_id from it
    last_id = read_from_file(
        collection_name, INIT_SYNC_LAST_ID_FILE_NAME, FileType.PICKLE
    )
    if (init_sync_stat_flag == "N" and last_id):
        logger.info(
            f"interrupted init sync detected, continuing with previous _id={last_id}"
        )

    logger.info(f"begin init sync for {collection_name}")
    
    # begin by writing init_sync_stat file with "N" as value
    #set_init_flag(collection_name)
    if not init_sync_stat_flag:
        # writing init_sync_stat file with "N"
        init_sync_stat_flag = "N"
        logger.info(f"writing init sync stat file with as 'N' for {collection_name}")
        write_to_file(
            init_sync_stat_flag, collection_name, INIT_SYNC_STATUS_FILE_NAME, FileType.PICKLE
        ) 

    db_name = os.getenv("MONGO_DB_NAME")
    logger.debug(f"db_name={db_name}")
    logger.debug(f"collection={collection_name}")
    enable_perf_timer = os.getenv("DEBUG__ENABLE_PERF_TIMER")

    client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
    db = client[os.getenv("MONGO_DB_NAME")]
    collection = db[collection_name]

    # Capture or restore cluster time N for snapshot reads and change-stream handoff.
    cluster_time = read_from_file(
        collection_name, INIT_SYNC_CLUSTER_TIME_FILE_NAME, FileType.PICKLE
    )
    if isinstance(cluster_time, Timestamp):
        logger.info(f"resumed init cluster time N={cluster_time}")
    else:
        cluster_time = get_cluster_time(client)
        logger.info(f"captured init cluster time N={cluster_time}")
        write_to_file(
            cluster_time,
            collection_name,
            INIT_SYNC_CLUSTER_TIME_FILE_NAME,
            FileType.PICKLE,
        )

    count = collection.estimated_document_count()

    with start_snapshot_session_at(client, cluster_time) as session:
        # use max_id as the mechanism to set the stopping point of init sync
        max_id_from_file = read_from_file(
            collection_name, INIT_SYNC_MAX_ID_FILE_NAME, FileType.PICKLE
        )
        if max_id_from_file:
            max_id = max_id_from_file
            logger.info(f"resumed max_id={max_id}")
        else:
            max_id = __get_max_id(collection, logger, session=session)
            if max_id:
                logger.info(f"writing max_id into file: {max_id}")
                write_to_file(
                    max_id, collection_name, INIT_SYNC_MAX_ID_FILE_NAME, FileType.PICKLE
                )

        batch_size = int(os.getenv("INIT_LOAD_BATCH_SIZE"))

        columns_to_convert_to_str = None

        while last_id is None or (max_id is not None and last_id < max_id):
            # for debug only
            debug_env_var_sleep_sec = os.getenv("DEBUG__INIT_SYNC_SLEEP_SEC")
            if debug_env_var_sleep_sec and debug_env_var_sleep_sec.isnumeric():
                logger.info(f"sleep({debug_env_var_sleep_sec}) begin")
                time.sleep(int(debug_env_var_sleep_sec))
                logger.info(f"sleep({debug_env_var_sleep_sec}) ends")

            if not last_id:
                batch_cursor = (
                    collection.find(session=session).sort({"_id": 1}).limit(batch_size)
                )
            else:
                batch_cursor = (
                    collection.find(
                        {"_id": {"$gt": last_id, "$lte": max_id}},
                        session=session,
                    )
                    .sort({"_id": 1})
                    .limit(batch_size)
                )

            read_start_time = time.time()
            batch_df = pd.DataFrame(list(batch_cursor))

            read_end_time = time.time()
            if enable_perf_timer:
                logger.info(f"TIME: read took {read_end_time-read_start_time:.2f} seconds")

            # quit the loop if no more data
            if batch_df.empty:
                break

            # get the last _id of its original data type ObjectId, before we convert it to string later
            raw_last_id = batch_df["_id"].iloc[-1]
            first_id = batch_df["_id"][0]
            logger.info("starting a new batch.")
            logger.info(f"first _id of this batch: {first_id}")
            logger.info(f"last _id of this batch: {raw_last_id}")

            # process df according to internal schema
            schema_utils.process_dataframe(collection_name, batch_df)

            trans_end_time = time.time()
            if enable_perf_timer:
                logger.info(f"TIME: trans took {trans_end_time-read_end_time:.2f} seconds")

            logger.debug("creating parquet file...")
            # changed to get last parquet file number from LZ for resilience
            #parquet_full_path_filename = get_parquet_full_path_filename(collection_name)
            last_parquet_file_num = read_from_file(
            collection_name, LAST_PARQUET_FILE_NUMBER, FileType.PICKLE
            )
            if not last_parquet_file_num:
               last_parquet_file_num = 0

            parquet_full_path_filename = get_parquet_full_path_filename(collection_name, last_parquet_file_num)
            
            logger.info(f"writing parquet file: {parquet_full_path_filename}")

            schema_utils.finalize_dataframe_for_parquet(collection_name, batch_df)

            # Write the parquet file
            batch_df.to_parquet(parquet_full_path_filename, index=False)
            # os.remove("temp.csv")
            write_end_time = time.time()    
            if enable_perf_timer:
                logger.info(f"TIME: write took {write_end_time-trans_end_time:.2f} seconds")
                
        #>>># changes to remove write metadata.json here as it will now be written as the first file in mongodb_generic_mirroring.py - 6Mar2025
     #      if not last_id:
                # do not copy, but send the template file directly
                # metadata_json_path = os.path.join(
                #     os.path.dirname(os.path.abspath(__file__)), METADATA_FILE_NAME
                # )
                # logger.info("writing metadata file to LZ")
                # push_file_to_lz(metadata_json_path, collection_name)
            # write the current batch to LZ
            push_start_time = time.time()
            logger.info("writing parquet file to LZ")
            push_file_to_lz(parquet_full_path_filename, collection_name)
            push_end_time = time.time()
            if enable_perf_timer:
                logger.info(f"TIME: push took {push_end_time-push_start_time:.2f} seconds")
            last_id = raw_last_id
            logger.debug(f"DATA TYPE OF last_id IS: {type(last_id)}")
            # write current last_id to file
            logger.info(f"writing last_id into file: {last_id}")
            write_to_file(
                last_id, collection_name, INIT_SYNC_LAST_ID_FILE_NAME, FileType.PICKLE
            )
            # write last parquet file number to file
            last_parquet_file_num += 1
            logger.info(f"writing last parquet number into file: {last_parquet_file_num}")
            write_to_file(
                last_parquet_file_num,
                collection_name,
                LAST_PARQUET_FILE_NUMBER,
                FileType.PICKLE,
            )
            #>>># added sleep to ensure that Fabric picks up one file at a time - 14Mar2025
            time.sleep(30)

    # delete last_id file, as init sync is complete
    logger.info("removing the last_id file")
    delete_file(collection_name, INIT_SYNC_LAST_ID_FILE_NAME)

    #set_init_flag_stat as complete = Y
    logger.info("Setting init_sync_stat flag as Y")
    init_sync_stat_flag = "Y"
    write_to_file(
        init_sync_stat_flag, collection_name, INIT_SYNC_STATUS_FILE_NAME, FileType.PICKLE
    )
    logger.info(f"init sync completed for collection {collection_name}")


def __get_max_id(collection: Collection, logger: logging.Logger, session=None):
    pipeline = [{"$sort": {"_id": -1}}, {"$limit": 1}, {"$project": {"_id": 1}}]
    result = collection.aggregate(pipeline, session=session)
    try:
        doc = result.next()
        max_id = doc["_id"]
        return max_id
    except StopIteration:
        logger.warning(f"Can't get max _id, empty or non-exists collection.")
        return None
