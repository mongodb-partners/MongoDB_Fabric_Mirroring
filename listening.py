import pymongo
import pandas as pd
import time
import logging
import os
import pickle
from threading import Thread

from constants import (
    ROW_MARKER_COLUMN_NAME,
    CHANGE_STREAM_OPERATION_MAP,
    CHANGE_STREAM_OPERATION_MAP_WHEN_INIT,
    TYPES_TO_CONVERT_TO_STR,
    TEMP_PREFIX_DURING_INIT,
    DATA_FILES_PATH,
    DELTA_SYNC_CACHE_PARQUET_FILE_NAME,
    DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
    DTYPE_KEY,
    TYPE_KEY,
)
from utils import to_string, get_parquet_full_path_filename, get_table_dir
from push_file_to_lz import push_file_to_lz
from flags import get_init_flag
from init_sync import init_sync
import schemas
import schema_utils
from file_utils import FileType, read_from_file, write_to_file


# MAX_ROWS = 1
# Time is an env variable now
#TIME_THRESHOLD_IN_SEC = 300
time_threshold_in_sec = float(os.getenv("TIME_THRESHOLD_IN_SEC"))
#print(f"The type of time_threshold_in_sec is: {type(time_threshold_in_sec)}")

def listening(collection_name: str):
    logger = logging.getLogger(f"{__name__}[{collection_name}]")
    db_name = os.getenv("MONGO_DB_NAME")
    logger.debug(f"db_name={db_name}")
    logger.debug(f"collection={collection_name}")
    post_init_flush_done = False

    table_dir = get_table_dir(collection_name)
    resume_token = read_from_file(
        collection_name, DELTA_SYNC_RESUME_TOKEN_FILE_NAME, FileType.PICKLE
    )
    if resume_token:
        logger.info(
            f"interrupted incremental sync detected, continuing with resume_token={resume_token}"
        )

    client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
    db = client[db_name]
    collection = db[collection_name]
    cursor = collection.watch(full_document="updateLookup", resume_after=resume_token)

    # use df  - enables variable schemas
    # and consistent as resume_token is updated when file is pushed to LZ

    accumulative_df: pd.DataFrame = None
    # start init sync after we get cursor from Change Stream
    Thread(target=init_sync, args=(collection_name,)).start()

    logger.info(f"start listening to change stream for collection {collection_name}")
    for change in cursor:
        init_flag = get_init_flag(collection_name)
        # do post init flush if this is the first iteration after init is done
        if not init_flag and not post_init_flush_done:
            __post_init_flush(collection_name, logger)
            post_init_flush_done = True
        # logger.debug(type(change))
        logger.debug("original change from Change Stream:")
        logger.debug(change)
        operationType = change["operationType"]
        if operationType not in CHANGE_STREAM_OPERATION_MAP:
            logger.error(f"ERROR: unsupported operation found: {operationType}")
            continue
        if operationType == "delete":
            doc: dict = change["documentKey"]
        else:  # insert or update
            doc: dict = change["fullDocument"]
        df = pd.DataFrame([doc])

        # process df according to internal schema
        schema_utils.process_dataframe(collection_name, df)

        if init_flag:
            logger.debug(
                f"collection {collection_name} still initializing, use UPSERT instead of INSERT"
            )
            row_marker_value = CHANGE_STREAM_OPERATION_MAP_WHEN_INIT[operationType]
        else:
            row_marker_value = CHANGE_STREAM_OPERATION_MAP[operationType]
        df.insert(0, ROW_MARKER_COLUMN_NAME, [row_marker_value])


        # merge the df to accumulative_df till batch size reached
        if accumulative_df is not None:
            accumulative_df = pd.concat([accumulative_df, df], ignore_index=True)
            logger.info("concat accumulative_df result:")
            logger.info(accumulative_df)
        else:
            logger.info("df created")
            accumulative_df = df
            last_sync_time: float = time.time()
            logger.info(f"last_sync_time when first record added: {last_sync_time}")


        if init_flag:
            if (accumulative_df is not None
                and (
                      (accumulative_df.shape[0] >= int(os.getenv("DELTA_SYNC_BATCH_SIZE")))
                )
            ):
                prefix = TEMP_PREFIX_DURING_INIT
                parquet_full_path_filename = get_parquet_full_path_filename(
                collection_name, prefix=prefix
                )
  
                logger.info(f"writing parquet file: {parquet_full_path_filename}")
                accumulative_df.to_parquet(parquet_full_path_filename)
                accumulative_df = None

        if not init_flag:
            if (accumulative_df is not None
                and (
                      (accumulative_df.shape[0] >= int(os.getenv("DELTA_SYNC_BATCH_SIZE")))
#                      or (time.time() - last_sync_time >= TIME_THRESHOLD_IN_SEC)
                       or ((time.time() - last_sync_time) >= time_threshold_in_sec)
                )
            ):
                prefix = ""
                parquet_full_path_filename = get_parquet_full_path_filename(
                collection_name, prefix=prefix
               )
                logger.info(f"writing parquet file: {parquet_full_path_filename}")
                accumulative_df.to_parquet(parquet_full_path_filename)
                accumulative_df = None

                push_file_to_lz(parquet_full_path_filename, collection_name)
                # wirte _id of current change as resume token to file, after successful
                #     push parquet to lz
                resume_token = change["_id"]
                logger.info(f"writing resume_token into file: {resume_token}")
                write_to_file(
                    resume_token,
                    collection_name,
                    DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
                    FileType.PICKLE,
                )


def __post_init_flush(table_name: str, logger):
    if not logger:
        logger = logging.getLogger(f"{__name__}[{table_name}]")
    logger.info(f"begin post init flush of delta change for collection {table_name}")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    table_dir = get_table_dir(table_name)
    if not os.path.exists(table_dir):
        return
    temp_parquet_filename_list = sorted(
        [
            filename
            for filename in os.listdir(table_dir)
            if os.path.splitext(filename)[1] == ".parquet"
            and os.path.splitext(filename)[0].startswith(TEMP_PREFIX_DURING_INIT)
        ]
    )
    for temp_parquet_filename in temp_parquet_filename_list:
        temp_parquet_full_path = os.path.join(table_dir, temp_parquet_filename)
        new_parquet_full_path = get_parquet_full_path_filename(table_name)
        logger.debug("renaming temp parquet file")
        logger.debug(f"old name: {temp_parquet_full_path}")
        logger.debug(f"new name: {new_parquet_full_path}")
        logger.info(
            f"renaming parquet file from {temp_parquet_full_path} to {new_parquet_full_path}"
        )
        os.rename(temp_parquet_full_path, new_parquet_full_path)
        push_file_to_lz(new_parquet_full_path, table_name)
