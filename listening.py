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
)
from utils import to_string, get_parquet_full_path_filename, get_table_dir
from push_file_to_lz import push_file_to_lz
from flags import get_init_flag
from init_sync import init_sync


# MAX_ROWS = 1
TIME_THRESHOLD_IN_SEC = 600

def listening(collection_name: str):
    logger = logging.getLogger(f"{__name__}[{collection_name}]")
    # logger.debug(f"conn_str={mongodb_params["conn_str"]}")
    logger.debug(f"db_name={os.getenv("MONGO_DB_NAME")}")
    logger.debug(f"collection={collection_name}")
    post_init_flush_done = False
    
    table_dir = get_table_dir(collection_name)
    resume_token_file_full_path = os.path.join(table_dir, DELTA_SYNC_RESUME_TOKEN_FILE_NAME)
    resume_token = None
    if os.path.exists(resume_token_file_full_path):
        with open(resume_token_file_full_path, "rb") as resume_token_file:
            resume_token = pickle.load(resume_token_file)
            logger.info(f"interrupted incremental sync detected, continuing with resume_token={resume_token}")
    
    client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
    db = client[os.getenv("MONGO_DB_NAME")]
    collection = db[collection_name]
    cursor = collection.watch(full_document='updateLookup', resume_after=resume_token)
    
    
    cache_parquet_full_path = os.path.join(table_dir, DELTA_SYNC_CACHE_PARQUET_FILE_NAME)
    cached_change_count = 0
    if os.path.exists(cache_parquet_full_path):
        # cache exists, needs to restore cache count
        cached_change_count = len(pd.read_parquet(cache_parquet_full_path))
        
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
        else: # insert or update
            doc: dict = change["fullDocument"]
        df = pd.DataFrame([doc])
        if init_flag:
            logger.debug(f"collection {collection_name} still initializing, use UPSERT instead of INSERT")
            row_marker_value = CHANGE_STREAM_OPERATION_MAP_WHEN_INIT[operationType]
        else:
            row_marker_value = CHANGE_STREAM_OPERATION_MAP[operationType]
        df.insert(0, ROW_MARKER_COLUMN_NAME, [row_marker_value])
        # logger.debug("constructed pandas DataFrame:")
        # logger.debug(df)
        # logger.debug("pandas DataFrame schema:")
        # logger.debug(df.dtypes)
        
        # data type conversion
        for key in df.keys():
            first_item = df[key][0]
            data_type = type(first_item)
            # logger.debug(f"key: {key}")
            # logger.debug(f"data_type: {data_type}")
            if any(isinstance(first_item, t) for t in TYPES_TO_CONVERT_TO_STR):
                df[key] = df[key].apply(to_string)
            # fix of the "Date" data type from MongoDB. Now it will become "datetime2" in Fabric
            if df[key].dtype == "datetime64[ns]":
                logger.debug("trying to convert datetime column...")
                df[key] = df[key].astype("datetime64[ms]")
            # remove spaces in key/column name
            if " " in key:
                df.rename(columns={key: key.replace(" ", "_")}, inplace=True)
            
            # truncate column name if longer than 128
            if len(key) > 128:
                df.rename(columns={key: key[:128]}, inplace=True)
        
        # logger.debug("pandas DataFrame schema after conversion:")
        # logger.debug(df.dtypes)
        
        # INCREMENTAL SYNC ANTI-CRASH REFACTORING
        # instead of accumulating to df, accumulating directly to parquet file, like accumulation.parquet
        df.to_parquet(cache_parquet_full_path, index=False, engine="fastparquet", append=os.path.exists(cache_parquet_full_path))
        cached_change_count += 1
        
        # wirte _id of current change as resume token to file, after successful
        #     write current change to cache parquet
        resume_token = change["_id"]
        with open(resume_token_file_full_path, "wb") as resume_token_file:
            logger.info(f"writing resume_token into file: {resume_token}")
            pickle.dump(resume_token, resume_token_file)
        
        # Always rename chached parquet if exists
        # But, if the collection is initializing, rename cache parquet file with
        # a prefix, and do not push it to LZ
        if (os.path.exists(cache_parquet_full_path)
                and cached_change_count >= int(os.getenv("DELTA_SYNC_BATCH_SIZE"))
            ):
            prefix = ""
            if init_flag:
                prefix=TEMP_PREFIX_DURING_INIT
            parquet_full_path_filename = get_parquet_full_path_filename(collection_name, prefix=prefix)
            logger.info(f"renaming caching parquet file to: {parquet_full_path_filename}")
            os.rename(cache_parquet_full_path, parquet_full_path_filename)
            if not init_flag:
                push_file_to_lz(parquet_full_path_filename, collection_name)
            cached_change_count = 0
            # from June-14 meeting with Vasanth Kumar:
            # TODO: new location to update resume token


def __post_init_flush(table_name: str, logger):
    if not logger:
        logger = logging.getLogger(f"{__name__}[{table_name}]")
    logger.info(f"begin post init flush of delta change for collection {table_name}")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    table_dir = get_table_dir(table_name)
    if not os.path.exists(table_dir):
        return
    temp_parquet_filename_list = sorted([
        filename
        for filename in os.listdir(table_dir)
        if os.path.splitext(filename)[1] == ".parquet"
        and os.path.splitext(filename)[0].startswith(TEMP_PREFIX_DURING_INIT)
    ])
    for temp_parquet_filename in temp_parquet_filename_list:
        temp_parquet_full_path = os.path.join(table_dir, temp_parquet_filename)
        new_parquet_full_path = get_parquet_full_path_filename(table_name)
        logger.debug("renaming temp parquet file")
        logger.debug(f"old name: {temp_parquet_full_path}")
        logger.debug(f"new name: {new_parquet_full_path}")
        logger.info(f"renaming parquet file from {temp_parquet_full_path} to {new_parquet_full_path}")
        os.rename(temp_parquet_full_path, new_parquet_full_path)
        push_file_to_lz(new_parquet_full_path, table_name)

