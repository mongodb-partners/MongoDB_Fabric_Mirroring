import pymongo
import pandas as pd
import time
import logging
import os

from constants import (
    ROW_MARKER_COLUMN_NAME,
    CHANGE_STREAM_OPERATION_MAP,
    CHANGE_STREAM_OPERATION_MAP_WHEN_INIT,
    TYPES_TO_CONVERT_TO_STR,
    TEMP_PREFIX_DURING_INIT,
    DATA_FILES_PATH,
)
from utils import to_string, get_parquet_full_path_filename
from push_file_to_lz import push_file_to_lz
from flags import get_init_flag


# MAX_ROWS = 1
TIME_THRESHOLD_IN_SEC = 600

def listening(collection_name: str):
    logger = logging.getLogger(f"{__name__}[{collection_name}]")
    # logger.debug(f"conn_str={mongodb_params["conn_str"]}")
    logger.debug(f"db_name={os.getenv("MONGO_DB_NAME")}")
    logger.debug(f"collection={collection_name}")
    post_init_flush_done = False
    client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
    db = client[os.getenv("MONGO_DB_NAME")]
    collection = db[collection_name]
    cursor = collection.watch(full_document='updateLookup')
    
    accumulative_df: pd.DataFrame = None
    last_sync_time: float = time.time()
    
    logger.info(f"start listening to change stream for collection {collection_name}")
    for change in cursor:
        init_flag = get_init_flag(collection_name)
        # do post init flush if this is the first iteration after init is done
        if not init_flag and not post_init_flush_done:
            __post_init_flush(collection_name, logger)
            post_init_flush_done = True
        if accumulative_df is None:
            last_sync_time: float = time.time()
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
        
        # merge the df to accumulative_df
        if accumulative_df is not None:
            accumulative_df = pd.concat([accumulative_df, df], ignore_index=True)
            logger.debug("concat accumulative_df result:")
            logger.debug(accumulative_df)
        else:
            logger.debug("df created")
            accumulative_df = df
        
        # Always write to parquet if accumulative_df is not empty
        # But, if the collection is initializing, write parquet file with a
        # prefix, and do not push it to LZ
        if (accumulative_df is not None
                and (accumulative_df.shape[0] >= int(os.getenv("DELTA_SYNC_BATCH_SIZE"))
                    or time.time() - last_sync_time >= TIME_THRESHOLD_IN_SEC
                    )
            ):
            prefix = ""
            if init_flag:
                prefix=TEMP_PREFIX_DURING_INIT
            parquet_full_path_filename = get_parquet_full_path_filename(collection_name, prefix=prefix)
            logger.info(f"writing parquet file: {parquet_full_path_filename}")
            accumulative_df.to_parquet(parquet_full_path_filename)
            accumulative_df = None
            if not init_flag:
                push_file_to_lz(parquet_full_path_filename, collection_name)


def __post_init_flush(table_name: str, logger):
    if not logger:
        logger = logging.getLogger(f"{__name__}[{table_name}]")
    logger.info(f"begin post init flush of delta change for collection {table_name}")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    table_dir = os.path.join(current_dir, DATA_FILES_PATH, table_name + os.sep)
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

