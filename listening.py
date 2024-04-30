import pymongo
import pandas as pd
import time
import logging

from constants import (
    ROW_MARKER_COLUMN_NAME,
    CHANGE_STREAM_OPERATION_MAP,
    CHANGE_STREAM_OPERATION_MAP_WHEN_INIT,
    TYPES_TO_CONVERT_TO_STR,
)
from utils import to_string, get_parquet_filename
from push_file_to_lz import push_file_to_lz
from flags import get_init_flag


MAX_ROWS = 2
TIME_THRESHOLD_IN_SEC = 600

def listening(mongodb_params, lz_params):
    logger = logging.getLogger(f"{__name__}[{mongodb_params["collection"]}]")
    # logger.debug(f"conn_str={mongodb_params["conn_str"]}")
    logger.debug(f"db_name={mongodb_params["db_name"]}")
    logger.debug(f"collection={mongodb_params["collection"]}")
    client = pymongo.MongoClient(mongodb_params["conn_str"])
    db = client[mongodb_params["db_name"]]
    collection = db[mongodb_params["collection"]]
    cursor = collection.watch(full_document='updateLookup')
    
    accumulative_df: pd.DataFrame = None
    last_sync_time: float = time.time()
    
    logger.info("start listening to change stream...")
    for change in cursor:
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
        if get_init_flag(mongodb_params["collection"]):
            logger.debug(f"collection {mongodb_params["collection"]} still initializing, use UPSERT instead of INSERT")
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
        
        if get_init_flag(mongodb_params["collection"]):
            logger.debug(f"collection {mongodb_params["collection"]} is initializing, skip parquet writing...")
            continue
        # write to parquet if accumulative_df is not empty, and:
        #     a. accumulative_df reaches MAX_ROWS, or
        #     b. it has been TIME_THRESHOLD_IN_SEC since last sync
        if (accumulative_df is not None
                and not get_init_flag(mongodb_params["collection"])
                and (accumulative_df.shape[0] >= MAX_ROWS
                    or time.time() - last_sync_time >= TIME_THRESHOLD_IN_SEC)):
            parquet_filename = get_parquet_filename(mongodb_params["collection"])
            logger.debug(f"filename={parquet_filename}")
            accumulative_df.to_parquet(parquet_filename)
            accumulative_df = None
            push_file_to_lz(parquet_filename, lz_params["url"], mongodb_params["collection"], lz_params["app_id"], lz_params["secret"], lz_params["tenant_id"])