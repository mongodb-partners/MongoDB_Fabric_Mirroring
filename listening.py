import pymongo
import pandas as pd
import time

from constants import (
    ROW_MARKER_COLUMN_NAME,
    CHANGE_STREAM_OPERATION_MAP,
    TYPES_TO_CONVERT_TO_STR,
)
from utils import to_string, get_parquet_filename
from push_file_to_lz import push_file_to_lz

MAX_ROWS = 3
TIME_THRESHOLD_IN_SEC = 600


def listening(mongodb_params, lz_params):
    client = pymongo.MongoClient(mongodb_params["conn_str"])
    db = client[mongodb_params["db_name"]]
    collection = db[mongodb_params["collection"]]
    cursor = collection.watch(full_document='updateLookup')
    
    accumulative_df: pd.DataFrame = None
    last_sync_time: float = time.time()
    
    print("start listening to change stream...")
    for change in cursor:
        if accumulative_df is None:
            last_sync_time: float = time.time()
        # print(type(change))
        print("original change from Change Stream:")
        print(change)
        operationType = change["operationType"]
        if operationType not in CHANGE_STREAM_OPERATION_MAP:
            print(f"ERROR: unsupported operation found: {operationType}")
            continue
        if operationType == "delete":
            doc: dict = change["documentKey"]
        else: # insert or update
            doc: dict = change["fullDocument"]
        df = pd.DataFrame([doc])
        row_marker_value = CHANGE_STREAM_OPERATION_MAP[operationType]
        df.insert(0, ROW_MARKER_COLUMN_NAME, [row_marker_value])
        # print("constructed pandas DataFrame:")
        # print(df)
        # print("pandas DataFrame schema:")
        # print(df.dtypes)
        
        # data type conversion
        for key in df.keys():
            first_item = df[key][0]
            data_type = type(first_item)
            # print(f"key: {key}")
            # print(f"data_type: {data_type}")
            if any(isinstance(first_item, t) for t in TYPES_TO_CONVERT_TO_STR):
                df[key] = df[key].apply(to_string)
            # fix of the "Date" data type from MongoDB. Now it will become "datetime2" in Fabric
            if df[key].dtype == "datetime64[ns]":
                print("trying to convert datetime column...")
                df[key] = df[key].astype("datetime64[ms]")
            # remove spaces in key/column name
            if " " in key:
                df.rename(columns={key: key.replace(" ", "_")}, inplace=True)
            
            # truncate column name if longer than 128
            if len(key) > 128:
                df.rename(columns={key: key[:128]}, inplace=True)
        
        # print("pandas DataFrame schema after conversion:")
        # print(df.dtypes)
        
        # merge the df to accumulative_df
        if accumulative_df is not None:
            accumulative_df = pd.concat([accumulative_df, df], ignore_index=True)
            # print("concat accumulative_df result:")
            # print(accumulative_df)
        else:
            accumulative_df = df
        
        # write to parquet if accumulative_df is not empty, and:
        #     a. accumulative_df reaches MAX_ROWS, or
        #     b. it has been TIME_THRESHOLD_IN_SEC since last sync
        if (accumulative_df is not None
                and (accumulative_df.shape[0] >= MAX_ROWS
                    or time.time() - last_sync_time >= TIME_THRESHOLD_IN_SEC)):
            parquet_filename = write_parquet_file(accumulative_df, mongodb_params["collection"])
            push_file_to_lz(parquet_filename, lz_params["url"], mongodb_params["collection"], lz_params["app_id"], lz_params["secret"], lz_params["tenant_id"])

def write_parquet_file(accumulative_df: pd.DataFrame, table_name: str):
    filename = get_parquet_filename(table_name)
    print(f"filename={filename}")
    accumulative_df.to_parquet(filename)
    accumulative_df = None
    return filename