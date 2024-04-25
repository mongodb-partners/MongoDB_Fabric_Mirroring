import pymongo
import time
import os
import pandas as pd
import shutil

from constants import TYPES_TO_CONVERT_TO_STR, MONGODB_READING_BATCH_SIZE, METADATA_FILE_NAME, DATA_FILES_PATH
from utils import get_parquet_filename, to_string
from push_file_to_lz import push_file_to_lz

def init_sync(mongodb_params, lz_params):
    parquet_filename = get_parquet_filename(mongodb_params["collection"])
    
    client = pymongo.MongoClient(mongodb_params["conn_str"])
    db = client[mongodb_params["db_name"]]
    collection = db[mongodb_params["collection"]]
    
    count = collection.estimated_document_count()
    
    batch_size = MONGODB_READING_BATCH_SIZE
    
    columns_to_convert_to_str = []
    
    # last_id = None
    
    for index, current_skip in enumerate(range(0, count, batch_size)):
        print(f"batch index: {index}")
        
        batch_cursor = collection.find().skip(current_skip).limit(batch_size)
        # batch_cursor = collection.find().sort({"_id": 1}).skip(current_skip).limit(batch_size)
        # if index == 0:
        #     batch_cursor = collection.find().sort({"_id": 1}).limit(batch_size)
        # else:
        #     batch_cursor = collection.find({"_id": {"$gt": last_id}}).sort({"_id": 1}).limit(batch_size)
        

        # start_time = time.time()
        batch_df = pd.DataFrame(list(batch_cursor))
        # end_time = time.time()
        # print(f"list(cursor) took {end_time-start_time:.2f} seconds")
        
        # last_id = batch_df["_id"].iloc[-1]

        for key in batch_df.keys():
            # only detect column type and determine if convert in the first batch
            if index == 0:
                # print(f"key: {key}")
                first_item = batch_df[key][0]
                data_type = type(first_item)
                # print(f"data_type: {data_type}")
                if any(isinstance(first_item, t) for t in TYPES_TO_CONVERT_TO_STR):
                    columns_to_convert_to_str.append(key)
            if key in columns_to_convert_to_str:
                batch_df[key] = batch_df[key].apply(to_string)
                # print(f"data_type afterwards: {type(batch_df[key][0])}")
            
            # remove spaces in key/column name
            if " " in key:
                batch_df.rename(columns={key: key.replace(" ", "_")}, inplace=True)
            
            # truncate column name if longer than 128
            if len(key) > 128:
                batch_df.rename(columns={key: key[:128]}, inplace=True)

        # print(batch_df.info())
        if index == 0:
            print("creating parquet file...")
            batch_df.to_parquet(parquet_filename, index=False, engine="fastparquet")
        else:
            print("appending to the parquet file...")
            batch_df.to_parquet(parquet_filename, index=False, engine="fastparquet", append=True)
    metadata_json_path = __copy_metadata_json(mongodb_params["collection"])
    push_file_to_lz(metadata_json_path, lz_params["url"], mongodb_params["collection"], lz_params["app_id"], lz_params["secret"], lz_params["tenant_id"])
    push_file_to_lz(parquet_filename, lz_params["url"], mongodb_params["collection"], lz_params["app_id"], lz_params["secret"], lz_params["tenant_id"])
    

def __copy_metadata_json(table_name):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    source = os.path.join(current_dir, METADATA_FILE_NAME)
    dest = os.path.join(current_dir, DATA_FILES_PATH, table_name, METADATA_FILE_NAME)
    shutil.copyfile(source, dest)
    return dest
