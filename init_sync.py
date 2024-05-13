import pymongo
import time
import os
import pandas as pd
import shutil
import logging

from constants import TYPES_TO_CONVERT_TO_STR, MONGODB_READING_BATCH_SIZE, METADATA_FILE_NAME, DATA_FILES_PATH
from utils import get_parquet_full_path_filename, to_string
from push_file_to_lz import push_file_to_lz
from flags import set_init_flag, clear_init_flag


def init_sync(collection_name: str):
    logger = logging.getLogger(f"{__name__}[{collection_name}]")
    logger.info(f"begin init sync for {collection_name}")
    set_init_flag(collection_name)
    logger.debug(f"db_name={os.getenv("MONGO_DB_NAME")}")
    logger.debug(f"collection={collection_name}")
    # for test only
    # logger.debug(f"sleep(60) begin")
    # time.sleep(60)
    # logger.debug(f"sleep(60) ends")
    
    client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
    db = client[os.getenv("MONGO_DB_NAME")]
    collection = db[collection_name]
    
    count = collection.estimated_document_count()
    
    batch_size = int(os.getenv("INIT_LOAD_BATCH_SIZE"))
    
    columns_to_convert_to_str = []
    
    # last_id = None
    
    for index, current_skip in enumerate(range(0, count, batch_size)):
        logger.debug(f"batch index: {index}")
        
        batch_cursor = collection.find().skip(current_skip).limit(batch_size)
        # batch_cursor = collection.find().sort({"_id": 1}).skip(current_skip).limit(batch_size)
        # if index == 0:
        #     batch_cursor = collection.find().sort({"_id": 1}).limit(batch_size)
        # else:
        #     batch_cursor = collection.find({"_id": {"$gt": last_id}}).sort({"_id": 1}).limit(batch_size)
        

        # start_time = time.time()
        batch_df = pd.DataFrame(list(batch_cursor))
        # end_time = time.time()
        # logger.debug(f"list(cursor) took {end_time-start_time:.2f} seconds")
        
        # last_id = batch_df["_id"].iloc[-1]

        for key in batch_df.keys():
            # only detect column type and determine if convert in the first batch
            if index == 0:
                # logger.debug(f"key: {key}")
                first_item = batch_df[key][0]
                data_type = type(first_item)
                # logger.debug(f"data_type: {data_type}")
                if any(isinstance(first_item, t) for t in TYPES_TO_CONVERT_TO_STR):
                    columns_to_convert_to_str.append(key)
            if key in columns_to_convert_to_str:
                batch_df[key] = batch_df[key].apply(to_string)
                # logger.debug(f"data_type afterwards: {type(batch_df[key][0])}")
            # fix of the "Date" data type from MongoDB. Now it will become "datetime2" in Fabric
            if batch_df[key].dtype == "datetime64[ns]":
                logger.debug("trying to convert datetime column...")
                batch_df[key] = batch_df[key].astype("datetime64[ms]")
            # remove spaces in key/column name
            if " " in key:
                batch_df.rename(columns={key: key.replace(" ", "_")}, inplace=True)
            
            # truncate column name if longer than 128
            if len(key) > 128:
                batch_df.rename(columns={key: key[:128]}, inplace=True)

        # logger.debug(batch_df.info())
        logger.debug("creating parquet file...")
        parquet_full_path_filename = get_parquet_full_path_filename(collection_name)
        logger.info(f"writing parquet file: {parquet_full_path_filename}")
        batch_df.to_parquet(parquet_full_path_filename, index=False)
        if index == 0:
            metadata_json_path = __copy_metadata_json(collection_name)
            push_file_to_lz(metadata_json_path, collection_name)
        push_file_to_lz(parquet_full_path_filename, collection_name)
    clear_init_flag(collection_name)
    

def __copy_metadata_json(table_name):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    source = os.path.join(current_dir, METADATA_FILE_NAME)
    dest = os.path.join(current_dir, DATA_FILES_PATH, table_name, METADATA_FILE_NAME)
    shutil.copyfile(source, dest)
    return dest
