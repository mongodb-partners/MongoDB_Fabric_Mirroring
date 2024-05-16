import pymongo
import time
import os
import pandas as pd
import shutil
import logging
import glob

from constants import (
    TYPES_TO_CONVERT_TO_STR, 
    MONGODB_READING_BATCH_SIZE, 
    METADATA_FILE_NAME, 
    DATA_FILES_PATH, 
    INIT_SYNC_CURRENT_SKIP_FILE_NAME,
    )
from utils import get_parquet_full_path_filename, to_string, get_table_dir
from push_file_to_lz import push_file_to_lz
from flags import set_init_flag, clear_init_flag


def init_sync(collection_name: str):
    logger = logging.getLogger(f"{__name__}[{collection_name}]")
    # skip init_sync if there's already parquet files and no current_skip file
    table_dir = get_table_dir(collection_name)
    current_skip_file_path = os.path.join(table_dir, INIT_SYNC_CURRENT_SKIP_FILE_NAME)
    if not os.path.exists(current_skip_file_path) and glob.glob(os.path.join(table_dir, "*.parquet")):
        logger.info(f"init sync for collection {collection_name} has already finished previously. Skipping init sync this time.")
        return
    logger.info(f"begin init sync for {collection_name}")
    set_init_flag(collection_name)
    logger.debug(f"db_name={os.getenv("MONGO_DB_NAME")}")
    logger.debug(f"collection={collection_name}")
    
    client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
    db = client[os.getenv("MONGO_DB_NAME")]
    collection = db[collection_name]
    
    count = collection.estimated_document_count()
    
    batch_size = int(os.getenv("INIT_LOAD_BATCH_SIZE"))
    
    columns_to_convert_to_str = []
    
    # last_id = None
    
    # detect if there's a current_skip file, and start from there if there is
    init_skip = 0
    if os.path.exists(current_skip_file_path):
        with open(current_skip_file_path, "r") as current_skip_file:
            content = current_skip_file.read()
            if content.isnumeric():
                init_skip = int(content)
                logger.info(f"interrupted init sync detected, continuing with current_skip={init_skip}")
                
    
    for index, current_skip in enumerate(range(init_skip, count, batch_size)):
        logger.debug(f"batch index: {index}")
        # for test only
        debug_env_var_sleep_sec = os.getenv("DEBUG__INIT_SYNC_SLEEP_SEC")
        if debug_env_var_sleep_sec and debug_env_var_sleep_sec.isnumeric():
            logger.info(f"sleep({debug_env_var_sleep_sec}) begin")
            time.sleep(int(debug_env_var_sleep_sec))
            logger.info(f"sleep({debug_env_var_sleep_sec}) ends")
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
        # write current_skip to a file
        with open(current_skip_file_path, "w") as current_skip_file:
            # since when resuming, we will directly use the current_skip in 
            # file, and at this point we have finished pushing for the 
            # current_skip, we want to continue with the next skip, which is 
            # current_skip + batch_size
            logger.info(f"writing current_skip into file: {current_skip + batch_size}")
            current_skip_file.write(str(current_skip + batch_size))
    # delete current_skip file, as init sync is complete
    logger.info("removing the current_skip file")
    os.remove(current_skip_file_path)
    clear_init_flag(collection_name)
    

def __copy_metadata_json(table_name):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    source = os.path.join(current_dir, METADATA_FILE_NAME)
    dest = os.path.join(current_dir, DATA_FILES_PATH, table_name, METADATA_FILE_NAME)
    shutil.copyfile(source, dest)
    return dest
