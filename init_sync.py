import pymongo
import time
import os
import pandas as pd
import shutil
import logging
import glob
from bson import ObjectId
import pickle

from constants import (
    TYPES_TO_CONVERT_TO_STR, 
    MONGODB_READING_BATCH_SIZE, 
    METADATA_FILE_NAME, 
    DATA_FILES_PATH, 
    INIT_SYNC_CURRENT_SKIP_FILE_NAME,
    INIT_SYNC_LAST_ID_FILE_NAME,
    )
from utils import get_parquet_full_path_filename, to_string, get_table_dir
from push_file_to_lz import push_file_to_lz
from flags import set_init_flag, clear_init_flag


def init_sync(collection_name: str):
    logger = logging.getLogger(f"{__name__}[{collection_name}]")
    # skip init_sync if there's already parquet files and no current_skip/last_id file
    table_dir = get_table_dir(collection_name)
    current_skip_file_path = os.path.join(table_dir, INIT_SYNC_CURRENT_SKIP_FILE_NAME)
    last_id_file_path = os.path.join(table_dir, INIT_SYNC_LAST_ID_FILE_NAME)
    # needs to exclude the situation of cache or temp parquet files exist but
    # not normal numbered parquet files, in which case we shouldn't skip init sync
    if (not os.path.exists(last_id_file_path) 
        and os.path.exists(table_dir)
        and any(
            file.endswith(".parquet") 
            and os.path.splitext(file)[0].isnumeric()
            for file in os.listdir(table_dir)
        )
    ):
        logger.info(f"init sync for collection {collection_name} has already finished previously. Skipping init sync this time.")
        return
    logger.info(f"begin init sync for {collection_name}")
    set_init_flag(collection_name)
    logger.debug(f"db_name={os.getenv("MONGO_DB_NAME")}")
    logger.debug(f"collection={collection_name}")
    enable_perf_timer = os.getenv("DEBUG__ENABLE_PERF_TIMER")
    
    client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
    db = client[os.getenv("MONGO_DB_NAME")]
    collection = db[collection_name]
    
    count = collection.estimated_document_count()
    
    batch_size = int(os.getenv("INIT_LOAD_BATCH_SIZE"))
    
    columns_to_convert_to_str = None
    
    last_id = None
    
    # detect if there's a last_id file, and restore last_id from it
    if os.path.exists(last_id_file_path):
        with open(last_id_file_path, "rb") as last_id_file:
            last_id = pickle.load(last_id_file)
            logger.info(f"interrupted init sync detected, continuing with previous _id={last_id}")
    # detect if there's a current_skip file, and start from there if there is
    # init_skip = 0
    # if os.path.exists(current_skip_file_path):
    #     with open(current_skip_file_path, "r") as current_skip_file:
    #         content = current_skip_file.read()
    #         if content.isnumeric():
    #             init_skip = int(content)
    #             logger.info(f"interrupted init sync detected, continuing with current_skip={init_skip}")
    
    # TODO: anti-crash for this count as well
    processed_doc_count = 0
    
    # for index, current_skip in enumerate(range(init_skip, count, batch_size)):
    while processed_doc_count < count:
        # for debug only
        debug_env_var_sleep_sec = os.getenv("DEBUG__INIT_SYNC_SLEEP_SEC")
        if debug_env_var_sleep_sec and debug_env_var_sleep_sec.isnumeric():
            logger.info(f"sleep({debug_env_var_sleep_sec}) begin")
            time.sleep(int(debug_env_var_sleep_sec))
            logger.info(f"sleep({debug_env_var_sleep_sec}) ends")
        # batch_cursor = collection.find().skip(current_skip).limit(batch_size)
        
        # batch_cursor = collection.find().sort({"_id": 1}).skip(current_skip).limit(batch_size)
        
        if not last_id:
            batch_cursor = collection.find().sort({"_id": 1}).limit(batch_size)
        else:
            batch_cursor = collection.find({"_id": {"$gt": last_id}}).sort({"_id": 1}).limit(batch_size)
        

        read_start_time = time.time()
        batch_df = pd.DataFrame(list(batch_cursor))
        read_end_time = time.time()
        if enable_perf_timer:
            logger.info(f"TIME: read took {read_end_time-read_start_time:.2f} seconds")
        
        # quit the loop if no more data
        if batch_df.empty:
            break
        
        raw_last_id = batch_df["_id"].iloc[-1]
        
        for key in batch_df.keys():
            # only detect column type and determine if convert in the first batch
            if columns_to_convert_to_str is None:
                columns_to_convert_to_str = []
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
        
        trans_end_time = time.time()
        if enable_perf_timer:
            logger.info(f"TIME: trans took {trans_end_time-read_end_time:.2f} seconds")

        # logger.debug(batch_df.info())
        logger.debug("creating parquet file...")
        parquet_full_path_filename = get_parquet_full_path_filename(collection_name)
        logger.info(f"writing parquet file: {parquet_full_path_filename}")
        batch_df.to_parquet(parquet_full_path_filename, index=False)
        write_end_time = time.time()
        if enable_perf_timer:
            logger.info(f"TIME: write took {write_end_time-trans_end_time:.2f} seconds")
        if not last_id:
            # do not copy, but send the template file directly
            metadata_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), METADATA_FILE_NAME)
            push_file_to_lz(metadata_json_path, collection_name)
        push_start_time = time.time()
        push_file_to_lz(parquet_full_path_filename, collection_name)
        push_end_time = time.time()
        if enable_perf_timer:
            logger.info(f"TIME: push took {push_end_time-push_start_time:.2f} seconds")
        # write current_skip to a file
        # with open(current_skip_file_path, "w") as current_skip_file:
        #     # since when resuming, we will directly use the current_skip in 
        #     # file, and at this point we have finished pushing for the 
        #     # current_skip, we want to continue with the next skip, which is 
        #     # current_skip + batch_size
        #     logger.info(f"writing current_skip into file: {current_skip + batch_size}")
        #     current_skip_file.write(str(current_skip + batch_size))
        last_id = raw_last_id
        logger.debug(f"DATA TYPE OF last_id IS: {type(last_id)}")
        # write current last_id to file
        with open(last_id_file_path, "wb") as last_id_file:
            logger.info(f"writing last_id into file: {last_id}")
            pickle.dump(last_id, last_id_file)
        processed_doc_count += batch_size
        # FOR TEST ONLY
        # logger.info("sleep(10) after write and push a parquet file")
        # time.sleep(10)
    # delete current_skip file, as init sync is complete
    # logger.info("removing the current_skip file")
    # os.remove(current_skip_file_path)
    
    # delete last_id file, as init sync is complete
    logger.info("removing the last_id file")
    os.remove(last_id_file_path)
    
    clear_init_flag(collection_name)
    logger.info(f"init sync completed for collection {collection_name}")
