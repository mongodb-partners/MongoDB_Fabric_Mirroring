import bson
import pandas as pd


TYPES_TO_CONVERT_TO_STR = [
    bson.objectid.ObjectId,
    dict,
    list,
    # pd.Timestamp
]

DATA_FILES_PATH = "data_files"

FILE_NAME_LENGTH = 20

MONGODB_READING_BATCH_SIZE = 100000

METADATA_FILE_NAME = "_metadata.json"

ROW_MARKER_COLUMN_NAME = "__rowMarker__"

CHANGE_STREAM_OPERATION_MAP = {
    "insert": 0,
    "update": 1,
    "delete": 2,
}

CHANGE_STREAM_OPERATION_MAP_WHEN_INIT = {
    "insert": 4,
    "update": 1,
    "delete": 2,
}

TEMP_PREFIX_DURING_INIT = "Temp_"

#INIT_SYNC_CURRENT_SKIP_FILE_NAME = "init_sync_current_skip"
# added the two new files to save the initial sync status and last parquet file number

INIT_SYNC_STATUS_FILE_NAME = "_init_sync_status.pkl"

LAST_PARQUET_FILE_NUMBER = "_last_created_parquet.pkl"

INIT_SYNC_LAST_ID_FILE_NAME = "_last_id.pkl"

INIT_SYNC_MAX_ID_FILE_NAME = "_max_id.pkl"

DELTA_SYNC_CACHE_PARQUET_FILE_NAME = "_incremental_change_cache.parquet"

DELTA_SYNC_RESUME_TOKEN_FILE_NAME = "_resume_token.pkl"

INTERNAL_SCHEMA_FILE_NAME = "_internal_schema.pkl"

COLUMN_RENAMING_FILE_NAME = "_column_renaming.pkl"

CONVERSION_LOG_FILE_NAME = "_conversion_log.txt"
# dict keys for schema
TYPE_KEY = "type"
DTYPE_KEY = "dtype"
