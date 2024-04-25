import bson
import pandas as pd


TYPES_TO_CONVERT_TO_STR = [
    bson.objectid.ObjectId, 
    dict, 
    list, 
    pd.Timestamp
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