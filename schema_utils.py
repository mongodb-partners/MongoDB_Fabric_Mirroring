import os
import logging
from types import NoneType
import pymongo
import pandas as pd
import numpy as np
import pickle

from utils import get_table_dir
from constants import (
    INTERNAL_SCHEMA_FILE_NAME,
    TYPE_KEY,
    DTYPE_KEY,
    TYPES_TO_CONVERT_TO_STR,
    COLUMN_RENAMING_FILE_NAME,
)
import schemas


logger = logging.getLogger(f"{__name__}")


def _converter_template(obj, type_name, simple_convert_func):
    original_type = type(obj)
    try:
        return simple_convert_func(obj)
    except (ValueError, TypeError):
        logger.warning(
            f'Unsuccessful conversion from "{obj}" of type {original_type} to {type_name}.'
        )
        if "bool" in type_name:
            return False
        elif "int" in type_name:
            return 0
        else:
            return None #NULL


def to_string(obj) -> str:
    return _converter_template(obj, "string", lambda o: str(o))


def to_numpy_int64(obj) -> np.int64:
    return _converter_template(obj, "numpy.int64", lambda o: np.int64(o))


def to_numpy_bool(obj) -> np.bool_:
    return _converter_template(
        obj, "numpy.bool_", lambda o: np.bool_(True if o else False)
    )


def to_numpy_float64(obj) -> np.float64:
    return _converter_template(obj, "numpy.float64", lambda o: np.float64(o))


def to_pandas_timestamp(obj) -> pd.Timestamp:
    return _converter_template(obj, "pandas.Timestamp", lambda o: pd.Timestamp(o))


def do_nothing(obj):
    original_type = type(obj)
    logger.info(f'Did not convert "{obj}" of type {original_type}.')
    return obj


TYPE_TO_CONVERT_FUNCTION_MAP = {
    str: to_string,
    np.int64: to_numpy_int64,
    np.bool_: to_numpy_bool,
    np.float64: to_numpy_float64,
    # TODO: test if these 2 are the same thing:
    #       1. pandas._libs.tslibs.timestamps.Timestamp
    #       2. pandas.Timestamp
    pd._libs.tslibs.timestamps.Timestamp: to_pandas_timestamp,
    pd.Timestamp: to_pandas_timestamp,
    # TODO: determine what to do with NoneType
    NoneType: do_nothing,
}


def init_column_schema(column_dtype, first_item) -> dict:
    item_type = type(first_item)
    schema_of_this_column = {}
    if any(isinstance(first_item, t) for t in TYPES_TO_CONVERT_TO_STR):
        item_type = str
    if column_dtype == "datetime64[ns]":
        column_dtype = "datetime64[ms]"
    schema_of_this_column[DTYPE_KEY] = column_dtype
    # TODO: if it is NoneType, make it str
    schema_of_this_column[TYPE_KEY] = item_type
    return schema_of_this_column


def process_column_name(column_name: str) -> str:
    return str(column_name).replace(" ", "_")[:128]


def init_table_schema(table_name: str):
    # determine if the internal schema file exist
    table_dir = get_table_dir(table_name)
    schema_file_path = os.path.join(table_dir, INTERNAL_SCHEMA_FILE_NAME)
    if os.path.exists(schema_file_path):
        # if exists, load from file and return
        with open(schema_file_path, "rb") as schema_file:
            schema_of_this_table = pickle.load(schema_file)
            logger.info(f"loaded schema of {table_name} from file")
            schemas.init_table_schema(table_name, schema_of_this_table)
        # load column renaming if it exists, otherwise this table has been previously
        # initiated but no column is renamed, so we don't need to do anything
        column_renaming_file_path = os.path.join(table_dir, COLUMN_RENAMING_FILE_NAME)
        if os.path.exists(column_renaming_file_path):
            with open(column_renaming_file_path, "rb") as column_renaming_file:
                table_column_renaming = pickle.load(column_renaming_file)
                logger.info(f"loaded column renaming of {table_name} from file")
                schemas.init_column_renaming(table_name, table_column_renaming)
    else:
        # else, init schema from collection
        client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
        db = client[os.getenv("MONGO_DB_NAME")]
        collection = db[table_name]
        schema_of_this_table = {}
        with collection.find().sort({"_id": 1}).limit(1) as cursor:
            df = pd.DataFrame(list(cursor))
            for col_name in df.keys().values:
                first_item = df[col_name][0]
                column_dtype = df[col_name].dtype
                schema_of_this_column = init_column_schema(column_dtype, first_item)
                processed_col_name = process_column_name(col_name)
                if processed_col_name != col_name:
                    schemas.add_column_renaming(
                        table_name, col_name, processed_col_name
                    )
                schema_of_this_table[processed_col_name] = schema_of_this_column
        schemas.init_table_schema(table_name, schema_of_this_table)
        schemas.write_table_schema_to_file(table_name)
        # TODO: sort out writing strategy consistency between schema and column renaming when initializing
        # write_table_column_renaming_to_file(table_name)
