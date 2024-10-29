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
from file_utils import FileType, read_from_file


logger = logging.getLogger(f"{__name__}")


def _converter_template(obj, type_name, raw_convert_func, default_value=None):
    original_type = type(obj)
    try:
        return raw_convert_func(obj)
    except (ValueError, TypeError):
        logger.warning(
            f'Unsuccessful conversion from "{obj}" of type {original_type} to {type_name}.'
        )
        return default_value


def to_string(obj) -> str:
    return _converter_template(
        obj, "string", lambda o: str(o) if o is not None else None
    )


def to_numpy_int64(obj) -> np.int64:
    def raw_to_numpy_int64(obj) -> np.int64:
        # there's a rare case that converting a list of int to numpy.int64 won't
        # raise any error, hence covering it here separately
        if isinstance(obj, list) or isinstance(obj, dict):
            raise ValueError
        return np.int64(obj)

    return _converter_template(obj, "numpy.int64", raw_to_numpy_int64)


def to_numpy_bool(obj) -> np.bool_:
    def raw_to_numpy_bool(obj) -> np.bool_:
        if obj == 0 or isinstance(obj, str) and (obj == "0" or obj.lower() == "false"):
            return False
        elif obj == 1 or isinstance(obj, str) and (obj == "1" or obj.lower() == "true"):
            return True
        else:
            return None

    return _converter_template(obj, "numpy.bool_", raw_to_numpy_bool, None)


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
    pd.Timestamp: to_pandas_timestamp,
}

COLUMN_DTYPE_CONVERSION_MAP = {
    # date type fix
    "datetime64[ns]": "datetime64[ms]",
    # nullable fix
    "bool": "boolean",
    # nullable fix
    "int64": "Int64",
}


def init_column_schema(column_dtype, first_item) -> dict:
    item_type = type(first_item)
    schema_of_this_column = {}
    if any(isinstance(first_item, t) for t in TYPES_TO_CONVERT_TO_STR):
        item_type = str
    # when encountering NoneType column, force convert it to str
    if item_type == NoneType:
        item_type = str
        column_dtype = "object"

    #Diana 107 comment and prints added
    # if not column_dtype:
    print(f"SU: original column_dtype={column_dtype}")
    column_dtype = COLUMN_DTYPE_CONVERSION_MAP.get(column_dtype.__str__(), column_dtype)
    print(f"SU: converted column_dtype={column_dtype}")
    schema_of_this_column[DTYPE_KEY] = column_dtype
    schema_of_this_column[TYPE_KEY] = item_type
    return schema_of_this_column


def process_column_name(column_name: str) -> str:
    return str(column_name).replace(" ", "_")[:128]


def _get_first_item(df: pd.DataFrame, column_name: str):
    """
    Get the first non-null item from given DataFrame column.
    This is useful when reading data in init sync, and a few (or even just one)
    documents have an extra column, making most items of this column to be null.
    In this case we really want to find the actual non-null item, and derive
    data type based on it.

    Args:
        df (pd.DataFrame): The DataFrame object
        column_name (str): The name of the column

    Returns:
        Any: the first non-null item in given DataFrame column
    """
    first_valid_index = (
        df[column_name].first_valid_index() or 0
    )  # in case of first_valid_index() return None, let it be zero
    first_valid_item = df[column_name][first_valid_index]
    logger.debug(
        f"get first item {first_valid_item} of type {type(first_valid_item)} in column {column_name}"
    )
    return first_valid_item


def init_table_schema(table_name: str):
    # determine if the internal schema file exist
    table_dir = get_table_dir(table_name)
    schema_file_path = os.path.join(table_dir, INTERNAL_SCHEMA_FILE_NAME)
    schema_of_this_table = read_from_file(
        table_name, INTERNAL_SCHEMA_FILE_NAME, FileType.PICKLE
    )
    if schema_of_this_table:
        logger.info(f"loaded schema of {table_name} from file")
        schemas.init_table_schema(table_name, schema_of_this_table)
        # load column renaming if it exists, otherwise this table has been previously
        # initiated but no column is renamed, so we don't need to do anything
        table_column_renaming = read_from_file(
            table_name, COLUMN_RENAMING_FILE_NAME, FileType.PICKLE
        )
        if table_column_renaming:
            logger.info(f"loaded column renaming of {table_name} from file")
            schemas.init_column_renaming(table_name, table_column_renaming)
    else:
        # else, init schema from collection
        client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
        db = client[os.getenv("MONGO_DB_NAME")]
        collection = db[table_name]
        schema_of_this_table = {}
        column_renaming_of_this_table = {}
        with collection.find().sort({"_id": 1}).limit(1) as cursor:
            df = pd.DataFrame(list(cursor))
            for col_name in df.keys().values:
                first_item = _get_first_item(df, col_name)
                column_dtype = df[col_name].dtype
                schema_of_this_column = init_column_schema(column_dtype, first_item)
                processed_col_name = process_column_name(col_name)
                if processed_col_name != col_name:
                    column_renaming_of_this_table[col_name] = processed_col_name
                schema_of_this_table[processed_col_name] = schema_of_this_column
        schemas.init_table_schema(table_name, schema_of_this_table)
        schemas.init_column_renaming(table_name, column_renaming_of_this_table)


def process_dataframe(table_name: str, df: pd.DataFrame):
    for col_name in df.keys().values:
        current_dtype = df[col_name].dtype
        current_first_item = _get_first_item(df, col_name)
        current_item_type = type(current_first_item)

        processed_col_name = schemas.find_column_renaming(table_name, col_name)
        schema_of_this_column = schemas.get_table_column_schema(table_name, col_name)

        if not processed_col_name and not schema_of_this_column:
            # new column, process it and append schema
            schema_of_this_column = init_column_schema(
                current_dtype, current_first_item
            )
            processed_col_name = process_column_name(col_name)
            if processed_col_name != col_name:
                schemas.add_column_renaming(table_name, col_name, processed_col_name)
            schemas.append_schema_column(
                table_name, processed_col_name, schema_of_this_column
            )

        # processed_col_name might have been updated for new column, so no need to use elif here
        # 2 scenarios are included by this if clause:
        #       1. existing column renaming found
        #       2. new column with the need to rename
        # and the extra processed_col_name can make sure to exclude the scenario
        # of existing column without the need to rename, in which case processed_col_name
        # from find_column_renaming() will be None.
        if processed_col_name and processed_col_name != col_name:
            df.rename(columns={col_name: processed_col_name}, inplace=True)
            col_name = processed_col_name

        # schema_of_this_colum should always exists at this point
        # existing column or new column with schema appended, process accroding to schema_of_this_colum
        if current_item_type != schema_of_this_column[TYPE_KEY]:
            logger.debug(
                f"different item type detected: current_item_type={current_item_type}, item type from schema={schema_of_this_column[TYPE_KEY]}"
            )
            df[col_name] = df[col_name].apply(
                TYPE_TO_CONVERT_FUNCTION_MAP.get(
                    schema_of_this_column[TYPE_KEY], do_nothing
                )
            )
        logger.debug(f"current_dtype={current_dtype}")
        logger.debug(
            f"schema_of_this_column[DTYPE_KEY]={schema_of_this_column[DTYPE_KEY]}"
        )
        if current_dtype != schema_of_this_column[DTYPE_KEY]:
            try:
                logger.debug(
                    f"different column dtype detected: current_dtype={current_dtype}, item type from schema={schema_of_this_column[DTYPE_KEY]}"
                )
                df[col_name] = df[col_name].astype(schema_of_this_column[DTYPE_KEY])
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"An {e.__class__.__name__} was caught when trying to convert "
                    + f"the dtype of the column {col_name} from {current_dtype} to {schema_of_this_column[DTYPE_KEY]}"
                )
