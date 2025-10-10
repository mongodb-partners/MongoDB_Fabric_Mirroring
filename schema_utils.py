from datetime import datetime
import os
import json
import logging
#17June2025 - doesnt work for 3.9 and lesser Python versions
#from types import NoneType
import bson.int64
import pymongo
import pandas as pd
import numpy as np
import pickle
# from bson import Decimal128, int64
import bson
from push_file_to_lz import push_file_to_lz
from utils import get_table_dir
from constants import (
    CONVERSION_LOG_FILE_NAME,
    INTERNAL_SCHEMA_FILE_NAME,
    TYPE_KEY,
    DTYPE_KEY,
    TYPES_TO_CONVERT_TO_STR,
    COLUMN_RENAMING_FILE_NAME
)
import schemas
from file_utils import FileType, append_to_file, read_from_file
from pandas.api.types import (
    is_numeric_dtype,
    is_string_dtype,
    is_object_dtype,
    is_datetime64_any_dtype
)


logger = logging.getLogger(f"{__name__}")
#17June2025 - added NoneType manually instead of importing from types for 3.9 and lesser Python versions
NoneType = type(None)

def _converter_template(obj, type_name, raw_convert_func, default_value=None):
    original_type = type(obj) 
    logger.debug(f"Converting {obj} of type {original_type} to {type_name}.")
    try:
        return raw_convert_func(obj)
    except (ValueError, TypeError):
        logger.warning(f'Unsuccessful conversion from "{obj}" of type {original_type} to {type_name}.')
        global conversion_flag
        conversion_flag = True

        append_to_file(
            f"\n{current_column_name:<20} | {str(obj):<20} | {str(default_value):<20}",
            table_name,
            CONVERSION_LOG_FILE_NAME,
            FileType.TEXT
        )
        return default_value


def to_string(obj) -> str:
    if isinstance(obj, list) or isinstance(obj, dict):
        return to_json_string(obj)
    return _converter_template(
        obj, "string", lambda o: str(o) if o is not None and not pd.isna(o) else ''
    )


def to_numpy_int64(obj) -> np.int64:
    logger.debug(f"to_numpy_int64: obj={obj}, type={type(obj)}")
    def raw_to_numpy_int64(obj) -> np.int64:
        # there's a rare case that converting a list of int to numpy.int64 won't
        # raise any error, hence covering it here separately
        if isinstance(obj, bson.Decimal128): 
            return np.int64(obj.to_decimal())
        if isinstance(obj, list) or isinstance(obj, dict):
            return to_json_string(obj)
        if obj is not None and not pd.isna(obj):
           return np.int64(obj)
        else:
            return None

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
    if isinstance(obj, bson.Decimal128):
        obj = str(obj)
    return _converter_template(obj, "numpy.float64", lambda o: np.float64(o) if o is not None and not pd.isna(o) else None)


def to_pandas_timestamp(obj) -> pd.Timestamp:
    # return _converter_template(obj, "pandas.Timestamp", lambda o: pd.Timestamp(o))
    return _converter_template(obj, "pandas.Timestamp", lambda o: pd.to_datetime(o, utc=True).isoformat() if o is not None and not pd.isna(o) else '')


def do_nothing(obj):
    original_type = type(obj)
    logger.info(f'Did not convert "{obj}" of type {original_type}.')
    return obj

def to_datetime_iso(obj) -> str:
    if not isinstance(obj, datetime):
        return obj
    return _converter_template(obj, "string", lambda o: o.isoformat() if o is not None and not pd.isna(o) else '')


def to_json_string(obj) -> str:
    if isinstance(obj, list):
        return _converter_template(obj, "string", lambda o: json.dumps([ob for ob in o]))
    return _converter_template(obj, "string", lambda o: json.dumps(o, default=str, cls=CustomJSONEncoder) if o is not None and not pd.isna(o) else '')


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        conversion_fcn = TYPE_TO_CONVERT_FUNCTION_MAP.get(type(obj))
        if conversion_fcn:
            return conversion_fcn(obj)
        return super(CustomJSONEncoder, self).default(obj)

TYPE_TO_CONVERT_FUNCTION_MAP = {
    str: to_string,
    int: to_numpy_int64,
    float: to_numpy_float64,
    bool: to_numpy_bool,
    datetime: to_datetime_iso,
    bson.ObjectId: to_string,
    bson.Decimal128: to_numpy_float64,
    np.int32: to_numpy_int64,
    np.int64: to_numpy_int64,
    bson.int64.Int64: to_numpy_int64,
    np.bool_: to_numpy_bool,
    np.float64: to_numpy_float64,
    dict: to_json_string,
    list: to_json_string,
    pd.Timestamp: to_pandas_timestamp
}

COLUMN_DTYPE_CONVERSION_MAP = {
    # date type fix
    "datetime64[ns]": "datetime64[ms]",
    # nullable fix
    "bool": "boolean",
    # nullable fix
    "int64": "Int64"
}


def init_column_schema(column_dtype, first_item) -> dict:
    item_type = type(first_item)
    schema_of_this_column = {}
    if any(isinstance(first_item, t) for t in TYPES_TO_CONVERT_TO_STR):
        item_type = str
    # when encountering NoneType column, force convert it to str
    if item_type == NoneType:
    # if item_type is None:
        item_type = str
        column_dtype = "object"
    #Diana - handling bson.Decimal128 cant be target data type as it cant be written to parquet
    if isinstance(first_item, bson.Decimal128):
        item_type = float
    #It takes column dtype as object otherwise     
    #    column_dtype = "object"
        column_dtype = "float64"
    #Diana 107 comment and prints added
    # if not column_dtype:
    #print(f"SU: original column_dtype={column_dtype}")
    column_dtype = COLUMN_DTYPE_CONVERSION_MAP.get(column_dtype.__str__(), column_dtype)
    #print(f"SU: converted column_dtype={column_dtype}")
    schema_of_this_column[DTYPE_KEY] = column_dtype
    schema_of_this_column[TYPE_KEY] = item_type
    logger.debug(
        f"Internal schema file : column_dtype {column_dtype} and item_type {item_type}"
    )
    return schema_of_this_column


def process_column_name(column_name: str) -> str:
    return str(column_name).replace(" ", "_")[:128]


def _get_first_valid_id(df: pd.DataFrame, column_name: str):
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
    # first_valid_item = df[column_name][first_valid_index]
    first_valid_index_id = df['_id'][first_valid_index]
    # logger.debug(
    #     f"get first item {first_valid_index_id} of type {type(first_valid_index)} in column {column_name}"
    # )
    # logger.debug(
    #     f"get first item {first_valid_item} of type {type(first_valid_item)} in column {column_name}"
    # )
    # return first_valid_item
    return first_valid_index_id

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
    #schema_file_path = os.path.join(table_dir, INTERNAL_SCHEMA_FILE_NAME)
    schema_of_this_table = read_from_file(
        table_name, INTERNAL_SCHEMA_FILE_NAME, FileType.PICKLE
    )
    if schema_of_this_table:
        logger.info(f"loaded schema of {table_name} from file")
    #    schemas.init_table_schema(table_name, schema_of_this_table)
    # 9 May 2025 should not write back to internal schema file
        schemas.init_table_schema_to_mem(table_name, schema_of_this_table)
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
        batch_size = int(os.getenv("INIT_LOAD_BATCH_SIZE"))
        collection = db[table_name]
        schema_of_this_table = {}
        column_renaming_of_this_table = {}
        with collection.find().sort({"_id": 1}).limit(batch_size) as cursor:
            fetched_data = list(cursor)
            #print(f"fetched_data: {fetched_data}")
            df = pd.DataFrame(fetched_data)
            for col_name in df.keys().values:
                get_id = _get_first_valid_id(df, col_name)
                # Fetch the exact value from mongodb using the _id, dumping into df changes the data type.
                # projection = {col_name: 1, "_id": 0} if col_name != "_id" else {"_id": 1}
                # data = list(collection.find({"_id": get_id}, (projection)))[0].get(col_name)
                # logger.debug(
                #     f"get first item {data} of type {type(data)} in column {col_name}"
                # )
                data = next(item.get(col_name) for item in fetched_data if item.get('_id') == get_id)
                logger.debug(f"get first item {data} of type {type(data)} in column {col_name}")
                column_dtype = df[col_name].dtype
                # column_dtype = type(data)
                # schema_of_this_column = init_column_schema(column_dtype, first_item)
                schema_of_this_column = init_column_schema(column_dtype, data)
                processed_col_name = process_column_name(col_name)
                if processed_col_name != col_name:
                    column_renaming_of_this_table[col_name] = processed_col_name
                schema_of_this_table[processed_col_name] = schema_of_this_column
        schemas.init_table_schema(table_name, schema_of_this_table)
        schemas.init_column_renaming(table_name, column_renaming_of_this_table)


def process_dataframe(table_name_param: str, df: pd.DataFrame):
    global current_column_name, table_name, conversion_flag
    table_name = table_name_param
    conversion_flag = False
    for col_name in df.keys().values:
        current_dtype = df[col_name].dtype
        current_first_item = _get_first_item(df, col_name)
        #current_item_type = type(current_first_item)
        

        processed_col_name = schemas.find_column_renaming(table_name, col_name)
        logger.debug(
                    f"%%%% Processed col name found: processed_col_name is {processed_col_name} %%%%%"
        )
        schema_of_this_column = schemas.get_table_column_schema(table_name, col_name)
        logger.debug(
                    f"%%%% In process_df: schema_of_this_column is {schema_of_this_column} %%%%%"
                )
        if not processed_col_name and not schema_of_this_column:
            logger.debug(
                    f"%%%% In process_df, schema of col doesnt exist: schema_of_this_column is {schema_of_this_column} and processed_col_name is {processed_col_name} %%%%%"
                )
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
            # May 9 : get schema from file for the renamed column
            schema_of_this_column = schemas.get_table_column_schema(table_name, col_name)

        # schema_of_this_column should always exists at this point
        # existing column or new column with schema appended, process according to schema_of_this_column
        #if current_item_type != schema_of_this_column[TYPE_KEY]:
        expected_type = schema_of_this_column[TYPE_KEY]
        for item in df[col_name]:
            current_column_name = col_name
            if not isinstance(item, expected_type):
                logger.debug(
                    f" item type detected: current item is {item} of type={type(item)}, expected item type from schema= {expected_type}"
                )
                conversion_fcn = TYPE_TO_CONVERT_FUNCTION_MAP.get(
                    expected_type, do_nothing
                )
                
                # Set the current column name for logging
                df[col_name] = df[col_name].apply(conversion_fcn)
                print(df[col_name])
                break
        # for index, item in enumerate(df[col_name]):
            # print(f"Row {index}: Value={item}, Type={type(item)}")
            
        current_dtype = df[col_name].dtype
        logger.debug(f"current_dtype={current_dtype}")
        logger.debug(
            f"schema_of_this_column[DTYPE_KEY]={schema_of_this_column[DTYPE_KEY]}"
        )

        if (expected_type == bson.int64.Int64 or expected_type == int) and current_dtype == "float64":
            # Convert to int64
            logger.debug(
                f"Converting column {col_name} from float64 to Int64"
            )   
            df[col_name] = df[col_name].astype("Int64")

        current_dtype = df[col_name].dtype
        #if current_dtype != schema_of_this_column[DTYPE_KEY]:
        logger.debug(
            f">>>>>>>>>>current_dtype: {current_dtype}"
            )
        ##column_final_dtype = COLUMN_DTYPE_CONVERSION_MAP.get(current_dtype.__str__(), DEFAULT_DTYPE)
        # Date type needs to be converted to MILLIS from NANOS in all cases
        if is_datetime64_any_dtype(df[col_name]):
            try:
                logger.debug(
                    f"different column dtype detected: current_dtype={current_dtype}, item type from default=datetime64[ms]"
                )
                #df[col_name] = df[col_name].dt.floor('ms')
                df[col_name] = df[col_name].dt.tz_localize(None)
                df[col_name] = df[col_name].astype("datetime64[ms]")      
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"An {e.__class__.__name__} was caught when trying to convert "
                    + f"the dtype of the column {col_name} from {current_dtype} to datetime64[ms]"
                )
        
        current_dtype = df[col_name].dtype
        #if current_dtype != schema_of_this_column[DTYPE_KEY]:
        logger.debug(
            f">>>>>>>>>>current_dtype 1: {current_dtype}, "
            f">>>>>>>>>>schema_of_this_column[DTYPE_KEY] 1: {schema_of_this_column[DTYPE_KEY]}, "
            f"****is_datetime64_any_dtype(df['col_name']): {is_datetime64_any_dtype(df[col_name])}, "
            f"****is_object_dtype(schema_of_this_column[DTYPE_KEY]): {is_object_dtype(schema_of_this_column[DTYPE_KEY])}"
            )
        ##if current_dtype == datetime and schema_of_this_column[DTYPE_KEY] == object:
        #print(f"****is_datetime64_any_dtype(df['col_name']): {is_datetime64_any_dtype(df[col_name])}")
        #print(f"****is_object_dtype(schema_of_this_column[DTYPE_KEY]): {is_object_dtype(schema_of_this_column[DTYPE_KEY])}")
        if is_datetime64_any_dtype(df[col_name]) and is_object_dtype(schema_of_this_column[DTYPE_KEY]):
            do_nothing
        elif current_dtype.__str__() != schema_of_this_column[DTYPE_KEY].__str__():
            try:
                logger.debug(
                    f"different column dtype detected1: current_dtype={current_dtype}, item type from schema 1 ={schema_of_this_column[DTYPE_KEY]}"
                )
                df[col_name] = df[col_name].astype(schema_of_this_column[DTYPE_KEY])
                
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"An {e.__class__.__name__} was caught when trying to convert "
                    + f"the dtype of the column {col_name} from {current_dtype} to {schema_of_this_column[DTYPE_KEY]}"
                )  
    # Check if conversion log file exists before pushing
    print("conversion_flag: ", conversion_flag)
    conversion_log_path = os.path.join(get_table_dir(table_name), CONVERSION_LOG_FILE_NAME)
    if os.path.exists(conversion_log_path) and conversion_flag:
        push_file_to_lz(conversion_log_path, table_name)
