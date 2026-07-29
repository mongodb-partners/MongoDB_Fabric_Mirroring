from datetime import date, datetime
import os
import json
import logging
import time
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
    COLUMN_RENAMING_FILE_NAME,
    SCHEMA_BOOTSTRAP_MAX_FRACTION,
    SCHEMA_BOOTSTRAP_SAMPLE_MAX_ATTEMPTS,
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

INT64_MIN = np.iinfo(np.int64).min
INT64_MAX = np.iinfo(np.int64).max

current_column_name = None
current_document_id = None
table_name = None
conversion_flag = False


def _is_null_value(item) -> bool:
    if item is None:
        return True
    # Container/array-like values are real data, not null sentinels.
    if isinstance(item, (list, dict, tuple, np.ndarray, pd.Series, bytes, bytearray)):
        return False
    if isinstance(item, float) and np.isnan(item):
        return True
    try:
        result = pd.isna(item)
        if isinstance(result, (np.ndarray, pd.Series)):
            return False
        return bool(result)
    except (TypeError, ValueError):
        return False


def _is_non_null_scalar(item) -> bool:
    return not _is_null_value(item)


def _matches_expected_type(item, expected_type) -> bool:
    if _is_null_value(item):
        return True
    if expected_type == bool:
        return isinstance(item, (bool, np.bool_))
    if expected_type == int:
        if not isinstance(item, (int, np.integer, bson.int64.Int64)) or isinstance(
            item, (bool, np.bool_)
        ):
            return False
        try:
            value = int(item)
        except (ValueError, TypeError, OverflowError):
            return False
        return INT64_MIN <= value <= INT64_MAX
    if expected_type == float:
        return isinstance(item, (float, np.floating)) and not isinstance(
            item, (bool, np.bool_, np.integer, int)
        )
    if expected_type == str:
        return isinstance(item, str)
    return isinstance(item, expected_type)


def _needs_type_conversion(item, expected_type) -> bool:
    try:
        return not _matches_expected_type(item, expected_type)
    except Exception as error:
        logger.warning(
            f"Type check failed for value {item!r} ({type(item).__name__}): {error}"
        )
        return True


def _infer_schema_from_pandas_dtype(column_dtype) -> tuple[type, str] | None:
    """Map a pandas dtype to (TYPE_KEY, DTYPE_KEY) when Mongo value is null."""
    dtype_str = str(column_dtype).lower()
    if dtype_str in {"boolean", "bool", "bool[pyarrow]"}:
        return bool, "boolean"
    if dtype_str in {"int64", "int32", "int16", "int8", "int64[pyarrow]"}:
        return int, "Int64"
    if dtype_str in {"uint64", "uint32", "uint16", "uint8"}:
        return int, "Int64"
    if "int" in dtype_str and "print" not in dtype_str:
        return int, "Int64"
    if dtype_str in {"float64", "float32", "double[pyarrow]", "float"}:
        return float, "float64"
    if dtype_str.startswith("datetime"):
        return datetime, "datetime64[ms]"
    if dtype_str in {"string", "large_string", "str", "string[pyarrow]"}:
        return str, "object"
    return None


def _schema_target_is_typed(schema_of_this_column: dict) -> bool:
    if not schema_of_this_column:
        return False
    expected_type = schema_of_this_column.get(TYPE_KEY)
    target_dtype = str(schema_of_this_column.get(DTYPE_KEY, "")).lower()
    if expected_type in {bool, int, float, datetime, date}:
        return True
    if target_dtype in {"boolean", "int64", "float64", "float64[pyarrow]"}:
        return True
    if target_dtype.startswith("datetime"):
        return True
    return False


def _string_series_from_objects(series: pd.Series) -> pd.Series:
    return series.map(
        lambda value: ""
        if _is_null_value(value)
        else (
            json.dumps(value, default=str, cls=CustomJSONEncoder)
            if isinstance(value, (dict, list))
            else str(value)
        )
    ).astype("string")


def _resolve_string_schema_column(series: pd.Series) -> pd.Series:
    if is_object_dtype(series):
        return _string_series_from_objects(series)
    return series.astype("string")


def _typed_null_series(length: int, target_dtype) -> pd.Series:
    target_dtype_str = str(target_dtype)
    if target_dtype_str == "boolean":
        return pd.Series([pd.NA] * length, dtype="boolean")
    if target_dtype_str == "Int64":
        return pd.Series([pd.NA] * length, dtype="Int64")
    if target_dtype_str in ("float64", "Float64"):
        return pd.Series([np.nan] * length, dtype="float64")
    if target_dtype_str.startswith("datetime"):
        return pd.Series([pd.NaT] * length, dtype="datetime64[ms]")
    return pd.Series([pd.NA] * length, dtype="string")


def _cast_column_to_schema_dtype(col_name: str, series: pd.Series, target_dtype) -> pd.Series:
    target_dtype_str = str(target_dtype)
    if target_dtype_str in ("object", "string", "str") or target_dtype_str.startswith("string"):
        return _resolve_string_schema_column(series)
    if target_dtype_str == "boolean":
        return series.astype("boolean")
    if is_datetime64_any_dtype(series) or target_dtype_str.startswith("datetime"):
        series = pd.to_datetime(series, errors="coerce", utc=True)
        if hasattr(series.dt, "tz_localize"):
            series = series.dt.tz_localize(None)
        return series.astype("datetime64[ms]")
    if target_dtype_str in ("float64", "Float64"):
        if is_object_dtype(series) or series.map(
            lambda value: isinstance(value, (bson.Decimal128, str, bson.int64.Int64))
        ).any():
            series = series.map(_coerce_to_float64_value)
        return series.astype("float64")
    if target_dtype_str == "Int64":
        if is_object_dtype(series):
            series = series.map(_coerce_to_int64_value)
        return series.astype("Int64")
    if str(series.dtype) != target_dtype_str:
        return series.astype(target_dtype)
    return series


def finalize_dataframe_for_parquet(table_name: str, df: pd.DataFrame) -> None:
    """
    Enforce schema dtypes before parquet write.
    Only stringify columns that are not governed by a typed schema entry.
    """
    id_col = df["_id"] if "_id" in df.columns else None
    for col_name in df.columns:
        if col_name == "_id":
            continue
        schema_of_this_column = schemas.get_table_column_schema(table_name, col_name)
        if schema_of_this_column:
            try:
                df[col_name] = _cast_column_to_schema_dtype(
                    col_name, df[col_name], schema_of_this_column[DTYPE_KEY]
                )
            except (ValueError, TypeError, OverflowError) as e:
                logger.warning(
                    f"Could not cast column {col_name} to schema dtype "
                    f"{schema_of_this_column[DTYPE_KEY]}: {e}"
                )
            if is_object_dtype(df[col_name]):
                logger.warning(
                    f"Column {col_name} is still object dtype after cast; "
                    "applying BSON-safe coercion before parquet write"
                )
                try:
                    df[col_name] = _cast_column_to_schema_dtype(
                        col_name, df[col_name], schema_of_this_column[DTYPE_KEY]
                    )
                except (ValueError, TypeError, OverflowError) as e:
                    logger.warning(
                        f"Fallback coercion failed for column {col_name}: {e}"
                    )
                    if _schema_target_is_typed(schema_of_this_column):
                        df[col_name] = _typed_null_series(
                            len(df), schema_of_this_column[DTYPE_KEY]
                        )
                    else:
                        df[col_name] = _resolve_string_schema_column(df[col_name])
        elif is_object_dtype(df[col_name]):
            df[col_name] = df[col_name].map(
                lambda value: float(value.to_decimal())
                if isinstance(value, bson.Decimal128)
                else value
            )
            df[col_name] = df[col_name].astype(str, errors="ignore")
    if id_col is not None:
        df["_id"] = id_col


def _log_conversion_failure(obj, type_name, default_value, error=None):
    global conversion_flag
    conversion_flag = True
    doc_id = str(current_document_id) if current_document_id is not None else "unknown"
    error_detail = f" ({error})" if error else ""
    logger.warning(
        f"Unsuccessful conversion for document {doc_id}, column {current_column_name}: "
        f'"{obj}" ({type(obj).__name__}) to {type_name}{error_detail}. Using {default_value!r}.'
    )
    append_to_file(
        f"\n{doc_id:<24} | {current_column_name:<20} | {str(obj):<24} | {str(default_value):<20}",
        table_name,
        CONVERSION_LOG_FILE_NAME,
        FileType.TEXT,
    )


def _converter_template(obj, type_name, raw_convert_func, default_value=None):
    original_type = type(obj)
    logger.debug(f"Converting {obj} of type {original_type} to {type_name}.")
    try:
        return raw_convert_func(obj)
    except (ValueError, TypeError, OverflowError) as error:
        _log_conversion_failure(obj, type_name, default_value, error)
        return default_value


def _coerce_to_int64_value(obj):
    if _is_null_value(obj):
        return None
    if isinstance(obj, list) or isinstance(obj, dict):
        return to_json_string(obj)
    if isinstance(obj, bson.Decimal128):
        obj = int(obj.to_decimal())
    elif isinstance(obj, bson.int64.Int64):
        obj = int(obj)
    elif isinstance(obj, (str, bytes)):
        obj = int(obj)
    elif isinstance(obj, (float, np.floating)):
        if not float(obj).is_integer():
            raise ValueError(f"non-integer float value: {obj}")
        obj = int(obj)
    elif isinstance(obj, (int, np.integer)) and not isinstance(obj, (bool, np.bool_)):
        obj = int(obj)
    else:
        obj = int(obj)

    if obj < INT64_MIN or obj > INT64_MAX:
        raise OverflowError(f"value {obj} is outside int64 range")

    return np.int64(obj)


def _coerce_to_float64_value(obj):
    if _is_null_value(obj):
        return None
    if isinstance(obj, bson.Decimal128):
        return np.float64(obj.to_decimal())
    if isinstance(obj, (float, np.floating)) and not isinstance(obj, (bool, np.bool_)):
        return np.float64(obj)
    if isinstance(obj, (int, np.integer, bson.int64.Int64)) and not isinstance(
        obj, (bool, np.bool_)
    ):
        return np.float64(int(obj))
    if isinstance(obj, (str, bytes)):
        return np.float64(obj)
    raise ValueError(f"cannot convert {type(obj).__name__} to float64")


def _apply_column_conversion(df: pd.DataFrame, col_name: str, conversion_fcn):
    global current_document_id
    has_id = "_id" in df.columns
    converted = []
    for idx, item in df[col_name].items():
        if has_id:
            current_document_id = df.at[idx, "_id"]
        try:
            converted.append(conversion_fcn(item))
        except Exception as error:
            _log_conversion_failure(item, type(error).__name__, None, error)
            converted.append(None)
    return pd.Series(converted, index=df[col_name].index)


def to_string(obj) -> str:
    if isinstance(obj, list) or isinstance(obj, dict):
        return to_json_string(obj)
    return _converter_template(
        obj, "string", lambda o: str(o) if _is_non_null_scalar(o) else ''
    )


def to_numpy_int64(obj) -> np.int64:
    logger.debug(f"to_numpy_int64: obj={obj}, type={type(obj)}")
    return _converter_template(obj, "numpy.int64", _coerce_to_int64_value)


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
    return _converter_template(obj, "numpy.float64", _coerce_to_float64_value)


def to_pandas_timestamp(obj) -> pd.Timestamp:
    # return _converter_template(obj, "pandas.Timestamp", lambda o: pd.Timestamp(o))
    return _converter_template(obj, "pandas.Timestamp", lambda o: pd.to_datetime(o, utc=True).isoformat() if _is_non_null_scalar(o) else '')


def do_nothing(obj):
    original_type = type(obj)
    logger.info(f'Did not convert "{obj}" of type {original_type}.')
    return obj


def to_datetime_iso(obj) -> str:
    if not isinstance(obj, (date, datetime)):
        return obj
    return _converter_template(obj, "string", lambda o: o.isoformat() if _is_non_null_scalar(o) else '')


def to_json_string(obj) -> str:
    if isinstance(obj, list):
        return _converter_template(obj, "string", lambda o: json.dumps([ob for ob in o], default=str, cls=CustomJSONEncoder))
    return _converter_template(obj, "string", lambda o: json.dumps(o, default=str, cls=CustomJSONEncoder) if _is_non_null_scalar(o) else '')

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
    date: to_datetime_iso,
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
    pd.Timestamp: to_pandas_timestamp,
    bson.binary.Binary: do_nothing
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
    if _is_null_value(first_item):
        inferred = _infer_schema_from_pandas_dtype(column_dtype)
        if inferred:
            item_type, column_dtype = inferred
        else:
            item_type = str
            column_dtype = "object"
    elif any(isinstance(first_item, t) for t in TYPES_TO_CONVERT_TO_STR):
        item_type = str
    if isinstance(first_item, bson.Decimal128):
        item_type = float
        column_dtype = "float64"
    column_dtype = COLUMN_DTYPE_CONVERSION_MAP.get(column_dtype.__str__(), column_dtype)
    schema_of_this_column[DTYPE_KEY] = column_dtype
    schema_of_this_column[TYPE_KEY] = item_type
    logger.debug(
        f"Internal schema file : column_dtype {column_dtype} and item_type {item_type}"
    )
    return schema_of_this_column


def process_column_name(column_name: str) -> str:
    return str(column_name).replace(" ", "_")[:128]


def _schema_bootstrap_sample_size(estimated_count: int) -> int:
    """
    Number of documents to $sample for internal schema inference.

    Default is strictly below 5% of estimated collection size (4.9% floor),
    which aligns with MongoDB's $sample fast path when it is the first stage
    and N is under 5% of the collection.
    """
    if estimated_count <= 0:
        return 0
    override = os.getenv("SCHEMA_BOOTSTRAP_SAMPLE_SIZE")
    if override is not None and override.strip() != "":
        try:
            n = int(override)
        except ValueError:
            logger.warning(
                "Invalid SCHEMA_BOOTSTRAP_SAMPLE_SIZE=%r; using computed default",
                override,
            )
            n = max(1, int(estimated_count * SCHEMA_BOOTSTRAP_MAX_FRACTION))
        else:
            n = max(1, min(n, estimated_count))
        return n
    computed = int(estimated_count * SCHEMA_BOOTSTRAP_MAX_FRACTION)
    return max(1, min(computed, estimated_count))


def _is_sample_stage_failure(exc: Exception) -> bool:
    if not isinstance(exc, pymongo.errors.OperationFailure):
        return False
    msg = str(exc).lower()
    return "$sample" in msg or "non-duplicate document" in msg


def _sample_size_for_attempt(initial_sample_size: int, attempt: int) -> int:
    """Reduce by 25% of the initial size on each attempt (attempt is 1-based)."""
    reduction = 0.25 * (attempt - 1) * initial_sample_size
    return max(1, int(initial_sample_size - reduction))


def _fetch_bootstrap_sample_documents(
    collection,
    initial_sample_size: int,
    *,
    table_name: str,
) -> list[dict]:
    """
    Load documents for schema bootstrap.

    Tries $sample up to SCHEMA_BOOTSTRAP_SAMPLE_MAX_ATTEMPTS times with a
    decreasing size (initial, -25%, -50%, -75% of initial). On persistent
  failure, falls back to find().sort(_id).limit(initial_sample_size).
    """
    if initial_sample_size <= 0:
        return []

    last_error: Exception | None = None
    for attempt in range(1, SCHEMA_BOOTSTRAP_SAMPLE_MAX_ATTEMPTS + 1):
        sample_size = _sample_size_for_attempt(initial_sample_size, attempt)
        try:
            logger.info(
                "schema bootstrap for %s: $sample attempt %s/%s, size=%s",
                table_name,
                attempt,
                SCHEMA_BOOTSTRAP_SAMPLE_MAX_ATTEMPTS,
                sample_size,
            )
            return list(
                collection.aggregate(
                    [{"$sample": {"size": sample_size}}],
                    allowDiskUse=True,
                )
            )
        except Exception as exc:
            last_error = exc
            if _is_sample_stage_failure(exc):
                logger.warning(
                    "schema bootstrap for %s: $sample attempt %s failed (size=%s): %s",
                    table_name,
                    attempt,
                    sample_size,
                    exc,
                )
            else:
                logger.warning(
                    "schema bootstrap for %s: aggregate attempt %s failed (size=%s): %s",
                    table_name,
                    attempt,
                    sample_size,
                    exc,
                )
            if attempt < SCHEMA_BOOTSTRAP_SAMPLE_MAX_ATTEMPTS:
                time.sleep(0.5)

    logger.warning(
        "schema bootstrap for %s: $sample failed after %s attempts; "
        "falling back to find().sort(_id).limit(%s). Last error: %s",
        table_name,
        SCHEMA_BOOTSTRAP_SAMPLE_MAX_ATTEMPTS,
        initial_sample_size,
        last_error,
    )
    try:
        return list(
            collection.find().sort({"_id": 1}).limit(initial_sample_size)
        )
    except Exception as exc:
        logger.error(
            "schema bootstrap for %s: find fallback failed: %s",
            table_name,
            exc,
            exc_info=True,
        )
        raise


def _build_schema_from_documents(
    fetched_data: list[dict],
) -> tuple[dict, dict]:
    schema_of_this_table = {}
    column_renaming_of_this_table = {}
    if not fetched_data:
        return schema_of_this_table, column_renaming_of_this_table

    df = pd.DataFrame(fetched_data)
    df = df.convert_dtypes(dtype_backend="pyarrow")
    for col_name in df.keys().values:
        data = _get_first_non_null_mongo_value(fetched_data, col_name)
        logger.debug(
            f"get first item {data} of type {type(data)} in column {col_name}"
        )
        column_dtype = df[col_name].dtype
        schema_of_this_column = init_column_schema(column_dtype, data)
        processed_col_name = process_column_name(col_name)
        if processed_col_name != col_name:
            column_renaming_of_this_table[col_name] = processed_col_name
        schema_of_this_table[processed_col_name] = schema_of_this_column
    return schema_of_this_table, column_renaming_of_this_table


def _get_first_non_null_mongo_value(fetched_data: list[dict], column_name: str):
    for item in fetched_data:
        if column_name not in item:
            continue
        value = item.get(column_name)
        if not _is_null_value(value):
            return value
    for item in fetched_data:
        if column_name in item:
            return item.get(column_name)
    return None


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
    Falls back to the first row only when the entire column is null.
    """
    first_valid_index = df[column_name].first_valid_index()
    if first_valid_index is not None:
        first_valid_item = df[column_name][first_valid_index]
    else:
        first_valid_item = df[column_name].iloc[0]
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
        try:
            client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
            db = client[os.getenv("MONGO_DB_NAME")]
            collection = db[table_name]
            estimated_count = collection.estimated_document_count()
            bootstrap_sample_size = _schema_bootstrap_sample_size(estimated_count)
            logger.info(
                "schema bootstrap for %s: estimated_count=%s, target sample size=%s",
                table_name,
                estimated_count,
                bootstrap_sample_size,
            )
            fetched_data = _fetch_bootstrap_sample_documents(
                collection,
                bootstrap_sample_size,
                table_name=table_name,
            )
            schema_of_this_table, column_renaming_of_this_table = (
                _build_schema_from_documents(fetched_data)
            )
            schemas.init_table_schema(table_name, schema_of_this_table)
            schemas.init_column_renaming(table_name, column_renaming_of_this_table)
        except Exception as exc:
            logger.error(
                "schema bootstrap failed for %s; collection will still start mirroring "
                "and schema may evolve from init/delta batches: %s",
                table_name,
                exc,
                exc_info=True,
            )


def process_dataframe(table_name_param: str, df: pd.DataFrame):
    global current_column_name, table_name, conversion_flag
    table_name = table_name_param
    conversion_flag = False
    typed_df = df.convert_dtypes(dtype_backend="pyarrow")
    for column in typed_df.columns:
        df[column] = typed_df[column]
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
        conversion_fcn = TYPE_TO_CONVERT_FUNCTION_MAP.get(expected_type, do_nothing)
        if any(_needs_type_conversion(item, expected_type) for item in df[col_name]):
            current_column_name = col_name
            logger.debug(
                f"Converting column {col_name} values to expected type {expected_type}"
            )
            df[col_name] = _apply_column_conversion(df, col_name, conversion_fcn)
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
            except (ValueError, TypeError, OverflowError) as e:
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
        if not (
            is_datetime64_any_dtype(df[col_name])
            and is_object_dtype(schema_of_this_column[DTYPE_KEY])
        ):
            try:
                df[col_name] = _cast_column_to_schema_dtype(
                    col_name, df[col_name], schema_of_this_column[DTYPE_KEY]
                )
            except (ValueError, TypeError, OverflowError) as e:
                logger.warning(
                    f"An {e.__class__.__name__} was caught when trying to convert "
                    f"the dtype of the column {col_name} from {current_dtype} "
                    f"to {schema_of_this_column[DTYPE_KEY]}"
                )
    # Check if conversion log file exists before pushing
    logger.debug("conversion_flag: %s", conversion_flag)
    conversion_log_path = os.path.join(get_table_dir(table_name), CONVERSION_LOG_FILE_NAME)
    if os.path.exists(conversion_log_path) and conversion_flag:
        push_file_to_lz(conversion_log_path, table_name)
