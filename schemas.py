import logging
import os
import threading
import pickle

from utils import get_table_dir
from constants import INTERNAL_SCHEMA_FILE_NAME, COLUMN_RENAMING_FILE_NAME
from file_utils import FileType, write_to_file

logger = logging.getLogger(__name__)

__schemas = {}
__locks = {}
__column_renamings = {}


def write_table_schema_to_file(table_name: str):
    schema_of_this_table = __schemas.get(table_name, None)
    if not schema_of_this_table:
        return
    logger.info(f"writing schema of {table_name} into file")
    write_to_file(
        schema_of_this_table, table_name, INTERNAL_SCHEMA_FILE_NAME, FileType.PICKLE
    )


def init_table_schema(table_name: str, table_schema: dict):
    __schemas[table_name] = table_schema
    __locks[table_name] = threading.Lock()
    write_table_schema_to_file(table_name)

# 9 May 2025 when schema file exists no need to rewrite it
def init_table_schema_to_mem(table_name: str, table_schema: dict):
    __schemas[table_name] = table_schema
    __locks[table_name] = threading.Lock()
#    write_table_schema_to_file(table_name)


def get_table_schema(table_name: str) -> dict:
    return __schemas.get(table_name, None)


def get_table_column_schema(table_name: str, column_name: str) -> dict:
    return __schemas.get(table_name, {}).get(column_name, None)


def append_schema_column(table_name: str, column_name: str, column_schema: dict):
    # 0. get the lock of the collection
    with __locks[table_name]:
        # 1. append column
        __schemas.setdefault(table_name, {})[column_name] = column_schema
        # 2. write into file
        write_table_schema_to_file(table_name)


def write_table_column_renaming_to_file(table_name: str):
    table_column_renaming = __column_renamings.get(table_name, None)
    if not table_column_renaming:
        return
    logger.info(f"writing column renaming of {table_name} into file")
    write_to_file(
        table_column_renaming, table_name, COLUMN_RENAMING_FILE_NAME, FileType.PICKLE
    )


def init_column_renaming(table_name: str, column_renaming: dict):
    __column_renamings[table_name] = column_renaming
    write_table_column_renaming_to_file(table_name)


def add_column_renaming(
    table_name: str, original_column_name: str, new_column_name: str
):
    with __locks[table_name]:
        __column_renamings.setdefault(table_name, {})[
            original_column_name
        ] = new_column_name
        write_table_column_renaming_to_file(table_name)


def find_column_renaming(table_name: str, column_name: str) -> str:
    return __column_renamings.get(table_name, {}).get(column_name, None)


def get_table_column_renaming(table_name: str) -> dict:
    return __column_renamings.get(table_name, None)
